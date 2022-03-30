import {
	PuppetBridge,
	Log,
	IReceiveParams,
	IMessageEvent,
	IRemoteUser,
	IRemoteRoom,
	IRemoteGroup,
	IGroupInfo,
	IFileEvent,
	Util,
	IRetList,
	IStringFormatterVars,
	MessageDeduplicator,
	ISendingUser,
	Lock,
	IPresenceEvent,
	IPuppetData,
} from "mx-puppet-bridge";
import * as prometheus from "prom-client";
import {
	SlackMessageParser, ISlackMessageParserOpts, MatrixMessageParser, IMatrixMessageParserOpts,
} from "matrix-slack-parser";
import * as Slack from "soru-slack-client";
import * as Emoji from "node-emoji";
import { SlackProvisioningAPI } from "./api";
import { SlackStore } from "./store";
import * as escapeHtml from "escape-html";
import { Config } from "./index";
import * as express from "express";
import { LoggingConfig } from "mx-puppet-bridge/src/config";
import { IPuppet } from "mx-puppet-bridge/src/db/puppetstore";

const log = new Log("SlackPuppet:slack");

// tslint:disable-next-line:no-magic-numbers
const PREPARATION_LOCK_TIMEOUT = 1000 * 60 * 60 * 3;

const parallelLimit = async (tasks: (() => void)[] = [], limit: number = 5) => {
	const n = Math.min(limit, tasks.length);
	const ret: any[] = [];
	let index = 0;

	const next = async () => {
		const i = index++;
		const task = tasks[i];
		ret[i] = await task();
		if (index < tasks.length) {
			await next();
		}
	};

	const nextArr = Array(n).fill(next);
	await Promise.all(nextArr.map((fn) => fn()));

	return ret;
};

interface ISlackPuppet {
	client: Slack.Client;
	connectionTracker?: NodeJS.Timeout;
	data: any;
	clientStopped: boolean;
}

interface ISlackPuppets {
	[puppetId: number]: ISlackPuppet;
}

export class BridgeMetrics {
	public currentBackfill: prometheus.Gauge<string>;
	public backfillStarted: prometheus.Counter<string>;
	public backfillDuration: prometheus.Gauge<string>;
	public messageErrors: prometheus.Counter<string>;
}

export class App {
	private puppets: ISlackPuppets = {};
	private tsThreads: {[ts: string]: string} = {};
	private threadSendTs: {[ts: string]: string} = {};
	private slackMessageParser: SlackMessageParser;
	private matrixMessageParser: MatrixMessageParser;
	private messageDeduplicator: MessageDeduplicator;
	private provisioningAPI: SlackProvisioningAPI;
	private store: SlackStore;
	private preparationLock: Lock<string>;
	public metrics: BridgeMetrics;
	constructor(
		private puppet: PuppetBridge,
	) {
		this.slackMessageParser = new SlackMessageParser();
		this.matrixMessageParser = new MatrixMessageParser();
		this.messageDeduplicator = new MessageDeduplicator();
		this.provisioningAPI = new SlackProvisioningAPI(puppet);
		this.store = new SlackStore(puppet.store);
		this.preparationLock = new Lock(PREPARATION_LOCK_TIMEOUT);
		this.metrics = new BridgeMetrics();
		this.metrics.currentBackfill = new prometheus.Gauge({
			name: "bridge_current_backfill_counter",
			help: "Counter for current backfills",
			labelNames: ["protocol", "room", "puppet"],
		});
		this.metrics.backfillStarted = new prometheus.Counter({
			name: "bridge_backfill_started",
			help: "Counter for all backfills started",
			labelNames: ["protocol", "room", "puppet"],
		})
		this.metrics.backfillDuration = new prometheus.Gauge({
			name: "bridge_backfill_duration",
			help: "Guage for time for each backfill",
			labelNames: ["protocol", "room", "puppet"],
		});
		this.metrics.messageErrors = new prometheus.Counter({
			name: "bridge_message_errors",
			help: "Counter for all message errors",
			labelNames: ["protocol", "puppet"],
		});
	}

	public async init(): Promise<void> {
		await this.store.init();
	}

	public async getUserParams(puppetId: number, user: Slack.User | Slack.Bot): Promise<IRemoteUser> {
		if (user.partial) {
			await user.load();
		}

		const nameVars: IStringFormatterVars = {
			team: user.team.name,
			name: user instanceof Slack.Bot ? user.displayName : user.realName,
		};
		let userId = user.fullId;
		if (user instanceof Slack.Bot) {
			userId += `-${user.displayName}`;
		}
		return {
			puppetId,
			userId,
			avatarUrl: user.iconUrl,
			nameVars,
		};
	}

	public async getRoomParams(puppetId: number, chan: Slack.Channel): Promise<IRemoteRoom> {
		if (chan.partial) {
			await chan.load();
		}

		if (chan.type === "im") {
			const members = [...chan.members.values()];
			const currentUserName = this.puppets[puppetId].data.self.name;
			const nameVars: IStringFormatterVars = {};
			nameVars.name = members
				.filter(({ name }) => name !== currentUserName)
				.map(({ realName }) => realName)
				.sort((a, b) => a.localeCompare(b))
				.join(", ");

			return {
				puppetId,
				roomId: chan.fullId,
				nameVars,
				groupId: chan.team.id,
				isDirect: true,
			};
	}


		const nameVars: IStringFormatterVars = {
			name: chan.name,
			team: chan.team.name,
			type: chan.type,
		};


		//TODO, FIX SLACK API TO GET PROPER MPIM TYPE
		if (chan.type === "mpim" || (chan.purpose?.startsWith("Group messaging with: ") && chan.name?.startsWith("mpdm-") && chan.private)) {
			const members = [...chan.members.values()];
			const currentUserName = this.puppets[puppetId].data.self.name;
			nameVars.name = members
				.filter(({ name }) => name !== currentUserName)
				.map(({ realName }) => realName)
				.sort((a, b) => a.localeCompare(b))
				.join(", ");
			chan.type = "mpim";
			return {
				puppetId,
				roomId: chan.fullId,
				nameVars,
				avatarUrl: chan.team.iconUrl,
				topic: chan.topic,
				isDirect: true,
				groupId: chan.team.id,
			};
		}

		return {
			puppetId,
			roomId: chan.fullId,
			nameVars,
			avatarUrl: chan.team.iconUrl,
			topic: chan.topic,
			isDirect: false,
			groupId: chan.team.id,
		};
	}

	public async getGroupParams(puppetId: number, team: Slack.Team): Promise<IRemoteGroup> {
		if (team.partial) {
			await team.load();
		}
		const roomIds: string[] = [];
		let description = `<h1>${escapeHtml(team.name)}</h1>`;
		description += `<h2>Channels:</h2><ul>`;
		for (const [, chan] of team.channels) {
			if (!["channel", "group"].includes(chan.type)) {
				continue;
			}
			roomIds.push(chan.fullId);
			const mxid = await this.puppet.getMxidForRoom({
				puppetId,
				roomId: chan.fullId,
			});
			const url = "https://matrix.to/#/" + mxid;
			log.info(chan.name);
			const name = escapeHtml(chan.name);
			description += `<li>${name}: <a href="${url}">${name}</a></li>`;
		}
		description += "</ul>";
		log.info("team name", team.name);
		return {
			puppetId,
			groupId: team.id,
			nameVars: {
				name: team.name,
			},
			avatarUrl: team.iconUrl,
			roomIds,
			longDescription: description,
		};
	}

	public async getSendParams(
		puppetId: number,
		msgOrChannel: Slack.Message | Slack.Channel,
		user?: Slack.User | Slack.Bot,
	): Promise<IReceiveParams> {
		let externalUrl: string | undefined;
		let eventId: string | undefined;
		let channel: Slack.Channel;

		if (!user) {
			user = (msgOrChannel as Slack.Message).author;
			const msg = msgOrChannel as Slack.Message;
			channel = (msgOrChannel as Slack.Message).channel;
			externalUrl = `https://${user.team.domain}.slack.com/archives/${channel.id}/p${msg.ts}`;
			eventId = msg.ts;
		} else {
			channel = msgOrChannel as Slack.Channel;
		}
		if (user.team.partial) {
			await user.team.load();
		}
		return {
			room: await this.getRoomParams(puppetId, channel),
			user: await this.getUserParams(puppetId, user),
			eventId,
			externalUrl,
		};
	}

	public async removePuppet(puppetId: number) {
		log.info(`Removing puppet: puppetId=${puppetId}`);
		await this.stopClient(puppetId);
		delete this.puppets[puppetId];
	}

	public async stopClient(puppetId: number) {
		const p = this.puppets[puppetId];
		if (!p) {
			return;
		}
		p.clientStopped = true;
		await p.client.disconnect();
	}

	public async startClient(puppetId: number) {
		const p = this.puppets[puppetId];
		if (!p) {
			return;
		}
		const opts: Slack.IClientOpts = {};
		if (p.data.token) {
			opts.token = p.data.token;
		}
		if (p.data.cookie) {
			opts.cookie = p.data.cookie;
		}
		if (p.data.appId) {
			opts.events = {
				express: {
					app: this.puppet.AS.expressAppInstance,
					path: Config().slack.path,
				},
				appId: p.data.appId,
				clientId: p.data.clientId,
				clientSecret: p.data.clientSecret,
				signingSecret: p.data.signingSecret,
				storeToken: async (t: Slack.IStoreToken) => {
					await this.store.storeToken(puppetId, t);
				},
				getTokens: async (): Promise<Slack.IStoreToken[]> => {
					return await this.store.getTokens(puppetId);
				},
			};
		}
		const client = new Slack.Client(opts);
		client.on("connected", async () => {
			log.info("client connected for puppetid", puppetId)
			await this.puppet.sendStatusMessage(puppetId, "connected", true);
			for (const [, user] of client.users) {
				const d = this.puppets[puppetId].data;
				d.team = {
					id: user.team.id,
					name: user.team.name,
				};
				d.self = {
					id: user.fullId,
					name: user.name,
				};
				await this.puppet.setUserId(puppetId, user.fullId);
				await this.puppet.setPuppetData(puppetId, d);
				break;
			}
		});
		client.on("ready", async () => {
			this.onClientReady(puppetId);
			this.puppet.trackConnectionStatus(puppetId, true);
			log.info("client ready for puppetId", puppetId)
		});
		client.on("disconnected", async () => {
			if (p.clientStopped) {
				return;
			}
			log.info(`Lost connection for puppet ${puppetId}, reconnecting in a minute...`);
			await this.puppet.sendStatusMessage(puppetId, "Lost connection, reconnecting in a minute...", false);
			await this.puppet.setBridgeStatus("TRANSIENT_DISCONNECT", puppetId,  "slack-lost-connection");
			const MINUTE = 60000;
			await Util.sleep(MINUTE);
			try {	
				log.info("Reconnected for puppet", puppetId)
				await this.stopClient(puppetId);
				await this.startClient(puppetId);
			} catch (err) {
				log.warn("Failed to restart client for puppet", puppetId, err);
				await this.puppet.sendStatusMessage(puppetId, "Failed to restart client", false);
				await this.puppet.setBridgeStatus("UNKNOWN_ERROR", puppetId,  "slack-failed-restart");
			}
		});

		let messageQueue: [number, Slack.Message][] = [];
		let processingQueue = false;
		const processMessageQueue = async () => {
			try {
				processingQueue = true;
				while (messageQueue.length) {
					const [puppetId, msg] = messageQueue[0];
					await this.handleSlackMessage(puppetId, msg);
					messageQueue.shift();
				}
			} catch (err) {
				log.warn("message queue processing interrupted", err);
				const msg = messageQueue.shift();
				if (msg && msg[1].ts) {
					log.info("Shifting out of message", msg[1].ts);
					this.metrics.messageErrors.inc({
						protocol: "slack",
						puppet: puppetId,
					});
				}
				throw err;
			} finally {
				processingQueue = false;
			}
		}

		client.on("message", async (msg: Slack.Message) => {
			try {
				await this.waitPreparationLock(puppetId, msg.channel);
				log.verbose("Got new message even");
				messageQueue.push([puppetId, msg]);
				if (messageQueue.length === 1 || !processingQueue) {
					processMessageQueue();
				} else {
					log.info("Queuing message event");
				}
			} catch (err) {
				log.error("Error handling slack message event", err);
			}
		});
		client.on("messageChanged", async (msg1: Slack.Message, msg2: Slack.Message) => {
			try {
				await this.waitPreparationLock(puppetId, msg1.channel);
				log.verbose("Got new message changed event");
				await this.handleSlackMessageChanged(puppetId, msg1, msg2);
			} catch (err) {
				log.error("Error handling slack messageChanged event", err);
			}
		});
		client.on("messageDeleted", async (msg: Slack.Message) => {
			try {
				await this.waitPreparationLock(puppetId, msg.channel);
				log.verbose("Got new message deleted event");
				await this.handleSlackMessageDeleted(puppetId, msg);
			} catch (err) {
				log.error("Error handling slack messageDeleted event", err);
			}
		});
		client.on("addUser", async (user: Slack.User) => {
			await this.puppet.updateUser(await this.getUserParams(puppetId, user));
			log.info("ADDED TO TEAM");
			const teamSize = await this.listUsers(puppetId)
			log.info("users", teamSize.length);
			//const chan = await user.checkIm();
			//if (chan) {
				//await chan.load();
				//await this.puppet.sendMDirect(puppetId);
			//}
		});
		client.on("changeUser", async (_, user: Slack.User) => {
			await this.puppet.updateUser(await this.getUserParams(puppetId, user));
		});
		for (const ev of ["channelJoined", "channelOpen", "channelUnarchive"]) {
			client.on(ev, async (chan: Slack.Channel) => {
				await this.waitPreparationLock(puppetId, chan);
				await this.updateRoom(puppetId, chan, true, true);
				// if (chan.type === "im" ) {
				// 	await chan.load();
				// 	await this.puppet.sendMDirect(puppetId);
				// }
			});
		}
		for (const ev of ["channelLeft", "channelClose", "channelArchive", "channelRename"]) {
			client.on(ev, async (chan: Slack.Channel) => {
				await this.waitPreparationLock(puppetId, chan);
				await this.updateRoom(puppetId, chan);
			});
		}
		client.on("channelMarked", async (chan: Slack.Channel, user: Slack.User, ts: string) => {
			await this.waitPreparationLock(puppetId, chan);
			const params = {
				room: await this.getRoomParams(puppetId, chan),
				user: await this.getUserParams(puppetId, user),
				eventId: ts,
			};
			await this.puppet.sendReadReceipt(params);
			// TODO this should probably be a part of sendReadReceipt
			if (params.user.userId === p.data.self.id) {
				const puppetClient = await this.puppet.userSync.getPuppetClient(puppetId);
				if (!puppetClient) {
					return;
				}
				const roomID = await this.puppet.roomSync.maybeGetMxid(params.room);
				if (!roomID) {
					return;
				}
				const origEventIdIds = await this.puppet.eventSync.getMatrix(params.room, params.eventId);
				for (const origEventId of origEventIdIds) {
					await puppetClient.sendReadReceipt(roomID, origEventId);
				}
			}
		});
		client.on("addTeam", async (team: Slack.Team) => {
			await this.puppet.updateGroup(await this.getGroupParams(puppetId, team));

		});
		client.on("changeTeam", async (_, team: Slack.Team) => {
			await this.puppet.updateGroup(await this.getGroupParams(puppetId, team));
		});
		client.on("typing", async (chan: Slack.Channel, user: Slack.User) => {
			await this.waitPreparationLock(puppetId, chan);
			if (["mpim", "im"].includes(chan.type) && !chan.opened) {
				return;
			}
			const params = await this.getSendParams(puppetId, chan, user);
			await this.puppet.setUserTyping(params, true);
		});
		client.on("presenceChange", async (user: Slack.User, presence: string) => {
			log.verbose("Received presence change");
			let matrixPresence = {
				active: "online",
				away: "offline",
			}[presence];
			if (!matrixPresence) {
				matrixPresence = "offline";
			}
			await this.puppet.setUserPresence({
				userId: user.fullId,
				puppetId,
			}, matrixPresence);
		});
		client.on("reactionAdded", async (reaction: Slack.Reaction) => {
			await this.waitPreparationLock(puppetId, reaction.message.channel);
			log.verbose("Received new reaction");
			const params = await this.getSendParams(puppetId, reaction.message);
			const e = Emoji.get(reaction.reaction);
			if (!e) {
				return;
			}

			await this.puppet.sendReaction(params, reaction.message.ts, e);
		});
		client.on("reactionRemoved", async (reaction: Slack.Reaction) => {
			await this.waitPreparationLock(puppetId, reaction.message.channel);
			log.verbose("Received reaction remove");
			const params = await this.getSendParams(puppetId, reaction.message);
			const e = Emoji.get(reaction.reaction);
			if (!e) {
				return;
			}
			await this.puppet.removeReaction(params, reaction.message.ts, e);
		});
		client.on("memberJoinedChannel", async (user: Slack.User, chan: Slack.Channel) => {
			await this.waitPreparationLock(puppetId, chan);
			log.info("Received member join for", chan.fullId, "and", chan.id);
			const params = await this.getSendParams(puppetId, chan, user);
			await this.puppet.addUser(params);
		});
		client.on("memberLeftChannel", async (user: Slack.User, chan: Slack.Channel) => {
			await this.waitPreparationLock(puppetId, chan);
			log.verbose("Received member left");
			const params = await this.getSendParams(puppetId, chan, user);
			await this.puppet.removeUser(params);
		});
		p.client = client;
		if (p.connectionTracker) {
			clearInterval(p.connectionTracker);
		}
		p.connectionTracker = setInterval(() => {
			let connected = 0;
			let connecting = 0;
			for (const team of client.teams.values()) {
				// Skip counting this team - if it has a fakeId it's an external team, not a connected workspace
				if (!team.fakeId) {
					const rtm = client.rtm(team.id)
					if (rtm["stateMachine"].getCurrentState() === "connected" && rtm["stateMachine"].getStateHierarchy()[1] === 'ready') {
						connected++;
					} else if (rtm["stateMachine"].getCurrentState() === "connecting" || rtm["stateMachine"].getCurrentState() === "connected") {
						connecting++;
					}
				}
			}
			this.puppet.trackConnectionStatus(puppetId, connected > 0);
			if (connected > 0) {
				this.puppet.setBridgeStatus("CONNECTED", puppetId);
			} else if (connecting > 0) {
				this.puppet.setBridgeStatus("CONNECTING", puppetId);
			}
			else {
				this.puppet.setBridgeStatus("UNKNOWN_ERROR", puppetId, "slack-no-connections");
			}
		}, 3000);
		try {
			await client.connect();
		} catch (err) {
			log.warn("Failed to connect client", err);
			await this.puppet.sendStatusMessage(puppetId, `Failed to connect client: ${err.message}`, false);
			if (err.message.includes("invalid_auth")) {
				await this.puppet.setBridgeStatus("BAD_CREDENTIALS", puppetId, "slack-bad-credentials");
			} else {
				await this.puppet.setBridgeStatus("UNKNOWN_ERROR", puppetId, "slack-failed-connect");
			}
			throw err;
		}
	}

	public async newPuppet(puppetId: number, data: any) {
		log.info(`Adding new Puppet: puppetId=${puppetId}`);
		if (this.puppets[puppetId]) {
			await this.removePuppet(puppetId);
		}
		const client = new Slack.Client({});
		this.puppets[puppetId] = {
			client,
			data,
			clientStopped: false,
		};
		await this.startClient(puppetId);
	}

	public async deletePuppet(puppetId: number) {
		log.info(`Got signal to quit Puppet: puppetId=${puppetId}`);
		await this.stopClient(puppetId);
		await this.removePuppet(puppetId);
	}

	public async convertSlackMessageToEvents(puppetId: number, msg: Slack.Message): Promise<any[]> {
		if (msg.empty && !msg.attachments && !msg.files) {
			return [];
		}
		if (msg.author instanceof Slack.Bot) {
			const appUserId = msg.client.users.get(msg.author.team.id);
			if (msg.author.partial) {
				await msg.author.load();
			}
			if (appUserId && msg.author.user && appUserId.id === msg.author.user.id) {
				return [];
			}
		}
		const events = [] as { method: string, args: any[] }[];
		const params = await this.getSendParams(puppetId, msg);
		if (msg.ts) {
			params.ts = this.convertToMatrixTimestamp(msg.ts);
		}
		const client = this.puppets[puppetId].client;
		const parserOpts = this.getSlackMessageParserOpts(puppetId, msg.channel.team);
		log.verbose("Received message.");
		const dedupeKey = `${puppetId};${params.room.roomId}`;
		if (!(msg.empty && !msg.attachments) &&
			!await this.messageDeduplicator.dedupe(dedupeKey, params.user.userId, params.eventId, msg.text || "")) {
			const res = await this.slackMessageParser.FormatMessage(parserOpts, {
				text: msg.text || "",
				blocks: msg.blocks || undefined,
				attachments: msg.attachments || undefined,
			});
			const opts = {
				body: res.body,
				formattedBody: res.formatted_body,
				emote: msg.meMessage,
			};
			if (msg.threadTs) {
				const replyTs = this.threadSendTs[msg.threadTs] || msg.threadTs;
				this.threadSendTs[msg.threadTs] = msg.ts;
				this.tsThreads[msg.ts] = msg.threadTs;
				events.push({
					method: "sendReply",
					args: [params, replyTs, opts],
				});
			} else {
				events.push({
					method: "sendMessage",
					args: [params, opts],
				});
			}
		} else if (!(msg.empty && !msg.attachments)) {
				log.warn("Deduplicated message in convertSlackMessageToEvents: ", msg.ts);
		}
		if (msg.files) {
			let replyTs = "";
			let isReply = false;
			if (msg.threadTs) {
				isReply = true;
				replyTs = this.threadSendTs[msg.threadTs] || msg.threadTs;
				this.threadSendTs[msg.threadTs] = msg.ts;
				this.tsThreads[msg.ts] = msg.threadTs;
			}
			const method = isReply ? "sendReply" : "sendMessage";
			const generateSendArgs = opts => isReply ? [params, replyTs, opts] : [params, opts];

			// this has files
			for (const f of msg.files) {
				if (f.title &&
					await this.messageDeduplicator.dedupe(dedupeKey, params.user.userId, params.eventId, "file:" + f.title)) {
					// skip this, we sent it!
					continue;
				}
				if (f.size > 104857600) {
					log.debug("Not downloading too big file from Slack:", f);
					events.push({
						method,
						args: generateSendArgs({
							body: `sent a big file: ${f.url_private}`,
							emote: true,
						}),
					});
					continue;
				}
				let arrayBuffer = await this.tryDownloadFile(client, f);
				if (arrayBuffer === null) {
					log.debug("Retrying failed file download in 3 seconds");
					await new Promise(resolve => setTimeout(resolve, 3000));
					arrayBuffer = await this.tryDownloadFile(client, f);
				}
				if (arrayBuffer === null) {
					events.push({
						method,
						args: generateSendArgs({
							body: `sent a file: ${f.url_private}`,
							emote: true,
						}),
					});
				} else {
					events.push({
						method: isReply ? "sendReplyFileDetect" : "sendFileDetect",
						args: isReply
							? [params, replyTs, Buffer.from(arrayBuffer), f.name]
							: [params, Buffer.from(arrayBuffer), f.name],
					});
				}
				if (f.initial_comment) {
					const ret = await this.slackMessageParser.FormatText(parserOpts, f.initial_comment);
					events.push({
						method,
						args: generateSendArgs({
							body: ret.body,
							formattedBody: ret.formatted_body,
						}),
					});
				}
			}
		}
		return events;
	}

	private async tryDownloadFile(client: Slack.Client, f: any): Promise<ArrayBuffer | null> {
		try {
			const buffer = await client.downloadFile(f.url_private);
			const downloadedMime = Util.GetMimeType(buffer);
			if (this.isDownloadError(buffer, f.mimetype) || this.isUnexpectedMimeType(downloadedMime, f.mimetype)) {
				log.error("Failed to download file from Slack: got", downloadedMime, "when expecting", f.mimetype, "in", f);
				return null;
			}
			return buffer;
		} catch (err) {
			log.error("Failed to download file from Slack:", err, f);
			return null;
		}
	}

	private isUnexpectedMimeType(downloadedMime: string | undefined, mimetype: string | null): boolean {
		if (!mimetype || !downloadedMime) {
			return false;
		}
		return mimetype !== downloadedMime && !mimetype.startsWith("text/")
			&& (downloadedMime === "text/html" || downloadedMime === "application/xml")
	}

	private isDownloadError(buffer: Buffer, expectedMimetype: string | null): boolean {
		if (buffer.length === 19) {
			const downloadError = (new TextEncoder()).encode("Error serving file.");
			const isDownloadError = downloadError.every((value, index) => value === buffer[index]);
			if (isDownloadError) {
				return true;
			}
		} else if (buffer.length < 500 && expectedMimetype !== "application/xml") {
			const downloadError = (new TextEncoder()).encode("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message>");
			const isDownloadError = downloadError.every((value, index) => value === buffer[index]);
			if (isDownloadError) {
				return true;
			}
		}
		return false;
	}

	public async handleSlackMessage(puppetId: number, msg: Slack.Message) {
		const events = await this.convertSlackMessageToEvents(puppetId, msg);
		for (const event of events) {
			await this.puppet[event.method](...event.args);
		}
	}

	public async handleSlackMessageChanged(puppetId: number, msg1: Slack.Message, msg2: Slack.Message) {
		if (msg1.text === msg2.text) {
			return;
		}
		if (msg1.author instanceof Slack.Bot) {
			const appUserId = msg1.client.users.get(msg1.author.team.id);
			if (msg1.author.partial) {
				await msg1.author.load();
			}
			if (appUserId && msg1.author.user && appUserId.id === msg1.author.user.id) {
				return;
			}
		}
		const params = await this.getSendParams(puppetId, msg2);
		const client = this.puppets[puppetId].client;
		const parserOpts = this.getSlackMessageParserOpts(puppetId, msg1.channel.team);
		log.verbose("Received message edit");
		const dedupeKey = `${puppetId};${params.room.roomId}`;
		if (await this.messageDeduplicator.dedupe(dedupeKey, params.user.userId, params.eventId, msg2.text || "")) {
			return;
		}
		const res = await this.slackMessageParser.FormatMessage(parserOpts, {
			text: msg2.text || "",
			blocks: msg2.blocks || undefined,
		});
		await this.puppet.sendEdit(params, msg1.ts, {
			body: res.body,
			formattedBody: res.formatted_body,
		});
	}

	public async handleSlackMessageDeleted(puppetId: number, msg: Slack.Message) {
		if (msg.author instanceof Slack.Bot) {
			const appUserId = msg.client.users.get(msg.author.team.id);
			if (msg.author.partial) {
				await msg.author.load();
			}
			if (appUserId && msg.author.user && appUserId.id === msg.author.user.id) {
				return;
			}
		}
		const params = await this.getSendParams(puppetId, msg);
		await this.puppet.sendRedact(params, msg.ts);
	}

	public async handleMatrixMessage(room: IRemoteRoom, data: IMessageEvent, asUser: ISendingUser | null, event: any) {
		const p = this.puppets[room.puppetId];
		if (!p) {
			log.warn(`Puppet not found for ${room.puppetId}, aborting handleMatrixMessage`);
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room not found for ${room.roomId}, aborting handleMatrixMessage`);
			return;
		}
		if (asUser) {
			if (!event.content.formatted_body) {
				event.content.formatted_body = escapeHtml(event.content.body);
			}
			if (data.emote) {
				event.content.formatted_body = `<em>${event.content.formatted_body}</em>`;
			}
			data.emote = false;
			// add the fallback
			if (!p.data.appId) {
				event.content.formatted_body =
					`<strong>${escapeHtml(asUser.displayname)}</strong>: ${event.content.formatted_body}`;
			}
		}
		const msg = await this.matrixMessageParser.FormatMessage(
			this.getMatrixMessageParserOpts(room.puppetId),
			event.content,
		);

		if (msg.text.match(/^\/[0-9a-zA-Z]+/)) {
			log.info("Command detected in message");
			const [command, parameters] = msg.text.split(/ (.+)/);
			const retEventId = await chan.sendCommand(command, parameters);
			await this.puppet.eventSync.insert(room, data.eventId!, retEventId);
			return;
		}

		const dedupeKey = `${room.puppetId};${room.roomId}`;
		this.messageDeduplicator.lock(dedupeKey, p.data.self.id, msg.text);
		let eventId = "";
		if (asUser && p.data.appId) {
			log.info("Sending message from Matrix to Slack, asUser: false, chan.id:", chan.id)
			eventId = await chan.sendMessage(msg, {
				asUser: false,
				username: asUser.displayname,
				iconUrl: asUser.avatarUrl,
			});
		} else if (data.emote) {
			log.info("Sending emote from Matrix to Slack, chan.id:", chan.id)
			eventId = await chan.sendMeMessage(msg);
		} else {
			log.info("Sending message from Matrix to Slack, asUser: true, chan.id:", chan.id)
			eventId = await chan.sendMessage(msg, {
				asUser: true,
			});
		}
		this.messageDeduplicator.unlock(dedupeKey, p.data.self.id, eventId);
		if (eventId) {
			await this.puppet.eventSync.insert(room, data.eventId!, eventId);
			log.info(`Marking everything before ${eventId} as read on Slack ${chan.id} because of sent message`);
			await chan.mark(eventId);
		} else {
			log.warn(`No eventID - Cannot mark channel #${chan.name} with ${chan.id} for team ${chan.team.name} as read`);
		}
	}

	public async handleMatrixEdit(
		room: IRemoteRoom,
		eventId: string,
		data: IMessageEvent,
		asUser: ISendingUser | null,
		event: any,
	) {
		const p = this.puppets[room.puppetId];
		if (!p) {
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		const msg = await this.matrixMessageParser.FormatMessage(
			this.getMatrixMessageParserOpts(room.puppetId),
			event.content["m.new_content"],
		);
		if (asUser) {
			if (data.emote) {
				msg.text = `_${msg.text}_`;
			}
			data.emote = false;
			// add the fallback
			if (!p.data.appId) {
				msg.text = `*${asUser.displayname}*: ${msg.text}`;
			}
		}
		const dedupeKey = `${room.puppetId};${room.roomId}`;
		this.messageDeduplicator.lock(dedupeKey, p.data.self.id, msg.text);
		let newEventId = "";
		if (asUser && p.data.appId) {
			newEventId = await chan.editMessage(msg, eventId, {
				asUser: false,
				username: asUser.displayname,
				iconUrl: asUser.avatarUrl,
			});
		} else {
			newEventId = await chan.editMessage(msg, eventId, {
				asUser: true,
			});
		}
		this.messageDeduplicator.unlock(dedupeKey, p.data.self.id, newEventId);
		if (newEventId) {
			await this.puppet.eventSync.insert(room, data.eventId!, newEventId);
			log.info(`Marking everything before ${eventId} as read on Slack ${chan.id} because of edit`);
			await chan.mark(newEventId);
		} else {
			log.warn(`Cannot mark channel #${chan.name} for team ${chan.team.name} as read due to no event id for edit`);
		}
		
	}

	public async handleMatrixReply(
		room: IRemoteRoom,
		eventId: string,
		data: IMessageEvent,
		asUser: ISendingUser | null,
		event: any,
	) {
		const p = this.puppets[room.puppetId];
		if (!p) {
			return;
		}
		log.verbose(`Got reply to send of ts=${eventId}`);
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		let tsThread = eventId;
		while (this.tsThreads[tsThread]) {
			tsThread = this.tsThreads[tsThread];
		}
		log.verbose(`Determined thread ts=${tsThread}`);
		const msg = await this.matrixMessageParser.FormatMessage(
			this.getMatrixMessageParserOpts(room.puppetId),
			event.content,
		);
		if (asUser) {
			if (data.emote) {
				msg.text = `_${msg.text}_`;
			}
			data.emote = false;
			// add the fallback
			if (!p.data.appId) {
				msg.text = `*${asUser.displayname}*: ${msg.text}`;
			}
		}
		const dedupeKey = `${room.puppetId};${room.roomId}`;
		this.messageDeduplicator.lock(dedupeKey, p.data.self.id, msg.text);
		let newEventId = "";
		if (asUser && p.data.appId) {
			newEventId = await chan.sendMessage(msg, {
				asUser: false,
				username: asUser.displayname,
				iconUrl: asUser.avatarUrl,
				threadTs: tsThread,
			});
		} else {
			newEventId = await chan.sendMessage(msg, {
				asUser: true,
				threadTs: tsThread,
			});
		}
		this.messageDeduplicator.unlock(dedupeKey, p.data.self.id, newEventId);
		if (newEventId) {
			this.tsThreads[newEventId] = tsThread;
			await this.puppet.eventSync.insert(room, data.eventId!, newEventId);
			log.info(`Marking everything before ${eventId} as read on Slack ${chan.id} because of reply`);
			await chan.mark(newEventId);
		} else {
			log.warn(`Cannot mark channel #${chan.name} for team ${chan.team.name} as read due to no event id for reply`);
		}
	}

	public async handleMatrixRedact(room: IRemoteRoom, eventId: string, asUser: ISendingUser | null, event: any) {
		const p = this.puppets[room.puppetId];
		if (!p) {
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		if (asUser && p.data.appId) {
			await chan.deleteMessage(eventId, {
				asUser: true,
				username: asUser.displayname,
				iconUrl: asUser.avatarUrl,
			});
		} else {
			await chan.deleteMessage(eventId, {
				asUser: true,
			});
		}
	}

	public async handleMatrixReaction(room: IRemoteRoom, eventId: string, reaction: string, asUser: ISendingUser | null) {
		const p = this.puppets[room.puppetId];
		if (!p || asUser) {
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		const e = Emoji.find(reaction);
		if (!e) {
			return;
		}
		await chan.sendReaction(eventId, e.key);

	}

	public async handleMatrixRemoveReaction(
		room: IRemoteRoom,
		eventId: string,
		reaction: string,
		asUser: ISendingUser | null,
	) {
		const p = this.puppets[room.puppetId];
		if (!p || asUser) {
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		const e = Emoji.find(reaction);
		if (!e) {
			return;
		}
		await chan.removeReaction(eventId, e.key);

	}

	public async handleMatrixImage(
		room: IRemoteRoom,
		data: IFileEvent,
		asUser: ISendingUser | null,
		event: any,
	) {
		const p = this.puppets[room.puppetId];
		if (!p) {
			return;
		}
		if (!asUser) {
			await this.handleMatrixFile(room, data, asUser, event);
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		let eventId = "";
		if (p.data.appId) {
			eventId = await chan.sendMessage({
				text: "Please enable blocks...",
				blocks: [{
					type: "image",
					title: {
						type: "plain_text",
						text: data.filename,
					},
					image_url: data.url,
					alt_text: data.filename,
				}],
			}, {
				asUser: false,
				username: asUser.displayname,
				iconUrl: asUser.avatarUrl,
			});
		} else if (asUser) {
			eventId = await chan.sendMessage({
				text: "Please enable blocks...",
				blocks: [{
					type: "image",
					title: {
						type: "plain_text",
						text: `${asUser.displayname} just uploaded an image, ${data.filename}`,
					},
					image_url: data.url,
					alt_text: data.filename,
				}],
			}, {
				asUser: true,
			});
		}
		if (eventId) {
			await this.puppet.eventSync.insert(room, data.eventId!, eventId);
			log.info(`Marking everything before ${eventId} as read on Slack ${chan.id} because of image`);
			await chan.mark(eventId);
		} else {
			log.warn(`Cannot mark channel #${chan.name} for team ${chan.team.name} as read due to no image`);
		}
	}

	public async handleMatrixFile(
		room: IRemoteRoom,
		data: IFileEvent,
		asUser: ISendingUser | null,
		event: any,
	) {
		const p = this.puppets[room.puppetId];
		if (!p) {
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		const dedupeKey = `${room.puppetId};${room.roomId}`;
		this.messageDeduplicator.lock(dedupeKey, p.data.self.id, "file:" + data.filename);
		let eventId = "";
		if (asUser && p.data.appId) {
			eventId = await chan.sendMessage(`Uploaded a file: <${data.url}|${data.filename}>`, {
				asUser: false,
				username: asUser.displayname,
				iconUrl: asUser.avatarUrl,
			});
		} else if (asUser) {
			eventId = await chan.sendMessage(`${asUser.displayname} uploaded a file: <${data.url}|${data.filename}>`, {
				asUser: true,
			});
		} else {
			eventId = await chan.sendFile(data.url, data.filename);
		}

		this.messageDeduplicator.unlock(dedupeKey, p.data.self.id, eventId);
		if (eventId) {
			await this.puppet.eventSync.insert(room, data.eventId!, eventId);
			log.info(`Marking everything before ${eventId} as read on Slack ${chan.id} because of file`);
			await chan.mark(eventId);
		} else {
			log.warn(`Cannot mark channel #${chan.name} for team ${chan.team.name} as read due to no event id for file`);
		}
	}


	public async handleMatrixReceipt(room: IRemoteRoom, lastMatrixReceipt: number, eventId: string) {
		const p = this.puppets[room.puppetId];
		if (!p) {
			log.warn(`Failed to handle receipts for ${room.roomId}: puppet ${room.puppetId} not found`);
			log.debug(room, lastMatrixReceipt, eventId);
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		const reaction = await this.puppet.reactionStore.getFromReactionMxid(eventId);
		let [timestamp] = await this.puppet.eventSync.getRemote(room, eventId) || [];
		// getRemote for a reaction event returns the reaction target rather than actual reaction timestamp
		// for this case and also if we hasn't remote event for matrixId
		// we'll try to get last remote event between last read on Slack.com and read receipt from Matrix
		if ((!timestamp || reaction) && chan.lastRead) {
			log.info("checking messages before most recent receipt and after lastread")
			const events = await this.puppet.eventSync.getRemoteAfter(room, chan.lastRead);
			log.info("length of events is", events.length)
			try {
				log.info("last event is", events[events.length - 1])
			} catch (e){
				log.error("indexing error")
			}

			timestamp = events
				.filter(id => id <= this.convertToSlackTimestamp(lastMatrixReceipt))
				.sort((a, b) => parseFloat(a) - parseFloat(b))
				.pop()!;
		}
		if (!timestamp) {
			log.warn(`Cannot mark channel #${chan.name} with ${chan.id} for team ${chan.team.name} as read`);
			return;
		}
		log.info(`Marking everything before ${timestamp} as read on Slack ${chan.team.name} #${chan.name}`);
		await chan.mark(timestamp);
	}

	public async handleMatrixPresence(
		puppetId: number,
		presence: IPresenceEvent,
		asUser: ISendingUser | null,
		event: any,
	) {
		const p = this.puppets[puppetId];
		if (!p || asUser) {
			return;
		}
		if (presence.statusMsg) {
			await p.client.setStatus(presence.statusMsg);
		}
		await p.client.setPresence({
			online: "auto",
			offline: "away",
			unavailable: "away",
		}[presence.presence] as "auto" | "away");
	}

	public async handleMatrixTyping(
		room: IRemoteRoom,
		typing: boolean,
		asUser: ISendingUser | null,
		event: any,
	) {
		const p = this.puppets[room.puppetId];
		if (!p || asUser) {
			return;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			log.warn(`Room ${room.roomId} not found!`);
			return;
		}
		if (typing) {
			await chan.sendTyping();
		}
	}

	public async createRoom(room: IRemoteRoom): Promise<IRemoteRoom | null> {
		const p = this.puppets[room.puppetId];
		if (!p) {
			return null;
		}
		log.verbose(`Received create request for channel update puppetId=${room.puppetId} roomId=${room.roomId}`);
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			return null;
		}
		return await this.getRoomParams(room.puppetId, chan);
	}

	public async createUser(remoteUser: IRemoteUser): Promise<IRemoteUser | null> {
		const p = this.puppets[remoteUser.puppetId];
		if (!p) {
			return null;
		}
		log.info(`Received create request for user update puppetId=${remoteUser.puppetId} userId=${remoteUser.userId}`);
		const user = p.client.getUser(remoteUser.userId);
		if (!user) {
			return null;
		}
		return await this.getUserParams(remoteUser.puppetId, user);
	}

	public async createGroup(remoteGroup: IRemoteGroup): Promise<IRemoteGroup | null> {
		const p = this.puppets[remoteGroup.puppetId];
		if (!p) {
			return null;
		}
		log.info(`Received create request for group puppetId=${remoteGroup.puppetId} groupId=${remoteGroup.groupId}`);
		const group = p.client.teams.get(remoteGroup.groupId);
		if (!group) {
			return null;
		}
		return await this.getGroupParams(remoteGroup.puppetId, group);
	}

	public async getDmRoom(remoteUser: IRemoteUser): Promise<string | null> {
		const p = this.puppets[remoteUser.puppetId];
		if (!p) {
			return null;
		}
		const user = p.client.getUser(remoteUser.userId);
		if (!user) {
			return null;
		}
		const chan = await user.im();
		return chan ? chan.fullId : null;
	}

	public async listUsers(puppetId: number): Promise<IRetList[]> {
		const p = this.puppets[puppetId];
		if (!p) {
			return [];
		}
		const reply: IRetList[] = [];
		for (const [, team] of p.client.teams) {
			if (team.partial) {
				await team.load();
			}
			reply.push({
				category: true,
				name: team.name,
			});
			for (const [, user] of team.users) {
				reply.push({
					id: user.fullId,
					name: user.displayName,
				});
			}
		}
		return reply;
	}

	public async listRooms(puppetId: number): Promise<IRetList[]> {
		const p = this.puppets[puppetId];
		if (!p) {
			return [];
		}
		const reply: IRetList[] = [];
		for (const [, team] of p.client.teams) {
			if (team.partial) {
				await team.load();
			}
			reply.push({
				category: true,
				name: team.name,
			});
			for (const [, chan] of team.channels) {
				if (chan.type !== "im") {
					reply.push({
						id: chan.fullId,
						name: chan.name || chan.fullId,
					});
				}
			}
		}
		return reply;
	}

	public async listGroups(puppetId: number): Promise<IRetList[]> {
		const p = this.puppets[puppetId];
		if (!p) {
			return [];
		}
		const reply: IRetList[] = [];
		for (const [, team] of p.client.teams) {
			if (team.partial) {
				await team.load();
			}
			reply.push({
				name: team.name,
				id: team.id,
			});
		}
		return reply;
	}

	public async getGroupInfo(puppetId: number, groupId: string): Promise<IGroupInfo | null> {
		if (puppetId === -1) {
			for (const p of Object.values(this.puppets)) {
				const team = p.client.teams.get(groupId);
				if (team) {
					return {
						groupId: team.id,
						name: team.name,
					}
				}
			}
			return null;
		}
		const p = this.puppets[puppetId];
		if (!p) {
			return null;
		}
		const team = p.client.teams.get(groupId);
		if (!team) {
			return null;
		}
		return {
			groupId: team.id,
			name: team.name,
		}
	}

	public async getUserIdsInRoom(room: IRemoteRoom): Promise<Set<string> | null> {
		const p = this.puppets[room.puppetId];
		if (!p) {
			return null;
		}
		const chan = p.client.getChannel(room.roomId);
		if (!chan) {
			return null;
		}
		const users = new Set<string>();
		if (chan.partial) {
			await chan.load();
		}

		for (const [, member] of chan.members) {
			users.add(member.fullId);
		}

		return users;
	}

	private getMatrixMessageParserOpts(puppetId: number): IMatrixMessageParserOpts {
		return {
			callbacks: {
				canNotifyRoom: async () => true,
				getUserId: async (mxid: string) => {
					const parts = this.puppet.userSync.getPartsFromMxid(mxid);
					if (!parts || (parts.puppetId !== puppetId && parts.puppetId !== -1)) {
						return null;
					}
					return parts.userId.split("-")[1] || null;
				},
				getChannelId: async (mxid: string) => {
					const parts = await this.puppet.roomSync.getPartsFromMxid(mxid);
					if (!parts || (parts.puppetId !== puppetId && parts.puppetId !== -1)) {
						return null;
					}
					return parts.roomId.split("-")[1] || null;
				},
				mxcUrlToHttp: (mxc: string) => this.puppet.getUrlFromMxc(mxc),
			},
		};
	}

	private getSlackMessageParserOpts(puppetId: number, team: Slack.Team): ISlackMessageParserOpts {
		const client = this.puppets[puppetId].client;
		return {
			callbacks: {
				getUser: async (id: string, name: string) => {
					const user = client.getUser(id, team.id);
					if (!user) {
						return null;
					}
					return {
						mxid: await this.puppet.getMxidForUser({
							puppetId,
							userId: user.fullId,
						}),
						name: user.name,
					};
				},
				getChannel: async (id: string, name: string) => {
					const chan = client.getChannel(id, team.id);
					if (!chan) {
						return null;
					}
					return {
						mxid: await this.puppet.getMxidForRoom({
							puppetId,
							roomId: chan.fullId,
						}),
						name: "#" + chan.name,
					};
				},
				getMessage: async (teamDomain: string, channelId: string, messageId: string) => {
					const origPuppet = await this.puppet.provisioner.get(puppetId);
					if (!origPuppet) {
						log.warn(`Provisioner didn't return anything for puppetId given to getSlackMessageParserOpts`);
						return null;
					}
					let foundPuppetId: number | null = null;
					let foundTeam: Slack.Team | null = null;
					for (const [iterPuppetId, puppet] of Object.entries(this.puppets)) {
						const puppetData = await this.puppet.provisioner.get(+iterPuppetId);
						if (!puppetData || (puppetData.puppetMxid !== origPuppet.puppetMxid && !puppetData.isPublic)) {
							continue;
						}
						for (const clientTeam of puppet.client.teams.values()) {
							if (clientTeam.domain === teamDomain) {
								foundTeam = clientTeam;
								foundPuppetId = +iterPuppetId;
								break;
							}
						}
					}
					if (!foundTeam || !foundPuppetId) {
						log.debug(`Didn't find team ${teamDomain} to get message ${channelId}/${messageId}`);
						return null;
					}
					const room = {puppetId: foundPuppetId, roomId: `${foundTeam.id}-${channelId}`};
					const roomId = await this.puppet.roomSync.maybeGetMxid(room);
					if (!roomId) {
						log.debug(`Didn't find Matrix room ID for ${room.roomId} to get message ${messageId}`);
						return null;
					}
					const message = await this.puppet.eventSync.getMatrix(room, messageId);
					if (message.length === 0) {
						log.debug(`Didn't find Matrix event ID for ${room.roomId}/${messageId}`);
						return null;
					}
					return {
						mxid: message[0],
						roomId,
					};
				},
				getUsergroup: async (id: string, name: string) => null,
				getTeam: async (id: string, name: string) => null,
				urlToMxc: async (url: string) => {
					try {
						return await this.puppet.uploadContent(null, url);
					} catch (err) {
						log.error("Error uploading file:", err.error || err.body || err);
					}
					return null;
				},
			},
		};
	}

	private async onClientReady(puppetId: number) {
		await this.prepareRoomsForPuppet(puppetId);
	}

	private async prepareRoomsForPuppet(puppetId: number) {
		const puppet = this.puppets[puppetId];
		if (!puppet || puppet.clientStopped) {
			return;
		}
		const team = puppet.client.getTeam(puppet.data.team.id);
		if (!team) {
			return;
		}
		const updateFuncList: (() => void)[] = [];
		const inviteFuncList: (() => void)[] = [];
		for (const chan of team.channels.values()) {
			// lock all channels on start, they will unlock later
			const lockKey = `${puppetId};${chan.fullId}`;
			this.preparationLock.set(lockKey);
			const lockedTs = this.convertToSlackTimestamp(Date.now());
			updateFuncList.push(async () => {
				if (!puppet || puppet.clientStopped) {
					return;
				}
				const params = await this.getRoomParams(puppetId, chan);
				const shouldAddUsers = await this.updateRoom(puppetId, chan, true, false, lockedTs);
				if (shouldAddUsers) {
					inviteFuncList.push(async () => {
						if (!puppet || puppet.clientStopped) {
							return;
						}
						await this.puppet.addUsers(params);
					});
				}
			});
		}
		const { maxParallelRoomCreation } = Config().limits;
		// tslint:disable-next-line:no-magic-numbers
		await parallelLimit(updateFuncList, maxParallelRoomCreation);
		// tslint:disable-next-line:no-magic-numbers
		await parallelLimit(inviteFuncList, maxParallelRoomCreation);
		await this.puppet.sendMDirect(puppetId);
	}

	private async updateRoom(
		puppetId,
		chan,
		doBackfill: boolean = false,
		addUsers: boolean = false,
		lockedTs?: string,
	): Promise<boolean> {
		const lockKey = `${puppetId};${chan.fullId}`;
		this.preparationLock.set(lockKey);
		const p = this.puppets[puppetId];
		if (!p || p.clientStopped) {
			this.preparationLock.release(lockKey);
			return false;
		}
		if (!lockedTs) {
			lockedTs = this.convertToSlackTimestamp(Date.now());
		}
		const roomParams = await this.getRoomParams(puppetId, chan);
		const joined = chan.opened || (chan.isMember && !chan.archived);
		try {
			if (joined) {
				await this.puppet.updateRoom(roomParams, true);
				if (doBackfill) {
					log.info("Starting backfill for room:", roomParams.roomId, "and puppet:", roomParams.puppetId);
					this.metrics.currentBackfill.inc({
						protocol: "slack",
						room: roomParams.roomId,
						puppet: roomParams.puppetId,
					});
					this.metrics.backfillStarted.inc({
						protocol: "slack",
						room: roomParams.roomId,
						puppet: roomParams.puppetId,
					});
					const stopTimer = this.metrics.backfillDuration.startTimer({
						protocol: "slack",
						room: roomParams.roomId,
						puppet: roomParams.puppetId,
					})
					// check notification state for room and disable if needs
					const client = await this.puppet.userSync.getPuppetClient(puppetId);
					const mxid = await this.puppet.roomSync.maybeGetMxid(roomParams);
					let shouldDisableNotifs = false;
					if (client && mxid) {
						const url = `/_matrix/client/r0/pushrules/global/room/${encodeURIComponent(mxid)}`;
						try {
							await client.doRequest("GET", url, null, null);
						} catch (err) {
							// room not muted, it's our case
							if (err.body.errcode === "M_NOT_FOUND") {
								shouldDisableNotifs = true;
							} else {
								log.error(`Cannot get push rules for room ${mxid} before backfilling. Got error: `, err);
							}
						}
						if (shouldDisableNotifs) {
							try {
								await client.doRequest("PUT", url, null, {
									"actions": ["dont_notify"],
								});
							} catch (err) {
								log.error(`Cannot disable notifications for room ${mxid} before backfilling. Got error: `, err);
								shouldDisableNotifs = false;
							}
						}
					}
					// do backfill
					const lastEventTs = mxid ? await this.puppet.getLastRemoteEvent(roomParams) : null;
					await this.backfillRoomHistory(roomParams, lockedTs, lastEventTs);
					// enable notifications
					if (shouldDisableNotifs) {
						const url = `/_matrix/client/r0/pushrules/global/room/${encodeURIComponent(mxid!)}`;
						try {
							await client!.doRequest("DELETE", url, null, null);
						} catch (err) {
							log.error(`Cannot enable notifications for room ${mxid} after backfilling. Got error: `, err);
						}
					}
					// then mark as read
					const activeUser = p.client.getActiveUser(chan.team.id);
					if (activeUser) {
						const userParams = await this.getUserParams(puppetId, activeUser);
						await this.puppet.sendReadReceipt({
							room: roomParams,
							user: userParams,
							eventId: chan.lastRead, // last read event id
						});
					}
					stopTimer();
					this.metrics.currentBackfill.dec({
						protocol: "slack",
						room: roomParams.roomId,
						puppet: roomParams.puppetId,
					});
					log.info("Backfilling complete for room:", roomParams.roomId, "and puppet:", roomParams.puppetId);
				}
			}
		} catch (err) {
			if (doBackfill) {
				this.metrics.currentBackfill.dec({
					protocol: "slack",
					room: roomParams.roomId,
					puppet: roomParams.puppetId,
				});
			}
			log.error(`Failed to update room ${roomParams.roomId}: for puppet ${roomParams.puppetId}:`, err);
		} finally {
			this.preparationLock.release(lockKey);
		}
		if (addUsers) {
			await this.puppet.addUsers(roomParams);
			log.info("ADDING GHOSTS");
		}

		return joined;
	}

	public async checkIfAlreadyRegistered(data: IPuppetData): Promise<boolean> {
		const puppets = await this.puppet.provisioner.getAll();
		const tokenStart = typeof data.token === "string" ? data.token.slice(0,19): undefined
		if (tokenStart) {
			for (const p of puppets) {
				if (typeof p.data.token === "string" && p.data.token.startsWith(tokenStart)){
					log.warn("Puppet already exists on bridge")
					return true;
				}
			}
		} 
		return false;
	}

	private async backfillRoomHistory(room: IRemoteRoom, latest: string, oldest: string | null) {
		const p = this.puppets[room.puppetId];
		if (!p || p.clientStopped) {
			return;
		}
		let channelHistory;
		const { missedLimit, initialLimit } = Config().backfill;
		if (oldest) {
			if (missedLimit === 0) {
				log.verbose("Backfilling missed messages disabled, returning...");
				return;
			}
			channelHistory = await p.client.getChannelHistory(room.roomId, missedLimit, latest, oldest);
		} else {
			if (initialLimit === 0) {
				log.verbose("Backfilling on room creation disabled, returning...");
				return;
			}
			channelHistory = await p.client.getChannelHistory(room.roomId, initialLimit, latest);
		}
		if (!channelHistory.length) {
			log.verbose(`No messages to backfilling for room ${room.roomId}, returning...`);
			return;
		}

		// Pre-load room member list, because getUserIdsInRoom won't reload it if it's been loaded before
		const chan = p.client.getChannel(room.roomId);
		if (chan) {
			await chan.load();
		}

		const userIdsInRoom = await this.getUserIdsInRoom(room);
		const alreadyJoined = new Set<string>();
		let messageIds = new Set();
		let eventIds = new Set();
		log.info("Starting backfill of room history, channelHistory.len:", channelHistory.length);
		for (const msg of channelHistory.reverse()) {
			if (messageIds.has(msg.ts)) { log.error("FOUND A DUPLICATE!!! (before convertSlackMessage)"); }
			const events = await this.convertSlackMessageToEvents(room.puppetId, msg);
			if (messageIds.has(msg.ts)) { log.error("FOUND A DUPLICATE!!! (after convertSlackMessage)"); }
			messageIds.add(msg.ts)
			log.info("message timestamp: ", msg.ts);
			for (const event of events) {
				if (event.method === "sendMessage" && eventIds.has(event.args[0].ts)) { log.error("FOUND A DUPLICATE!!! (start of event loop)"); }
				if (!p || p.clientStopped) {
					log.info("Client was stopped, stop backfilling...");
					return;
				}
				const [params] = event.args;
				const userId = params.user.userId;
				if (userIdsInRoom && !userIdsInRoom.has(userId)) {
					log.verbose(`User ${userId} not found in room ${room.roomId}, ignoring...`);
					continue;
				}
				// add user before he send message
				if (!alreadyJoined.has(userId)) {
					await this.puppet.addUser(params);
					alreadyJoined.add(userId);
				}
				await this.puppet[event.method](...event.args);
				if (event.method === "sendMessage" && eventIds.has(event.args[0].ts)) { log.error("FOUND A DUPLICATE!!! (end of event loop)"); }
				eventIds.add(event.args[0].ts)
				log.info("event timestamp: ", event.args[0].ts);
			}
		}
	}

	private convertToMatrixTimestamp(ts: string): number {
		// tslint:disable-next-line:no-magic-numbers
		return parseInt(ts.split(".").join("").substring(0, 13), 10);
	}

	private convertToSlackTimestamp(ts: number): string {
		const seconds = Math.floor(ts / 1000);
		const microseconds = (ts % 1000) * 1000 + 999
		return `${seconds}.${microseconds.toString().padStart(6, "0")}`;
	}

	private async waitPreparationLock(puppetId, chan) {
		const lockKey = `${puppetId};${chan.fullId}`;
		await this.preparationLock.wait(lockKey);
	}

	private emojiToSlack(msg: string): string {
		return msg.replace(/((?:\u00a9|\u00ae|[\u2000-\u3300]|\ud83c[\ud000-\udfff]|\ud83d[\ud000-\udfff]|\ud83e[\ud000-\udfff])[\ufe00-\ufe0f]?)/gi, (s) => {
			const e = Emoji.find(s);
			if (e) {
				return `:${e.key}:`;
			}
			return "";
		});
	}

	private slackToEmoji(msg: string): string {
		return msg.replace(/:([^\s:]+?):/gi, (s) => {
			const e = Emoji.get(s);
			if (Emoji.find(e + "\ufe0f")) {
				return e + "\ufe0f";
			}
			return e;
		});
	}
}
