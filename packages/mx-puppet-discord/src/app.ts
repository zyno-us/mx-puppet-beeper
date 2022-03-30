/* tslint:disable: no-any */
/*
Copyright 2019, 2020 mx-puppet-discord
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import {
	PuppetBridge,
	Log,
	Util,
	IRetList,
	MessageDeduplicator,
	IRemoteRoom,
	IGroupInfo,
	IPuppetData
} from "mx-puppet-bridge";
import * as Discord from "better-discord.js";
import {
	DiscordMessageParser,
	MatrixMessageParser,
} from "matrix-discord-parser";
import * as path from "path";
import * as mime from "mime";
import { DiscordStore } from "./store";
import {
	DiscordUtil, TextGuildChannel, TextChannel, BridgeableGuildChannel, BridgeableChannel,
} from "./discord/DiscordUtil";
import { MatrixUtil } from "./matrix/MatrixUtil";
import { Commands } from "./Commands";
import ExpireSet from "expire-set";

const log = new Log("DiscordPuppet:App");
export const AVATAR_SETTINGS: Discord.ImageURLOptions & { dynamic?: boolean | undefined; }
= { format: "png", size: 2048, dynamic: true };
export const MAXFILESIZE = 8000000;

export interface IDiscordPuppet {
	client: Discord.Client;
	data: any;
	deletedMessages: ExpireSet<string>;
}

export interface IDiscordPuppets {
	[puppetId: number]: IDiscordPuppet;
}

export interface IDiscordSendFile {
	buffer: Buffer;
	filename: string;
	url: string;
	isImage: boolean;
}

export class App {
	public puppets: IDiscordPuppets = {};
	public discordMsgParser: DiscordMessageParser;
	public matrixMsgParser: MatrixMessageParser;
	public messageDeduplicator: MessageDeduplicator;
	public store: DiscordStore;
	public lastEventIds: {[chan: string]: string} = {};

	public readonly discord: DiscordUtil;
	public readonly matrix: MatrixUtil;
	public readonly commands: Commands;

	constructor(
		public puppet: PuppetBridge,
	) {
		this.discordMsgParser = new DiscordMessageParser();
		this.matrixMsgParser = new MatrixMessageParser();
		this.messageDeduplicator = new MessageDeduplicator();
		this.store = new DiscordStore(puppet.store);

		this.discord = new DiscordUtil(this);
		this.matrix = new MatrixUtil(this);
		this.commands = new Commands(this);
	}

	public async init(): Promise<void> {
		await this.store.init();
			log.warn(`IT IS WORRKKKKKIIINNNNGGGGGGGGGGGGGGGGHGHHGHGHGHGHGHHG`);
	}

	public async handlePuppetName(puppetId: number, name: string) {
		const p = this.puppets[puppetId];
		if (!p || !p.data.syncProfile || !p.client.user!.bot) {
			// bots can't change their name
			return;
		}
		try {
			await p.client.user!.setUsername(name);
		} catch (err) {
			log.warn(`Couldn't set name for ${puppetId}`, err);
		}
	}

	public async handlePuppetAvatar(puppetId: number, url: string, mxc: string) {
		const p = this.puppets[puppetId];
		if (!p || !p.data.syncProfile) {
			return;
		}
		try {
			const AVATAR_SIZE = 800;
			const realUrl = this.puppet.getUrlFromMxc(mxc, AVATAR_SIZE, AVATAR_SIZE, "scale");
			const buffer = await Util.DownloadFile(realUrl);
			await p.client.user!.setAvatar(buffer);
		} catch (err) {
			log.warn(`Couldn't set avatar for ${puppetId}`, err);
		}
	}

	public async newPuppet(puppetId: number, data: any) {
		// tslint:disable-next-line:no-magic-numbers
		log.info(`Adding new Puppet: puppetId=${puppetId} bot=${data.bot} username=${data.username} id=${data.id} token_prefix=${data.token?.substring(0, 4)} token_length=${data.token?.length}`);
		if (this.puppets[puppetId]) {
			await this.deletePuppet(puppetId);
		}
		let client: Discord.Client;
		if (data.bot || false) {
			client = new Discord.Client({ ws: { intents: Discord.Intents.NON_PRIVILEGED }});
		} else {
			client = new Discord.Client({ presence: {status: 'invisible', afk: true}});
		}
		client.on("ready", async () => {
			const d = this.puppets[puppetId].data;
			d.username = client.user!.tag;
			d.id = client.user!.id;
			d.bot = client.user!.bot;
			await this.puppet.setUserId(puppetId, client.user!.id);
			await this.puppet.setPuppetData(puppetId, d);
			await this.puppet.setBridgeStatus("CONNECTING", puppetId)
			await this.puppet.sendStatusMessage(puppetId, "connected", true);
			await this.updateUserInfo(puppetId);
			this.puppet.trackConnectionStatus(puppetId, true);
			this.puppet.setBridgeStatus("CONNECTED", puppetId)
			// set initial presence for everyone
			for (const user of client.users.cache.array()) {
				await this.discord.updatePresence(puppetId, user.presence);
			}
		});
		client.on("message", async (msg: Discord.Message) => {
			try {
				await this.discord.events.handleDiscordMessage(puppetId, msg);
			} catch (err) {
				log.error("Error handling discord message event", err.error || err.body || err);
			}
		});
		client.on("messageUpdate", async (msg1: Discord.Message, msg2: Discord.Message) => {
			try {
				await this.discord.events.handleDiscordMessageUpdate(puppetId, msg1, msg2);
			} catch (err) {
				log.error("Error handling discord messageUpdate event", err.error || err.body || err);
			}
		});
		client.on("messageDelete", async (msg: Discord.Message) => {
			try {
				await this.discord.events.handleDiscordMessageDelete(puppetId, msg);
			} catch (err) {
				log.error("Error handling discord messageDelete event", err.error || err.body || err);
			}
		});
		client.on("messageDeleteBulk", async (msgs: Discord.Collection<Discord.Snowflake, Discord.Message>) => {
			for (const msg of msgs.array()) {
				try {
					await this.discord.events.handleDiscordMessageDelete(puppetId, msg);
				} catch (err) {
					log.error("Error handling one discord messageDeleteBulk event", err.error || err.body || err);
				}
			}
		});
		client.on("typingStart", async (chan: Discord.Channel, user: Discord.User) => {
			try {
				if (!this.discord.isBridgeableChannel(chan)) {
					return;
				}
				const params = this.matrix.getSendParams(puppetId, chan as BridgeableChannel, user);
				await this.puppet.setUserTyping(params, true);
			} catch (err) {
				log.error("Error handling discord typingStart event", err.error || err.body || err);
			}
		});
		client.on("presenceUpdate", async (_, presence: Discord.Presence) => {
			try {
				await this.discord.updatePresence(puppetId, presence);
			} catch (err) {
				log.error("Error handling discord presenceUpdate event", err.error || err.body || err);
			}
		});
		client.on("messageReactionAdd", async (reaction: Discord.MessageReaction, user: Discord.User) => {
			try {
				// TODO: filter out echo back?
				const chan = reaction.message.channel;
				if (!await this.bridgeRoom(puppetId, chan)) {
					return;
				}
				const params = this.matrix.getSendParams(puppetId, chan, user);
				if (reaction.emoji.id) {
					const mxc = await this.matrix.getEmojiMxc(
						puppetId, reaction.emoji.name, reaction.emoji.animated, reaction.emoji.id,
					);
					await this.puppet.sendReaction(params, reaction.message.id, mxc || reaction.emoji.name);
				} else {
					await this.puppet.sendReaction(params, reaction.message.id, reaction.emoji.name);
				}
			} catch (err) {
				log.error("Error handling discord messageReactionAdd event", err.error || err.body || err);
			}
		});
		client.on("messageReactionRemove", async (reaction: Discord.MessageReaction, user: Discord.User) => {
			try {
				// TODO: filter out echo back?
				const chan = reaction.message.channel;
				if (!await this.bridgeRoom(puppetId, chan)) {
					return;
				}
				const params = this.matrix.getSendParams(puppetId, chan, user);
				if (reaction.emoji.id) {
					const mxc = await this.matrix.getEmojiMxc(
						puppetId, reaction.emoji.name, reaction.emoji.animated, reaction.emoji.id,
					);
					await this.puppet.removeReaction(params, reaction.message.id, mxc || reaction.emoji.name);
				} else {
					await this.puppet.removeReaction(params, reaction.message.id, reaction.emoji.name);
				}
			} catch (err) {
				log.error("Error handling discord messageReactionRemove event", err.error || err.body || err);
			}
		});
		client.on("messageReactionRemoveAll", async (message: Discord.Message) => {
			try {
				const chan = message.channel;
				if (!await this.bridgeRoom(puppetId, chan)) {
					return;
				}
				// alright, let's fetch *an* admin user
				let user: Discord.User;
				if (this.discord.isBridgeableGuildChannel(chan)) {
					const gchan = chan as BridgeableGuildChannel;
					user = gchan.guild.owner ? gchan.guild.owner.user : client.user!;
				} else if (chan instanceof Discord.DMChannel) {
					user = chan.recipient;
				} else if (chan instanceof Discord.GroupDMChannel) {
					user = chan.owner;
				} else {
					user = client.user!;
				}
				const params = this.matrix.getSendParams(puppetId, chan, user);
				await this.puppet.removeAllReactions(params, message.id);
			} catch (err) {
				log.error("Error handling discord messageReactionRemoveAll event", err.error || err.body || err);
			}
		});
		client.on("channelUpdate", async (_, chan: Discord.Channel) => {
			if (!this.discord.isBridgeableChannel(chan)) {
				return;
			}
			const remoteChan = this.matrix.getRemoteRoom(puppetId, chan as BridgeableChannel);
			await this.puppet.updateRoom(remoteChan);
		});
		client.on("guildMemberUpdate", async (oldMember: Discord.GuildMember, newMember: Discord.GuildMember) => {
			const promiseList: Promise<void>[] = [];
			if (oldMember.displayName !== newMember.displayName) {
				promiseList.push((async () => {
					const remoteUser = this.matrix.getRemoteUser(puppetId, newMember);
					await this.puppet.updateUser(remoteUser);
				})());
			}
			// aaaand check for role change
			const leaveRooms = new Set<BridgeableGuildChannel>();
			const joinRooms = new Set<BridgeableGuildChannel>();
			for (const chan of newMember.guild.channels.cache.array()) {
				if (!this.discord.isBridgeableGuildChannel(chan)) {
					continue;
				}
				const gchan = chan as BridgeableGuildChannel;
				if (gchan.members.has(newMember.id)) {
					joinRooms.add(gchan);
				} else {
					leaveRooms.add(gchan);
				}
			}
			for (const chan of leaveRooms) {
				promiseList.push((async () => {
					const params = this.matrix.getSendParams(puppetId, chan, newMember);
					await this.puppet.removeUser(params);
				})());
			}
			for (const chan of joinRooms) {
				promiseList.push((async () => {
					const params = this.matrix.getSendParams(puppetId, chan, newMember);
					await this.puppet.addUser(params);
				})());
			}
			await Promise.all(promiseList);
		});
		client.on("userUpdate", async (_, user: Discord.User) => {
			const remoteUser = this.matrix.getRemoteUser(puppetId, user);
			await this.puppet.updateUser(remoteUser);
		});
		client.on("guildUpdate", async (_, guild: Discord.Guild) => {
			try {
				const remoteGroup = await this.matrix.getRemoteGroup(puppetId, guild);
				await this.puppet.updateGroup(remoteGroup);
				for (const chan of guild.channels.cache.array()) {
					if (!this.discord.isBridgeableGuildChannel(chan)) {
						return;
					}
					const remoteChan = this.matrix.getRemoteRoom(puppetId, chan as BridgeableGuildChannel);
					await this.puppet.updateRoom(remoteChan);
				}
			} catch (err) {
				log.error("Error handling discord guildUpdate event", err.error || err.body || err);
			}
		});
		client.on("relationshipAdd", async (_, relationship: Discord.Relationship) => {
			if (relationship.type === "incoming") {
				const msg = `New incoming friends request from ${relationship.user.username}!

Type \`addfriend ${puppetId} ${relationship.user.id}\` to accept it.`;
				await this.puppet.sendStatusMessage(puppetId, msg);
			}
		});
		client.on("guildMemberAdd", async (member: Discord.GuildMember) => {
			const promiseList: Promise<void>[] = [];
			for (const chan of member.guild.channels.cache.array()) {
				if ((await this.bridgeRoom(puppetId, chan)) && chan.members.has(member.id)) {
					promiseList.push((async () => {
						const params = this.matrix.getSendParams(puppetId, chan as BridgeableGuildChannel, member);
						await this.puppet.addUser(params);
					})());
				}
			}
			await Promise.all(promiseList);
		});
		client.on("guildMemberRemove", async (member: Discord.GuildMember) => {
			const promiseList: Promise<void>[] = [];
			for (const chan of member.guild.channels.cache.array()) {
				if (this.discord.isBridgeableGuildChannel(chan)) {
					promiseList.push((async () => {
						const params = this.matrix.getSendParams(puppetId, chan as BridgeableGuildChannel, member);
						await this.puppet.removeUser(params);
					})());
				}
			}
			await Promise.all(promiseList);
		});

		client.on("disconnect", async () => {
			log.error("client disconnected")
			await this.puppet.setBridgeStatus("UNKNOWN_ERROR", puppetId, "discord-disconnected")
		})
		const TWO_MIN = 120000;
		this.puppets[puppetId] = {
			client,
			data,
			deletedMessages: new ExpireSet(TWO_MIN),
		};

		// tslint:disable-next-line:no-magic-numbers
		const NUM_ATTEMPTS = 5;
		let timeout = 5;
		for (let i = 0; i < NUM_ATTEMPTS; ++i) {
			try {
				log.info("Attempting client login with", data.bot, "and", data.token?.substring(0, 4));
				
				await client.login(data.token, data.bot || false);
				log.info("login successfull");
				return; // Success!
			} catch (e) {
				log.error(`Failed to log in puppetId ${puppetId}:`, e);
				await this.puppet.sendStatusMessage(puppetId, "Failed to connect: " + e);
				this.puppet.trackConnectionStatus(puppetId, false);

				if (e.code === "TOKEN_INVALID") {
					log.error("TOKEN_INVALID from Discord, not retrying login, user needs to log in again");

					// Check if we even have a userId. If we don't, don't keep this puppet around
					const puppet = await this.puppet.puppetStore.get(puppetId);
					if (puppet) {
						if (!puppet.userId) {
							log.error("After TOKEN_INVALID, puppet does not have userId, deleting puppet from DB");
							this.puppet.puppetStore.delete(puppetId);
							this.deletePuppet(puppetId);
							return;
						}
					}
					await this.puppet.setBridgeStatus("BAD_CREDENTIALS", puppetId);
					break;
				}

				if (i == NUM_ATTEMPTS - 1) {
					// Check if we even have a userId. If we don't, don't keep this puppet around
					const puppet = await this.puppet.puppetStore.get(puppetId);
					if (puppet) {
						if (!puppet.userId) {
							log.error("After several login attempts, puppet does not have userId, deleting puppet from DB");
							this.puppet.puppetStore.delete(puppetId);
							this.deletePuppet(puppetId);
							return;
						}
					}
					await this.puppet.setBridgeStatus("UNKNOWN_ERROR",  puppetId)
				} else {
					await this.puppet.setBridgeStatus("TRANSIENT_DISCONNECT",  puppetId)
				}

			}
			// Login failed, wait 30 seconds before trying again.
			log.warn(`Sleeping ${timeout} seconds before attempting to login again, ${NUM_ATTEMPTS - i - 1} attempts left`);
			// tslint:disable-next-line:no-magic-numbers
			log.info("starting sleep")
			await Util.sleep(timeout * 1000);
			log.info("ending sleep")
			timeout = timeout * 2
		}
	}

	public async deletePuppet(puppetId: number) {
		log.info(`Got signal to quit Puppet: puppetId=${puppetId}`);
		const p = this.puppets[puppetId];
		if (!p) {
			return; // nothing to do
		}
		p.client.destroy();
		delete this.puppets[puppetId];
		if (Object.keys(this.puppets).length === 0) {
			await this.puppet.setBridgeStatus("UNCONFIGURED")
		}
	}

	public async listUsers(puppetId: number): Promise<IRetList[]> {
		const retUsers: IRetList[] = [];
		const retGuilds: IRetList[] = [];
		const p = this.puppets[puppetId];
		if (!p) {
			return [];
		}
		const blacklistedIds = [p.client.user!.id, "1"];
		for (const guild of p.client.guilds.cache.array()) {
			retGuilds.push({
				category: true,
				name: guild.name,
			});
			for (const member of guild.members.cache.array()) {
				if (!blacklistedIds.includes(member.user.id)) {
					retGuilds.push({
						name: member.user.username,
						id: member.user.id,
					});
				}
			}
		}

		for (const user of p.client.users.cache.array()) {
			const found = retGuilds.find((element) => element.id === user.id);
			if (!found && !blacklistedIds.includes(user.id)) {
				retUsers.push({
					name: user.username,
					id: user.id,
				});
			}
		}

		return retUsers.concat(retGuilds);
	}
	
	public async getGroupInfo(puppetId: number, groupId: string): Promise<IGroupInfo | null> {
		if (puppetId === -1) {
			for (const p of Object.values(this.puppets)) {
				const guild = await p.client.guilds.fetch(groupId)
				if (guild) {
					return {
						groupId: guild.id,
						name: guild.name,

					}
				}
			}
			return null;
		}
		const p = this.puppets[puppetId];
		if (!p) {
			return null;
		}
		const guild = await p.client.guilds.fetch(groupId)
		if (!guild) {
			return null;
		}
		return {
		 	groupId: guild.id,
		 	name: guild.name,
		}
	}

	public async checkIfAlreadyRegistered(data: IPuppetData): Promise<boolean> {
		const puppets = await this.puppet.provisioner.getAll();
		const tokenStart = typeof data.token === "string" ? data.token.slice(0,25): undefined
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

	public async getUserIdsInRoom(room: IRemoteRoom): Promise<Set<string> | null> {
		const chan = await this.discord.getDiscordChan(room);
		if (!chan) {
			return null;
		}
		const users = new Set<string>();
		if (chan instanceof Discord.DMChannel) {
			users.add(chan.recipient.id);
			return users;
		}
		if (chan instanceof Discord.GroupDMChannel) {
			for (const recipient of chan.recipients.array()) {
				users.add(recipient.id);
			}
			return users;
		}
		if (this.discord.isBridgeableGuildChannel(chan)) {
			// chan.members already does a permission check, yay!
			const gchan = chan as BridgeableGuildChannel;
			for (const member of gchan.members.array()) {
				users.add(member.id);
			}
			return users;
		}
		return null;
	}

	public async updateUserInfo(puppetId: number) {
		const p = this.puppets[puppetId];
		if (!p || !p.data.syncProfile) {
			return;
		}
		const userInfo = await this.puppet.getPuppetMxidInfo(puppetId);
		if (userInfo) {
			if (userInfo.name) {
				await this.handlePuppetName(puppetId, userInfo.name);
			}
			if (userInfo.avatarUrl) {
				await this.handlePuppetAvatar(puppetId, userInfo.avatarUrl, userInfo.avatarMxc as string);
			}
		}
	}

	public async bridgeRoom(puppetId: number, chan: Discord.Channel): Promise<boolean> {
		if (["dm", "group"].includes(chan.type)) {
			return true; // we handle all dm and group channels
		}
		if (!this.discord.isBridgeableChannel(chan)) {
			return false; // we only handle text and news things
		}
		if (this.puppets[puppetId] && this.puppets[puppetId].data.bridgeAll) {
			return true; // we want to bridge everything anyways, no need to hit the store
		}
		if (this.discord.isBridgeableGuildChannel(chan)) {
			// we have a guild text channel, maybe we handle it!
			const gchan = chan as BridgeableGuildChannel;
			if (await this.store.isGuildBridged(puppetId, gchan.guild.id)) {
				return true;
			}
			// maybe it is a single channel override?
			return await this.store.isChannelBridged(puppetId, gchan.id);
		}
		return false;
	}

	public getFilenameForMedia(filename: string, mimetype: string): string {
		let ext = "";
		const mimeExt = mime.getExtension(mimetype);
		if (mimeExt) {
			ext = "." + ({
				oga: "ogg",
			}[mimeExt] || mimeExt);
		}
		if (filename) {
			if (path.extname(filename) !== "") {
				return filename;
			}
			return path.basename(filename) + ext;
		}
		return "matrix-media" + ext;
	}
}
