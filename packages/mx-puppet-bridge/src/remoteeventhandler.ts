/*
Copyright 2020 mx-puppet-bridge
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

import { Log } from "./log";
import { PuppetBridge } from "./puppetbridge";
import { Util } from "./util";
import { TimedCache } from "./structures/timedcache";
import { IRemoteUser, IReceiveParams, IMessageEvent, MatrixPresence } from "./interfaces";
import {
	TextualMessageEventContent, FileMessageEventContent, FileWithThumbnailInfo, MatrixClient, DimensionalFileInfo,
	VideoFileInfo, TimedFileInfo, MessageEvent, MessageEventContent,
} from "@sorunome/matrix-bot-sdk";
import * as escapeHtml from "escape-html";
import * as unescapeHtml from "unescape";
import * as prometheus from "prom-client";
import { encode as blurhashEncode } from "blurhash";
import * as Canvas from "canvas";

const log = new Log("RemoteEventHandler");

// tslint:disable no-magic-numbers
const GHOST_PUPPET_LEAVE_TIMEOUT = 1000 * 60 * 60;
const PUPPET_INVITE_CACHE_LIFETIME = 1000 * 60 * 60 * 24;
// tslint:enable no-magic-numbers

interface ISendInfo {
	client: MatrixClient;
	mxid: string;
}

export class RemoteEventHandler {
	private ghostInviteCache: TimedCache<string, boolean>;

	constructor(
		private bridge: PuppetBridge,
	) {
		this.ghostInviteCache = new TimedCache(PUPPET_INVITE_CACHE_LIFETIME);
		this.bridge.metrics.remoteUpdateBucket = new prometheus.Histogram({
			name: "bridge_remote_update_seconds",
			help: "Time spent processing updates from the remote network, by protocol and type",
			labelNames: ["protocol", "type"],
			// tslint:disable-next-line no-magic-numbers
			buckets: [0.002, 0.005, 0.01, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 7, 10],
		});
		this.bridge.metrics.incomingMessages = new prometheus.Counter({
			name: "bridge_matrix_matrix_messages",
			help: "Counter for all messages",
			labelNames: ["protocol", "room", "puppet"],
		});

	}

	public async setUserPresence(user: IRemoteUser, presence: MatrixPresence) {
		if (this.bridge.protocol.features.presence && this.bridge.config.presence.enabled) {
			log.verbose(`Setting user presence for userId=${user.userId} to ${presence}`);
			const client = await this.bridge.userSync.maybeGetClient(user);
			if (!client) {
				return;
			}
			const userId = await client.getUserId();
			this.bridge.presenceHandler.set(userId, presence);
		}
	}

	public async setUserStatus(user: IRemoteUser, status: string) {
		if (this.bridge.protocol.features.presence && this.bridge.config.presence.enabled) {
			log.verbose(`Setting user status for userId=${user.userId} to ${status}`);
			const client = await this.bridge.userSync.maybeGetClient(user);
			if (!client) {
				return;
			}
			const userId = await client.getUserId();
			this.bridge.presenceHandler.setStatus(userId, status);
		}
	}

	public async setUserTyping(params: IReceiveParams, typing: boolean) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		log.verbose(`Setting user typing for userId=${params.user.userId} in roomId=${params.room.roomId} to ${typing}`);
		const ret = await this.maybePrepareSend(params);
		if (!ret) {
			log.verbose("User/Room doesn't exist, ignoring...");
			return;
		}
		if (await this.bridge.typingHandler.deduplicator.dedupe(
			`${params.room.puppetId};${params.room.roomId}`, params.user.userId, undefined, typing.toString(), false,
		)) {
			return;
		}
		await this.bridge.typingHandler.set(await ret.client.getUserId(), ret.mxid, typing);
	}

	public async sendReadReceipt(params: IReceiveParams) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		log.verbose(`Got request to send read indicators for userId=${params.user.userId} in roomId=${params.room.roomId}`);
		const ret = await this.maybePrepareSend(params);
		if (!ret || !params.eventId) {
			log.verbose("User/Room doesn't exist, ignoring...");
			return;
		}
		const origEventIdIds = await this.bridge.eventSync.getMatrix(params.room, params.eventId);
		for (const origEventId of origEventIdIds) {
			await ret.client.sendReadReceipt(ret.mxid, origEventId);
		}
	}

	public async addUser(params: IReceiveParams) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		const userIds = await this.bridge.namespaceHandler.getRoomPuppetUserIds(params.room);
		if (userIds.has(params.user.userId)) {
			return;
		}
		log.info(`Got request to add userId=${params.user.userId} to roomId=${params.room.roomId}` +
			` puppetId=${params.room.puppetId}`);
		const mxid = await this.bridge.roomSync.maybeGetMxid(params.room);
		if (!mxid) {
			return;
		}
		await this.bridge.roomSync.addGhost(params.user, mxid);
	}

	public async removeUser(params: IReceiveParams) {
		log.info(`Got request to remove userId=${params.user.userId} from roomId=${params.room.roomId}` +
			` puppetId=${params.room.puppetId}`);
		const ret = await this.maybePrepareSend(params);
		if (!ret) {
			return;
		}
		const userId = await ret.client.getUserId();
		if (!this.bridge.AS.isNamespacedUser(userId)) {
			return;
		}
		await this.bridge.roomSync.maybeLeaveGhost(ret.mxid, userId);
	}

	public async sendMessage(params: IReceiveParams, opts: IMessageEvent) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		const stopTimer = this.bridge.metrics.remoteUpdateBucket.startTimer({
			protocol: this.bridge.protocol.id,
		});
		log.info(`Received message from ${params.user.userId} to send to ${params.room.roomId}`);
		this.preprocessMessageEvent(opts);
		const { client, mxid } = await this.prepareSend(params);
		let msgtype = "m.text";
		if (opts.emote) {
			msgtype = "m.emote";
		} else if (opts.notice) {
			msgtype = "m.notice";
		}
		const send: TextualMessageEventContent = {
			msgtype,
			body: opts.body,
		};
		(send as any).source = this.bridge.protocol.id; // tslint:disable-line no-any
		if (opts.formattedBody) {
			send.format = "org.matrix.custom.html";
			send.formatted_body = opts.formattedBody;
		}
		if (params.externalUrl) {
			send.external_url = params.externalUrl;
		}
		const qs = this.getQueryForRemoteEvent(params);
		const matrixEventId = await client.sendMessage(mxid, send, qs);
		if (matrixEventId && params.eventId) {
			await this.bridge.eventSync.insert(params.room, matrixEventId, params.eventId);
		}
		// aaand stop typing
		await this.bridge.typingHandler.set(await client.getUserId(), mxid, false);
		this.bridge.metrics.incomingMessages.inc({
			protocol: this.bridge.protocol.id,
			room: params.room.roomId,
			puppet: params.room.puppetId,
		});
		stopTimer({ type: msgtype });
	}

	public async sendEdit(params: IReceiveParams, eventId: string, opts: IMessageEvent, ix: number = 0) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		const stopTimer = this.bridge.metrics.remoteUpdateBucket.startTimer({
			protocol: this.bridge.protocol.id,
		});
		log.info(`Received edit from ${params.user.userId} to send to ${params.room.roomId}`);
		this.preprocessMessageEvent(opts);
		const { client, mxid } = await this.prepareSend(params);
		let msgtype = "m.text";
		if (opts.emote) {
			msgtype = "m.emote";
		} else if (opts.notice) {
			msgtype = "m.notice";
		}
		const origEventIdIds = await this.bridge.eventSync.getMatrix(params.room, eventId);
		if (ix < 0) {
			// negative indexes are from the back
			ix = origEventIdIds.length + ix;
		}
		if (ix >= origEventIdIds.length) {
			// sanity check on the index
			ix = 0;
		}
		const origEventId = origEventIdIds[ix];
		// this object is set to any-type as the interfaces don't do edits yet
		const send = {
			"msgtype": msgtype,
			"body": `* ${opts.body}`,
			"source": this.bridge.protocol.id,
			"m.new_content": {
				body: opts.body,
				msgtype,
			},
		} as any; // tslint:disable-line no-any
		if (origEventId) {
			send["m.relates_to"] = {
				event_id: origEventId,
				rel_type: "m.replace",
			};
		} else {
			log.warn("Couldn't find event, sending as normal message...");
		}
		if (opts.formattedBody) {
			send.format = "org.matrix.custom.html";
			send.formatted_body = `* ${opts.formattedBody}`;
			send["m.new_content"].format = "org.matrix.custom.html";
			send["m.new_content"].formatted_body = opts.formattedBody;
		}
		if (params.externalUrl) {
			send.external_url = params.externalUrl;
			send["m.new_content"].external_url = params.externalUrl;
		}
		const qs = this.getQueryForRemoteEvent(params);
		const matrixEventId = await client.sendMessage(mxid, send, qs);
		if (matrixEventId && params.eventId) {
			await this.bridge.eventSync.insert(params.room, matrixEventId, params.eventId);
		}
		// aaand stop typing
		await this.bridge.typingHandler.set(await client.getUserId(), mxid, false);
		stopTimer({ type: msgtype });
	}

	public async sendRedact(params: IReceiveParams, eventId: string) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		log.info(`Received redact from ${params.user.userId} to send to ${params.room.roomId}`);
		const { client, mxid } = await this.prepareSend(params);
		const origEventIdIds = await this.bridge.eventSync.getMatrix(params.room, eventId);
		for (const origEventId of origEventIdIds) {
			await this.bridge.redactEvent(client, mxid, origEventId);
		}
	}

	public async sendReply(params: IReceiveParams, eventId: string, opts: IMessageEvent) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		const stopTimer = this.bridge.metrics.remoteUpdateBucket.startTimer({
			protocol: this.bridge.protocol.id,
		});
		log.info(`Received reply from ${params.user.userId} to send to ${params.room.roomId}`);
		this.preprocessMessageEvent(opts);
		const { client, mxid } = await this.prepareSend(params);
		let msgtype = "m.text";
		if (opts.emote) {
			msgtype = "m.emote";
		} else if (opts.notice) {
			msgtype = "m.notice";
		}
		const origEventIds = await this.bridge.eventSync.getMatrix(params.room, eventId);
		const origEventId = origEventIds[0];

		// this send object needs to be any-type, as the interfaces don't do replies yet
		const send = {
			msgtype,
			body: opts.body,
			format: "org.matrix.custom.html",
			formatted_body: opts.formattedBody ? opts.formattedBody : escapeHtml(opts.body).replace(/\n/g, "<br>"),
			source: this.bridge.protocol.id,
		} as any; // tslint:disable-line no-any
		if (opts.formattedBody) {
			send.format = "org.matrix.custom.html";
			send.formatted_body = opts.formattedBody;
		}
		if (origEventId) {
			send["m.relates_to"] = {
				"m.in_reply_to": {
					event_id: origEventId,
				},
			};
			try {
				const info = await this.bridge.getEventInfo(mxid, origEventId, client);
				if (info) {
					if (info.message) {
						if (!info.message.formattedBody) {
							info.message.formattedBody = escapeHtml(info.message.body).replace(/\n/g, "<br>");
						}
						const bodyParts = this.preprocessBody(info.message.body).split("\n");
						bodyParts[0] = `${info.message.emote ? "* " : ""}<${this.preprocessBody(info.user.mxid)}> ${bodyParts[0]}`;
						send.body = `${bodyParts.map((l) => `> ${l}`).join("\n")}\n\n${send.body}`;
						const matrixReplyRegex = /^<mx-reply>.*<\/mx-reply>/gs;
						const messageWithoutNestedReplies = info.message.formattedBody?.replace(matrixReplyRegex, "");

						const richHeader = `<mx-reply><blockquote>
	<a href="https://matrix.to/#/${mxid}/${origEventId}">In reply to</a>
	${info.message.emote ? "* " : ""}<a href="https://matrix.to/#/${info.user.mxid}">${info.user.mxid}</a>
	<br>${messageWithoutNestedReplies}
</blockquote></mx-reply>`;
						send.formatted_body = richHeader + send.formatted_body;
					} else if (info.file) {
						let msg = {
							image: "an image",
							audio: "an audio file",
							video: "a video",
							sticker: "a sticker",
						}[info.file.type];
						if (!msg) {
							msg = "a file";
						}
						const plainHeader = `> <${this.preprocessBody(info.user.mxid)}> sent ${msg}.\n\n`;
						send.body = plainHeader + send.body;
						const richHeader = `<mx-reply><blockquote>
	<a href="https://matrix.to/#/${mxid}/${origEventId}">In reply to</a>
	<a href="https://matrix.to/#/${info.user.mxid}">${info.user.mxid}</a>
	<br>sent ${msg}.
</blockquote></mx-reply>`;
						send.formatted_body = richHeader + send.formatted_body;
					}
				}
			} catch (err) {
				log.warn("Failed to add reply fallback", err.error || err.body || err);
			}
		} else {
			log.warn("Couldn't find event, sending as normal message...");
		}
		if (params.externalUrl) {
			send.external_url = params.externalUrl;
		}
		send.formatted_body = send.formatted_body.replace(/[\n\t]/g, "");
		const qs = this.getQueryForRemoteEvent(params);
		const matrixEventId = await client.sendMessage(mxid, send, qs);
		if (matrixEventId && params.eventId) {
			await this.bridge.eventSync.insert(params.room, matrixEventId, params.eventId);
		}
		// aaand stop typing
		await this.bridge.typingHandler.set(await client.getUserId(), mxid, false);
		stopTimer({ type: msgtype });
	}

	public async sendReaction(params: IReceiveParams, eventId: string, reaction: string) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		
		if (await this.bridge.reactionHandler.deduplicator.dedupe(`${params.room.puppetId};${params.room.roomId};${eventId};add`, params.user.userId, undefined, reaction)) {
			return;
		}
		const { client, mxid } = await this.prepareSend(params);
		await this.bridge.reactionHandler.addRemote(params, eventId, reaction, client, mxid);
	}

	public async removeReaction(params: IReceiveParams, eventId: string, reaction: string) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		if (await this.bridge.reactionHandler.deduplicator.dedupe(`${params.room.puppetId};${params.room.roomId};${eventId};remove`, params.user.userId, undefined, reaction)) {
			return;
		}
		const { client, mxid } = await this.prepareSend(params);
		await this.bridge.reactionHandler.removeRemote(params, eventId, reaction, client, mxid);
	}

	public async removeAllReactions(params: IReceiveParams, eventId: string) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		const { client, mxid } = await this.prepareSend(params);
		await this.bridge.reactionHandler.removeRemoteAllOnMessage(params, eventId, client, mxid);
	}

	public async sendFileByType(msgtype: string, params: IReceiveParams, thing: string | Buffer, name?: string, origEventId?: string) {
		if (await this.bridge.namespaceHandler.isMessageBlocked(params)) {
			return;
		}
		const stopTimer = this.bridge.metrics.remoteUpdateBucket.startTimer({
			protocol: this.bridge.protocol.id,
		});
		log.info(`Received file to send from ${params.user.userId} in ${params.room.roomId}.`);
		log.verbose(`thing=${typeof thing === "string" ? thing : "<Buffer>"} name=${name}`);
		if (!name) {
			name = "remote_file";
		}
		const { client, mxid } = await this.prepareSend(params);
		let buffer: Buffer;
		if (typeof thing === "string") {
			buffer = await Util.DownloadFile(thing);
		} else {
			buffer = thing;
		}
		const mimetype = Util.GetMimeType(buffer);
		if (msgtype === "detect") {
			if (mimetype) {
				const type = mimetype.split("/")[0];
				msgtype = {
					audio: "m.audio",
					image: "m.image",
					video: "m.video",
				}[type];
				if (!msgtype) {
					msgtype = "m.file";
				}
			} else {
				msgtype = "m.file";
			}
		}
		const fileMxc = await this.bridge.uploadContent(
			client,
			buffer,
			mimetype,
			name,
		);
		const info: FileWithThumbnailInfo = {
			mimetype,
			size: buffer.byteLength,
		};
		// alright, let's add some stuffs to the different msgtypes
		if (msgtype === "m.image") {
			try {
				const i = info as DimensionalFileInfo;
				const data = await Util.ffprobe(buffer);
				const imageData = data.streams.find((e) => e.codec_type === "video");
				if (typeof imageData.width === "number") {
					i.w = imageData.width;
				}
				if (typeof imageData.height === "number") {
					i.h = imageData.height;
				}
				try {
					const orientation = await Util.getExifOrientation(buffer);
					const FIRST_EXIF_ROTATED = 5;
					if (orientation > FIRST_EXIF_ROTATED) {
						// flip width and height
						const tmp = i.w;
						i.w = i.h;
						i.h = tmp;
					}
				} catch (err) {
					log.debug("Error fetching exif orientation for image", err);
				}
				const BLURHASH_CHUNKS = 4;
				const image = await new Promise<Canvas.Image>((resolve, reject) => {
					const img = new Canvas.Image();
					img.onload = () => resolve(img);
					img.onerror = (...args) => reject(args);
					img.src = "data:image/png;base64," + buffer.toString("base64");
				});
				let drawWidth = image.width;
				let drawHeight = image.height;
				const drawMax = 50;
				if (drawWidth > drawMax || drawHeight > drawMax) {
					if (drawWidth > drawHeight) {
						drawHeight = Math.round(drawMax * (drawHeight / drawWidth));
						drawWidth = drawMax;
					} else {
						drawWidth = Math.round(drawMax * (drawWidth / drawHeight));
						drawHeight = drawMax;
					}
				}

				const canvas = Canvas.createCanvas(drawWidth, drawHeight);
				const context = canvas.getContext("2d");
				if (context) {
					context.drawImage(image, 0, 0, drawWidth, drawHeight);
					const blurhashImageData = context.getImageData(0, 0, drawWidth, drawHeight);
					// tslint:disable-next-line no-any
					(i as any)["xyz.amorgan.blurhash"] = blurhashEncode(
						blurhashImageData.data, drawWidth, drawHeight, BLURHASH_CHUNKS, BLURHASH_CHUNKS,
					);
				}
			} catch (err) {
				log.debug("Error adding information for image", err);
			}
		}
		if (msgtype === "m.video") {
			try {
				const i = info as VideoFileInfo;
				const data = await Util.ffprobe(buffer);
				const imageData = data.streams.find((e) => e.codec_type === "video");
				if (typeof imageData.width === "number") {
					i.w = imageData.width;
				}
				if (typeof imageData.height === "number") {
					i.h = imageData.height;
				}
				const duration = Number(data.format.duration);
				if (!isNaN(duration)) {
					i.duration = Math.round(duration * 1000);
				}
			} catch (err) {
				log.debug("Error adding inromation for video", err);
			}
		}
		if (msgtype === "m.audio") {
			try {
				const i = info as TimedFileInfo;
				const data = await Util.ffprobe(buffer);
				let duration = Number(data.format.duration);
				if (!isNaN(duration)) {
					i.duration = Math.round(duration * 1000);
				} else {
					duration = Number(data.format.tags.TLEN);
					if (!isNaN(duration)) {
						i.duration = Math.round(duration);
					}
				}
			} catch (err) {
				log.debug("Error adding inromation for video", err);
			}
		}
		const sendData: FileMessageEventContent = {
			body: name,
			info,
			msgtype,
			url: fileMxc,
		};
		if (origEventId) {
			sendData["m.relates_to"] = {
				"m.in_reply_to": {
					event_id: origEventId,
				},
			};
		}
		(sendData as any).source = this.bridge.protocol.id; // tslint:disable-line no-any
		if (typeof thing === "string") {
			sendData.external_url = thing;
		}
		if (params.externalUrl) {
			sendData.external_url = params.externalUrl;
		}
		const qs = this.getQueryForRemoteEvent(params);
		const matrixEventId = await client.sendMessage(mxid, sendData, qs);
		if (matrixEventId && params.eventId) {
			await this.bridge.eventSync.insert(params.room, matrixEventId, params.eventId);
		}
		// aaand stop typing
		await this.bridge.typingHandler.set(await client.getUserId(), mxid, false);
		stopTimer({ type: msgtype });
	}

	public async sendReplyFileByType(msgtype: string, replyTs: string, params: IReceiveParams, thing: string | Buffer, name?: string) {
		const origEventIds = await this.bridge.eventSync.getMatrix(params.room, replyTs);
		const origEventId = origEventIds[0];
		await this.sendFileByType(msgtype, params, thing, name, origEventId);
	}

	private async maybePrepareSend(params: IReceiveParams): Promise<ISendInfo | null> {
		log.verbose(`Maybe preparing room`, params);
		const mxid = await this.bridge.roomSync.maybeGetMxid(params.room);
		if (!mxid) {
			return null;
		}
		const client = await this.bridge.userSync.maybeGetClient(params.user);
		if (!client) {
			return null;
		}
		return { client, mxid };
	}

	private async prepareSend(params: IReceiveParams): Promise<ISendInfo> {
		log.verbose(`Preparing room`, params);
		const puppetData = await this.bridge.provisioner.get(params.room.puppetId);
		if (!puppetData) {
			throw new Error("puppetData wasn't found, THIS SHOULD NEVER HAPPEN!");
		}
		const puppetMxid = puppetData.puppetMxid;
		const client = await this.bridge.userSync.getClient(params.user);
		const userId = await client.getUserId();
		let { mxid, created } = await this.bridge.roomSync.getMxid(params.room, undefined, false);
		if (!mxid) {
			// alright, the room doesn't exist yet....time to create it!
			const retCall = await this.bridge.roomSync.getMxid(params.room, client);
			mxid = retCall.mxid;
			created = retCall.created;
		}

		// ensure that the intent is in the room
		if (this.bridge.AS.isNamespacedUser(userId)) {
			log.silly("Joining ghost to room...");
			const intent = this.bridge.AS.getIntentForUserId(userId);
			await intent.ensureRegisteredAndJoined(mxid);
			// if the ghost was ourself, leave it again
			if (puppetData.userId === params.user.userId) {
				const delayedKey = `${userId}_${mxid}`;
				this.bridge.delayedFunction.set(delayedKey, async () => {
					await this.bridge.roomSync.maybeLeaveGhost(mxid!, userId);
				}, GHOST_PUPPET_LEAVE_TIMEOUT);
			}
			// set the correct m.room.member override if the room just got created
			if (created) {
				log.verbose("Maybe applying room membership overrides");
				await this.bridge.userSync.setRoomOverride(params.user, params.room.roomId, null, client);
			}
		}

		// ensure our puppeted user is in the room
		if (puppetData.autoinvite) {
			const cacheKey = `${params.room.puppetId}_${mxid}`;
			try {
				const cache = this.ghostInviteCache.get(cacheKey);
				if (!cache) {
					let inviteClient = await this.bridge.roomSync.getRoomOp(mxid);
					if (!inviteClient) {
						inviteClient = client;
					}
					// we can't really invite ourself...
					if (await inviteClient.getUserId() !== puppetMxid) {
						// we just invited if we created, don't try to invite again
						if (!created) {
							log.silly("Inviting puppet to room...");
							await inviteClient.inviteUser(puppetMxid, mxid);
						}
						this.ghostInviteCache.set(cacheKey, true);

						// let's try to also join the room, if we use double-puppeting
						const puppetClient = await this.bridge.userSync.getPuppetClient(params.room.puppetId);
						if (puppetClient) {
							log.silly("Joining the room...");
							await puppetClient.joinRoom(mxid);
						}
					}
				}
			} catch (err) {
				if (err.body && err.body.errcode === "M_FORBIDDEN" && err.body.error.includes("is already in the room")) {
					log.verbose("Failed to invite user, as they are already in there");
					this.ghostInviteCache.set(cacheKey, true);
				} else {
					log.warn("Failed to invite user:", err.error || err.body || err);
				}
			}
		}

		return { client, mxid };
	}

	private getQueryForRemoteEvent(params: IReceiveParams) {
		return params.ts ? { ts: params.ts } : null;
	}

	private preprocessBody(body: string): string {
		for (const homeserver of this.bridge.config.bridge.stripHomeservers) {
			const urlRegex = homeserver.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
			body = body.replace(new RegExp(`@([\x21-\x39\x3b-\x7e]+):${urlRegex}`, "g"), "@$1");
		}
		return body;
	}

	private preprocessMessageEvent(opts: IMessageEvent) {
		opts.body = this.preprocessBody(opts.body);
		if (!opts.formattedBody) {
			return;
		}
		const html = opts.formattedBody.toLowerCase();
		let stripPTags = (html.match(/<p[^>]*>/g) || []).length <= 1;
		if (stripPTags) {
			const otherBlockTags = ["table", "pre", "ol", "ul", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "div", "hr"];
			for (const tag of otherBlockTags) {
				if (html.match(new RegExp(`</?\\s*${tag}\\s*/?>`))) {
					stripPTags = false;
					break;
				}
			}
		}
		if (stripPTags) {
			opts.formattedBody = opts.formattedBody.replace(/<p[^>]*>/ig, "").replace(/<\/p>/ig, "");
		}
		if (unescapeHtml(opts.formattedBody.trim().replace(/<br\s*\/?>/gi, "\n").trim()) === opts.body.trim()) {
			delete opts.formattedBody;
		}
	}
}
