/*
Copyright 2020 soru-slack-client
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

import { Client } from "./client";
import { Base, IIconData, ICreatorValue } from "./base";
import { Team } from "./team";
import { User } from "./user";
import { Util } from "../util";
import { Logger } from "../logger";

const log = new Logger("Channel");

type ChannelTypes = "channel" | "group" | "mpim" | "im" | "unknown";

export interface IChannelData {
	id: string;
	name?: string;
	is_channel: boolean;
	is_group: boolean;
	is_mpim: boolean;
	is_im: boolean;
	is_open?: boolean;
	is_member?: boolean;
	is_archived?: boolean;
	last_read?: string;
	topic?: ICreatorValue;
	purpose?: ICreatorValue;
	private?: boolean;
	is_private?: boolean;
	team_id?: string;
	shared_team_ids?: string[];
	user?: string;
	num_members?: number;
}

interface ISendMessage {
	text: string;
	blocks?: any[]; // tslint:disable-line no-any
}


type SendableType = ISendMessage | string;


export interface ISendOpts {
	username?: string | null;
	iconUrl?: string | null;
	iconEmoji?: string | null;
	threadTs?: string | null;
	asUser?: boolean | null;
}

export class Channel extends Base {
	public members: Map<string, User> = new Map();
	public id: string;
	public name: string | null = null;
	public type: ChannelTypes;
	public topic: string | null = null;
	public purpose: string | null = null;
	public private: boolean;
	public team: Team;
	public partial = true;
	public opened: boolean = false;
	public archived: boolean = false;
	public isMember: boolean = false;
	public lastRead: string | null = null;
	private joined = false;
	constructor(client: Client, data: IChannelData) {
		super(client);
		const teamId = data.team_id || (data.shared_team_ids && data.shared_team_ids[0]);
		if (!teamId) {
			throw new Error("no associated team!");
		}
		const team = this.client.teams.get(teamId);
		if (!team) {
			throw new Error("team not found!");
		}
		this.team = team;
		this._patch(data);
	}

	public get fullId(): string {
		return `${this.team.id}${this.client.separator}${this.id}`;
	}

	public _patch(data: IChannelData) {
		this.id = data.id;
		this.name = data.name || null;
		let type: ChannelTypes = "unknown";
		if (data.is_channel) {
			type = "channel";
		} else if (data.is_mpim) {
			type = "mpim";
		} else if (data.is_group) {
			type = "group";
		} else if (data.is_im) {
			type = "im";
		}
		this.type = type;
		if (data.hasOwnProperty("is_member")) {
			this.isMember = Boolean(data.is_member && ["group", "channel"].includes(type));
		}
		if (data.hasOwnProperty("is_archived")) {
			this.archived = data.is_archived!;
		}
		if (data.hasOwnProperty("is_open")) {
			this.opened = Boolean(data.is_open && ["im", "mpim"].includes(type));
		}
		if (data.hasOwnProperty("last_read")) {
			this.lastRead = data.last_read!;
		}
		if (data.hasOwnProperty("topic")) {
			this.topic = data.topic!.value;
		}
		if (data.hasOwnProperty("purpose")) {
			this.purpose = data.purpose!.value;
		}
		this.private = Boolean(data.is_im || data.private || data.is_private);
		if (data.hasOwnProperty("user")) {
			const userObj = this.team.users.get(data.user!);
			if (userObj) {
				this.members.set(userObj.id, userObj);
			}
		}
	}

	public async load() {
		let loadMembers = false;
		// first load the info
		{
			const ret = await this.client.web(this.team.id).conversations.info({
				channel: this.id,
				include_num_members: true,
			});
			if (!ret || !ret.ok || !ret.channel) {
				throw new Error("Bad response");
			}
			const channel = ret.channel as IChannelData;
			this._patch(channel);
			if (channel.num_members! > 0) {
				loadMembers = true;
			}
		}
		// now load the members
		if (loadMembers) {
			let cursor: string | undefined;
			do {
				const ret = await this.client.web(this.team.id).conversations.members({
					channel: this.id,
					limit: 1000,
					cursor,
				});
				if (!ret || !ret.ok || !ret.members) {
					throw new Error("Bad response");
				}
				for (const memberId of ret.members as string[]) {
					const userObj = this.team.users.get(memberId);
					if (userObj) {
						this.members.set(userObj.id, userObj);
					}
				}
				cursor = ret.response_metadata && ret.response_metadata.next_cursor;
			} while (cursor);
		}
		this.partial = false;
	}

	public async join() {
		if (["im", "mpim", "group"].includes(this.type) || this.private || this.joined) {
			return;
		}
		await this.client.web(this.team.id).conversations.join({
			channel: this.id,
		});
		this.joined = true;
	}

	public isAlreadyRead(ts: string): boolean {
		if (!this.lastRead) {
			return false;
		}
		return parseFloat(this.lastRead) >= parseFloat(ts);
	}

	public async mark(ts: string) {
		if (this.isAlreadyRead(ts) || (!this.isMember) && !["im", "mpim"].includes(this.type)) {
			if((!this.isMember) && !["im", "mpim"].includes(this.type)) {
				log.warn("cannot mark dm in", this.id, " at ts", ts, "because you're not a member and type is", this.type)
			} else {
				log.warn("cannot mark dm in", this.id, "at ts", ts, "because its already read")
			}
			return;
		}
		const res = await this.client.web(this.team.id).conversations.mark({
			channel: this.id,
			ts,
		});
		if (res.ok) {
			this.lastRead = ts;
			log.info("marked event id", ts, "as read")
		} else {
			log.warn("failed to mark", this.id, "as read")
		}
		// im and mpim chats disappears after mark
		// if they were opened needs open them again
		if (this.opened) {
			log.info("reopening im/mpim for", this.id)
			await this.reopen();
		} else if (this.type === "mpim" || this.type === "im"){ 
			log.info("Not reopening im/mpim for", this.id)

		}
	}

	public async reopen(): Promise<boolean> {
		const canOpen = ["im", "mpim"].includes(this.type);
		if (!canOpen) {
			return false;
		}
		const res = await this.client.web(this.team.id).conversations.open({
			channel: this.id,
		});
		if (res.ok) {
			this.opened = true;
		}
		return res.ok as boolean;
	}

	public async close(): Promise<boolean> {
		const canClose = ["im", "mpim"].includes(this.type);
		if (!canClose) {
			return false;
		}
		const res = await this.client.web(this.team.id).conversations.close({
			channel: this.id,
		});
		if (res.ok) {
			this.opened = false;
		}
		return res.ok as boolean;
	}
	public async sendTyping() {
		await this.client.rtm(this.team.id).sendTyping(this.id);
	}

	public async sendMessage(sendable: SendableType, opts?: ISendOpts): Promise<string> {
		await this.join();
		const send: any = { // tslint:disable-line no-any
			...this.resolveSendable(sendable),
			channel: this.id,
		};
		this.applyOpts(send, opts);
		log.info("Posting message to Slack, channel:", this.id);
		const ret = await this.client.web(this.team.id).chat.postMessage(send);
		if (ret.error) {
			log.error("Error when posting message to Slack:", ret.error);
		}
		if (ret.ok && ret.ts) {
			log.info("ok:true when posting message to Slack, ts:", ret.ts);
		}
		if (!ret.ts) {
			log.warn("No timestamp found for sent message in", send.channel, "trying again")
			const message = ret.message as any
			if (!message.ts) {
				log.warn("No timestamps exist at all in", send.channel)
				log.warn("response contents", ret)
			}
			return message.ts as string
		}
		return ret.ts as string;
	}

	public async sendCommand(command: string, parameters?: string): Promise<string> {
		if (this.team.isBotToken()) {
			throw new Error("Not available with bot tokens");
		}
		await this.join();
		const send: any = { // tslint:disable-line no-any
			command,
			text: parameters,
			channel: this.id,
		};
		const ret = await this.client.web(this.team.id).apiCall("chat.command", send);
		return ret.ts as string;
	}

	public async sendMeMessage(sendable: SendableType): Promise<string> {
		await this.join();
		const send = this.resolveSendable(sendable);
		if (this.team.isBotToken()) {
			send.text = `_${send.text}_`;
			return await this.sendMessage(send);
		}
		const ret = await this.client.web(this.team.id).chat.meMessage({
			...send,
			channel: this.id,
		});
		return ret.ts as string;
	}

	public async deleteMessage(ts: string, opts?: ISendOpts) {
		await this.join();
		const send: any = { // tslint:disable-line no-any
			channel: this.id,
			ts,
		};
		this.applyOpts(send, opts);
		await this.client.web(this.team.id).chat.delete(send);
	}

	public async editMessage(sendable: SendableType, ts: string, opts?: ISendOpts): Promise<string> {
		await this.join();
		const send: any = { // tslint:disable-line no-any
			...this.resolveSendable(sendable),
			channel: this.id,
			ts,
		};
		this.applyOpts(send, opts);
		const ret = await this.client.web(this.team.id).chat.update(send);
		return ret.ts as string;
	}

	public async sendFile(urlOrBuffer: string | Buffer, title: string, filename?: string): Promise<string> {
		await this.join();
		if (!filename) {
			filename = title;
		}
		let buffer: Buffer;
		if (typeof urlOrBuffer === "string") {
			buffer = await Util.DownloadFile(urlOrBuffer);
		} else {
			buffer = urlOrBuffer;
		}
		const ret = await this.client.web(this.team.id).files.upload({
			filename,
			file: buffer,
			title,
			filetype: "auto",
			channels: this.id,
		});

		try {
			const resultFile = ret.file as any;
			let resultArray;
			if (resultFile.shares.public) {
				resultArray = Object.values(resultFile.shares.public)[0] as any;
			} else {
				resultArray = Object.values(resultFile.shares.private)[0] as any;
			}
			return resultArray[0].ts;
		} catch (e) { 
			log.info("Could not find timestamp for the file")
			return ret.ts as string; 
		}
		


	}

	public async sendReaction(ts: string, reaction: string) {
		await this.join();
		await this.client.web(this.team.id).reactions.add({
			channel: this.id,
			timestamp: ts,
			name: reaction,
		});
	}

	public async removeReaction(ts: string, reaction: string) {
		await this.join();
		await this.client.web(this.team.id).reactions.remove({
			channel: this.id,
			timestamp: ts,
			name: reaction,
		});
	}

	private resolveSendable(sendable: SendableType): ISendMessage {
		const msg: ISendMessage = typeof sendable === "string" ? {
			text: sendable,
		} : sendable;
		if (this.team.isBotToken() && msg.blocks && msg.blocks[0] && msg.blocks[0].type === "rich_text") {
			delete msg.blocks;
		}
		return msg;
	}

	private applyOpts(send: any, opts?: ISendOpts) { // tslint:disable-line no-any
		if (opts) {
			if (opts.username) {
				send.username = opts.username;
				if (opts.iconUrl) {
					send.icon_url = opts.iconUrl;
				}
				if (opts.iconEmoji) {
					send.icon_emoji = opts.iconEmoji;
				}
			}
			if (opts.asUser) {
				send.as_user = true;
			}
			if (opts.threadTs) {
				send.thread_ts = opts.threadTs;
			}
		}
	}
}
