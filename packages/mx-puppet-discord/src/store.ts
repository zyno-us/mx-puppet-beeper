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

import { Store } from "mx-puppet-bridge";

const CURRENT_SCHEMA = 4;

export class IDbEmoji {
	public emojiId: string;
	public name: string;
	public animated: boolean;
	public mxcUrl: string;
}

export class DiscordStore {
	constructor(
		private store: Store,
	) { }

	public async init(): Promise<void> {
		await this.store.init(CURRENT_SCHEMA, "discord_schema", (version: number) => {
			return require(`./db/schema/v${version}.js`).Schema;
		}, false);
	}

	public async getBridgedGuilds(puppetId: number): Promise<string[]> {
		const rows = await this.store.db.All("SELECT guild_id FROM discord_bridged_guilds WHERE puppet_id=$puppetId", {
			puppetId,
		});
		const result: string[] = [];
		for (const row of rows) {
			result.push(row.guild_id as string);
		}
		return result;
	}

	public async isGuildBridged(puppetId: number, guildId: string): Promise<boolean> {
		const exists = await this.store.db.Get("SELECT 1 FROM discord_bridged_guilds WHERE puppet_id=$p AND guild_id=$g", {
			p: puppetId,
			g: guildId,
		});
		return exists ? true : false;
	}

	public async setBridgedGuild(puppetId: number, guildId: string): Promise<void> {
		if (await this.isGuildBridged(puppetId, guildId)) {
			return;
		}
		await this.store.db.Run("INSERT INTO discord_bridged_guilds (puppet_id, guild_id) VALUES ($p, $g)", {
			p: puppetId,
			g: guildId,
		});
	}

	public async removeBridgedGuild(puppetId: number, guildId: string): Promise<void> {
		await this.store.db.Run("DELETE FROM discord_bridged_guilds WHERE puppet_id=$p AND guild_id=$g", {
			p: puppetId,
			g: guildId,
		});
	}

	public async getBridgedChannels(puppetId: number): Promise<string[]> {
		const rows = await this.store.db.All("SELECT channel_id FROM discord_bridged_channels WHERE puppet_id=$puppetId", {
			puppetId,
		});
		const result: string[] = [];
		for (const row of rows) {
			result.push(row.channel_id as string);
		}
		return result;
	}

	public async isChannelBridged(puppetId: number, channelId: string): Promise<boolean> {
		const exists = await this.store.db.Get("SELECT 1 FROM discord_bridged_channels" +
			" WHERE puppet_id=$p AND channel_id=$c", {
			p: puppetId,
			c: channelId,
		});
		return exists ? true : false;
	}

	public async setBridgedChannel(puppetId: number, channelId: string): Promise<void> {
		if (await this.isChannelBridged(puppetId, channelId)) {
			return;
		}
		await this.store.db.Run("INSERT INTO discord_bridged_channels (puppet_id, channel_id) VALUES ($p, $c)", {
			p: puppetId,
			c: channelId,
		});
	}

	public async removeBridgedChannel(puppetId: number, channelId: string): Promise<void> {
		await this.store.db.Run("DELETE FROM discord_bridged_channels WHERE puppet_id=$p AND channel_id=$c", {
			p: puppetId,
			c: channelId,
		});
	}
}
