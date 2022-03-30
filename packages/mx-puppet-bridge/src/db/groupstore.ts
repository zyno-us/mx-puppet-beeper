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

import { IDatabaseConnector, ISqlRow } from "./connector";
import { Log } from "../log";
import { TimedCache } from "../structures/timedcache";
import { IGroupStoreEntry } from "./interfaces";

const log = new Log("DbGroupStore");

// tslint:disable-next-line:no-magic-numbers
const GROUP_CACHE_LIFETIME = 1000 * 60 * 60 * 24;

export class DbGroupStore {
	private groupsCache: TimedCache<string, IGroupStoreEntry>;
	private protocol: string;
	constructor(
		private db: IDatabaseConnector,
		cache: boolean = true,
		protocolId: string = "unknown",
	) {
		this.groupsCache = new TimedCache(cache ? GROUP_CACHE_LIFETIME : 0);
		this.protocol = protocolId;
	}

	public newData(mxid: string, groupId: string, puppetId: number): IGroupStoreEntry {
		return {
			mxid,
			groupId,
			puppetId,
			roomIds: [],
		};
	}

	public async getByRemote(
		puppetId: number,
		groupId: string,
		ignoreCache: boolean = false,
	): Promise<IGroupStoreEntry | null> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_by_remote"));
		if (!ignoreCache) {
			const cached = this.groupsCache.get(`${puppetId};${groupId}`);
			if (cached) {
				return cached;
			}
		}
		const row = await this.db.Get(
			"SELECT * FROM group_store WHERE group_id = $groupId AND puppet_id = $puppetId", {
			groupId,
			puppetId,
		});
		const result =  await this.getFromRow(row);
		stopTimer();
		return result;
	}

	public async getByPuppetId(puppetId: number): Promise<IGroupStoreEntry[]> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_by_puppet"));
		const rows = await this.db.All(
			"SELECT * FROM group_store WHERE puppet_id = $puppetId", {
			puppetId,
		});
		const results: IGroupStoreEntry[] = [];
		for (const row of rows) {
			const res = await this.getFromRow(row);
			if (res) {
				results.push(res);
			}
		}
		stopTimer();
		return results;
	}

	public async getByMxid(mxid: string): Promise<IGroupStoreEntry | null> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_by_mxid"));
		const row = await this.db.Get(
			"SELECT * FROM group_store WHERE mxid = $mxid", { mxid },
		);
		const result = await this.getFromRow(row);
		stopTimer();
		return result;
	}

	public async set(data: IGroupStoreEntry) {
		const stopTimer = this.db.latency.startTimer(this.labels("insert_update"));
		// first de-dupe the room IDs
		const uniqueRoomIds: string[] = [];
		for (const roomId of data.roomIds) {
			if (!uniqueRoomIds.includes(roomId)) {
				uniqueRoomIds.push(roomId);
			}
		}
		data.roomIds = uniqueRoomIds;

		const oldData = await this.getByRemote(data.puppetId, data.groupId, true);
		if (!oldData) {
			// okay, we have to create a new entry
			await this.db.Run(`INSERT INTO group_store (
				mxid,
				group_id,
				puppet_id,
				name,
				avatar_url,
				avatar_mxc,
				avatar_hash,
				short_description,
				long_description
			) VALUES (
				$mxid,
				$groupId,
				$puppetId,
				$name,
				$avatarUrl,
				$avatarMxc,
				$avatarHash,
				$shortDescription,
				$longDescription
			)`, {
				mxid: data.mxid,
				groupId: data.groupId,
				puppetId: data.puppetId,
				name: data.name || null,
				avatarUrl: data.avatarUrl || null,
				avatarMxc: data.avatarMxc || null,
				avatarHash: data.avatarHash || null,
				shortDescription: data.shortDescription || null,
				longDescription: data.longDescription || null,
			});
			for (const roomId of data.roomIds) {
				await this.db.Run(`INSERT INTO group_store_rooms (
					group_id,
					puppet_id,
					room_id
				) VALUES (
					$groupId,
					$puppetId,
					$roomId
				)`, {
					groupId: data.groupId,
					puppetId: data.puppetId,
					roomId,
				});
			}
		} else {
			// we need to update an entry
			await this.db.Run(`UPDATE group_store SET
				group_id = $groupId,
				puppet_id = $puppetId,
				name = $name,
				avatar_url = $avatarUrl,
				avatar_mxc = $avatarMxc,
				avatar_hash = $avatarHash,
				short_description = $shortDescription,
				long_description = $longDescription
				WHERE mxid = $mxid`, {
				mxid: data.mxid,
				groupId: data.groupId,
				puppetId: data.puppetId,
				name: data.name || null,
				avatarUrl: data.avatarUrl || null,
				avatarMxc: data.avatarMxc || null,
				avatarHash: data.avatarHash || null,
				shortDescription: data.shortDescription || null,
				longDescription: data.longDescription || null,
			});
			// now we need to delete / add room IDs
			for (const oldRoomId of oldData.roomIds) {
				const found = data.roomIds.find((r: string) => oldRoomId === r);
				if (!found) {
					await this.db.Run(`DELETE FROM group_store_rooms WHERE
						group_id = $groupId AND puppet_id = $puppetId AND room_id = $roomId`, {
						groupId: data.groupId,
						puppetId: data.puppetId,
						roomId: oldRoomId,
					});
				}
			}
			// and now we create new ones
			for (const roomId of data.roomIds) {
				const found = oldData.roomIds.find((r: string) => roomId === r);
				if (!found) {
					await this.db.Run(`INSERT INTO group_store_rooms (
						group_id,
						puppet_id,
						room_id
					) VALUES (
						$groupId,
						$puppetId,
						$roomId
					)`, {
						groupId: data.groupId,
						puppetId: data.puppetId,
						roomId,
					});
				}
			}
		}
		this.groupsCache.set(`${data.puppetId};${data.groupId}`, data);
		stopTimer();
	}

	public async delete(data: IGroupStoreEntry) {
		const stopTimer = this.db.latency.startTimer(this.labels("delete"));
		await this.db.Run(
			"DELETE FROM group_store WHERE mxid = $mxid", { mxid: data.mxid },
		);
		await this.db.Run(
			"DELETE FROM group_store_rooms WHERE puppet_id = $puppetId AND group_id = $groupId", {
			puppetId: data.puppetId,
			groupId: data.groupId,
		});
		this.groupsCache.delete(`${data.puppetId};${data.groupId}`);
		stopTimer();
	}

	private async getFromRow(row: ISqlRow | null): Promise<IGroupStoreEntry | null> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_from_row"));
		if (!row) {
			return null;
		}
		const data = this.newData(
			row.mxid as string,
			row.group_id as string,
			Number(row.puppet_id),
		);
		data.name = (row.name || null) as string | null;
		data.avatarUrl = (row.avatar_url || null) as string | null;
		data.avatarMxc = (row.avatar_mxc || null) as string | null;
		data.avatarHash = (row.avatar_hash || null) as string | null;
		data.shortDescription = (row.short_description || null) as string | null;
		data.longDescription = (row.long_description || null) as string | null;

		const rows = await this.db.All(
			"SELECT room_id FROM group_store_rooms WHERE group_id = $groupId AND puppet_id = $puppetId", {
			groupId: data.groupId,
			puppetId: data.puppetId,
		});
		for (const r of rows) {
			if (r) {
				data.roomIds.push(r.room_id as string);
			}
		}

		this.groupsCache.set(`${data.puppetId};${data.groupId}`, data);
		stopTimer();
		return data;
	}

	private labels(queryName: string): object {
		return {
			protocol: this.protocol,
			engine: this.db.type,
			table: "group_store",
			type: queryName,
		};
	}
}
