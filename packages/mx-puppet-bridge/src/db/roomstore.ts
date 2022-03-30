/*
Copyright 2019, 2020 mx-puppet-bridge
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
import { IRoomStoreEntry } from "./interfaces";

const log = new Log("DbRoomStore");

// tslint:disable-next-line:no-magic-numbers
const ROOM_CACHE_LIFETIME = 1000 * 60 * 60 * 24;

export class DbRoomStore {
	private remoteCache: TimedCache<string, IRoomStoreEntry>;
	private mxidCache: TimedCache<string, IRoomStoreEntry>;
	private opCache: TimedCache<string, string>;
	private protocol: string;
	constructor(
		private db: IDatabaseConnector,
		cache: boolean = true,
		protocol: string = "unknown",
	) {
		this.remoteCache = new TimedCache(cache ? ROOM_CACHE_LIFETIME : 0);
		this.mxidCache = new TimedCache(cache ? ROOM_CACHE_LIFETIME : 0);
		this.opCache = new TimedCache(cache ? ROOM_CACHE_LIFETIME : 0);
		this.protocol = protocol;
	}

	public newData(mxid: string, roomId: string, puppetId: number): IRoomStoreEntry {
		return {
			mxid,
			roomId,
			puppetId,
			isDirect: false,
			e2be: false,
			isUsed: false,
		};
	}

	public async getAll(): Promise<IRoomStoreEntry[]> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_all"));
		const rows = await this.db.All("SELECT * FROM room_store");
		const results: IRoomStoreEntry[] = [];
		for (const row of rows) {
			const res = this.getFromRow(row);
			if (res) {
				results.push(res);
			}
		}
		stopTimer();
		return results;
	}

	public async getByIsDirect(): Promise<IRoomStoreEntry[]> {
		const rows = await this.db.All(
			"SELECT * FROM room_store WHERE is_direct = 1", {
		});
		const results: IRoomStoreEntry[] = [];
		for (const row of rows) {
			const res = this.getFromRow(row);
			if (res) {
				results.push(res);
			}
		}
		return results
	}

	public async getByRemote(puppetId: number, roomId: string): Promise<IRoomStoreEntry | null> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_by_remote"));
		const cached = this.remoteCache.get(`${puppetId};${roomId}`);
		if (cached) {
			return cached;
		}
		const row = await this.db.Get(
			"SELECT * FROM room_store WHERE room_id = $room_id AND puppet_id = $puppet_id", {
			room_id: roomId,
			puppet_id: puppetId,
		});
		stopTimer();
		return this.getFromRow(row);
	}

	public async getByPuppetId(puppetId: number): Promise<IRoomStoreEntry[]> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_by_puppet"));
		const rows = await this.db.All(
			"SELECT * FROM room_store WHERE puppet_id = $puppet_id", {
			puppet_id: puppetId,
		});
		const results: IRoomStoreEntry[] = [];
		for (const row of rows) {
			const res = this.getFromRow(row);
			if (res) {
				results.push(res);
			}
		}
		stopTimer();
		return results;
	}

	public async getByMxid(mxid: string): Promise<IRoomStoreEntry | null> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_by_mxid"));
		const cached = this.mxidCache.get(mxid);
		if (cached) {
			return cached;
		}
		const row = await this.db.Get(
			"SELECT * FROM room_store WHERE mxid = $mxid", { mxid },
		);
		stopTimer();
		return this.getFromRow(row);
	}

	public async set(data: IRoomStoreEntry) {
		const stopTimer = this.db.latency.startTimer(this.labels("insert_update"));
		const exists = await this.db.Get(
			"SELECT * FROM room_store WHERE mxid = $mxid", {mxid: data.mxid},
		);
		let query = "";
		if (!exists) {
			query = `INSERT INTO room_store (
				mxid,
				room_id,
				puppet_id,
				name,
				avatar_url,
				avatar_mxc,
				avatar_hash,
				topic,
				group_id,
				is_direct,
				e2be,
				external_url,
				is_used
			) VALUES (
				$mxid,
				$room_id,
				$puppet_id,
				$name,
				$avatar_url,
				$avatar_mxc,
				$avatar_hash,
				$topic,
				$group_id,
				$is_direct,
				$e2be,
				$external_url,
				$is_used
			)`;
		} else {
			query = `UPDATE room_store SET
				room_id = $room_id,
				puppet_id = $puppet_id,
				name = $name,
				avatar_url = $avatar_url,
				avatar_mxc = $avatar_mxc,
				avatar_hash = $avatar_hash,
				topic = $topic,
				group_id = $group_id,
				is_direct = $is_direct,
				e2be = $e2be,
				external_url = $external_url,
				is_used = $is_used
				WHERE mxid = $mxid`;
		}
		await this.db.Run(query, {
			mxid: data.mxid,
			room_id: data.roomId,
			puppet_id: data.puppetId,
			name: data.name || null,
			avatar_url: data.avatarUrl || null,
			avatar_mxc: data.avatarMxc || null,
			avatar_hash: data.avatarHash || null,
			topic: data.topic || null,
			group_id: data.groupId || null,
			is_direct: Number(data.isDirect),
			e2be: Number(data.e2be),
			external_url: data.externalUrl || null,
			is_used: Number(data.isUsed),
		});
		this.remoteCache.set(`${data.puppetId};${data.roomId}`, data);
		this.mxidCache.set(data.mxid, data);
		stopTimer();
	}

	public async delete(data: IRoomStoreEntry) {
		const stopTimer = this.db.latency.startTimer(this.labels("delete"));
		await this.db.Run(
			"DELETE FROM room_store WHERE mxid = $mxid", { mxid: data.mxid },
		);
		await this.db.Run(
			"DELETE FROM chan_op WHERE chan_mxid=$mxid", { mxid: data.mxid },
		);
		this.remoteCache.delete(`${data.puppetId};${data.roomId}`);
		this.mxidCache.delete(data.mxid);
		this.opCache.delete(data.mxid);
		stopTimer();
	}

	public async toGlobalNamespace(puppetId: number, roomId: string) {
		const stopTimer = this.db.latency.startTimer(this.labels("update_namespace"));
		const exists = await this.getByRemote(-1, roomId);
		if (exists) {
			return;
		}
		const room = await this.getByRemote(puppetId, roomId);
		if (!room) {
			return;
		}
		await this.db.Run("UPDATE room_store SET puppet_id = -1, group_id = '' WHERE puppet_id = $pid AND room_id = $rid", {
			pid: puppetId,
			rid: roomId,
		});
		this.remoteCache.delete(`${puppetId};${roomId}`);
		this.mxidCache.delete(room.mxid);
		this.opCache.delete(room.mxid);
		stopTimer();
	}

	public async setRoomOp(roomMxid: string, userMxid: string) {
		const stopTimer = this.db.latency.startTimer(this.labels("update_room_op"));
		const row = await this.db.Get("SELECT * FROM chan_op WHERE chan_mxid=$chan LIMIT 1", {
			chan: roomMxid,
		});
		if (row) {
			if ((row.user_mxid as string) === userMxid) {
				// nothing to do, we are already set
				stopTimer();
				return;
			}
			await this.db.Run("DELETE FROM chan_op WHERE chan_mxid=$chan", {
				chan: roomMxid,
			});
		}
		await this.db.Run("INSERT INTO chan_op (chan_mxid, user_mxid) VALUES ($chan, $user)", {
			chan: roomMxid,
			user: userMxid,
		});
		this.opCache.set(roomMxid, userMxid);
		stopTimer();
	}

	public async getRoomOp(roomMxid: string): Promise<string|null> {
		const stopTimer = this.db.latency.startTimer(this.labels("select_room_op"));
		const cached = this.opCache.get(roomMxid);
		if (cached) {
			return cached;
		}
		const row = await this.db.Get("SELECT user_mxid FROM chan_op WHERE chan_mxid=$chan LIMIT 1", {
			chan: roomMxid,
		});
		if (!row) {
			return null;
		}
		const userMxid = row.user_mxid as string;
		this.opCache.set(roomMxid, userMxid);
		stopTimer();
		return userMxid;
	}

	private getFromRow(row: ISqlRow | null): IRoomStoreEntry | null {
		if (!row) {
			return null;
		}
		const data = this.newData(
			row.mxid as string,
			row.room_id as string,
			Number(row.puppet_id),
		);
		data.name = (row.name || null) as string | null;
		data.avatarUrl = (row.avatar_url || null) as string | null;
		data.avatarMxc = (row.avatar_mxc || null) as string | null;
		data.avatarHash = (row.avatar_hash || null) as string | null;
		data.topic = (row.topic || null) as string | null;
		data.groupId = (row.group_id || null) as string | null;
		data.isDirect = Boolean(Number(row.is_direct));
		data.e2be = Boolean(Number(row.e2be));
		data.externalUrl = (row.external_url || null) as string | null;
		data.isUsed = Boolean(Number(row.is_used));

		this.remoteCache.set(`${data.puppetId};${data.roomId}`, data);
		this.mxidCache.set(data.mxid, data);
		return data;
	}

	private labels(queryName: string): object {
		return {
			protocol: this.protocol,
			engine: this.db.type,
			table: "room_store",
			type: queryName,
		};
	}
}
