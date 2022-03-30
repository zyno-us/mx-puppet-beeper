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

import { PuppetBridge } from "./puppetbridge";
import { IRemoteRoom } from "./interfaces";
import { IPuppet } from "./db/puppetstore";
import { Log } from "./log";
import { MatrixClient } from "@sorunome/matrix-bot-sdk";
import { Lock } from "./structures/lock";

interface IMatrixRoomEvents {
	timeline: {};
	state: {};
	account_data: {};
	ephemeral: {
		events: IMatrixReceiptEvent[];
	};
}

interface IMatrixReceiptEvent {
	type: "m.receipt";
	content: {
		[eventId: string]: {
			"m.read": {
				[userId: string]: {
					ts: number,
				},
			},
		},
	};
}

interface IMatrixSyncResponse {
	rooms: {
		join: {
			[roomId: string]: IMatrixRoomEvents,
		},
	};
}

const getFilter = (mxid) => ({
	account_data: {types: []},
	presence: {
		senders: [mxid],
		types: ["m.presence"],
	},
	room: {
		ephemeral: {
			types: ["m.receipt"],
		},
		include_leave: false,
		account_data: {types: []},
		state: {types: []},
		timeline: {types: []},
	},
});

const log = new Log("MatrixClientSyncroniser");

// tslint:disable-next-line:no-magic-numbers
const RECEIPT_TIMEOUT = 1000 * 5;
// tslint:disable-next-line:no-magic-numbers
const CLIENT_LOCK_TIMEOUT = 1000 * 5;

export class MatrixClientSyncroniser {
	private clientsForUsers: Map<string, MatrixClient>;
	private puppetsForUser: Map<string, Set<number>>;
	private lastReceiptForRoom: Map<string, { ts: number, eventId: string }>;
	private clientLock: Lock<string>;
	constructor(
		private bridge: PuppetBridge,
	) {
		this.clientsForUsers = new Map();
		this.puppetsForUser = new Map();
		this.lastReceiptForRoom = new Map();
		this.clientLock = new Lock(CLIENT_LOCK_TIMEOUT);
	}

	public async init(puppets: IPuppet[]) {
		for (const puppet of puppets) {
			await this.onNewPuppet(puppet.puppetId);
		}
		this.bridge.on("puppetNew", this.onNewPuppet.bind(this));
		this.bridge.on("puppetDelete", this.onRemovePuppet.bind(this));
	}

	private async onNewPuppet(puppetId: number) {
		const userId = await this.bridge.puppetStore.getMxid(puppetId);
		if (this.puppetsForUser.has(userId)) {
			this.puppetsForUser.get(userId)!.add(puppetId);
		} else {
			this.puppetsForUser.set(userId, new Set([puppetId]));
		}
		await this.maybeCreateClient(puppetId, userId);
	}

	private async onRemovePuppet(puppetId: number) {
		// puppet already removed, find only in map
		const [userId] = [...this.puppetsForUser.entries()].find(([, puppetsSet]) => puppetsSet.has(puppetId)) || [""];
		if (this.puppetsForUser.has(userId)) {
			this.puppetsForUser.get(userId)!.delete(puppetId);
		}
		await this.maybeRemoveClient(puppetId, userId);
	}

	private async maybeCreateClient(puppetId: number, userId: string) {
		// wait when newPuppet event will be handled for this userId
		await this.clientLock.wait(userId);
		if (this.clientsForUsers.has(userId)) {
			log.info(`Sync already running for userId: ${userId}. Aborting...`);
			return;
		}
		this.clientLock.set(userId);

		const token = await this.bridge.provisioner.getToken(puppetId);
		const client = await this.bridge.userSync.getClientFromTokenCallback(token);
		if (!client) {
			log.warn(`Failed to create client for userId: ${userId}`);
			return;
		}
		this.clientsForUsers.set(userId, client);
		await this.hacklyPrepareAndStartClient(userId);

		this.clientLock.release(userId);
	}

	private async maybeRemoveClient(puppetId: number, userId: string) {
		await this.clientLock.wait(userId);

		if (!this.puppetsForUser.has(userId) || !this.clientsForUsers.has(userId)) {
			log.warn(`Failed to stop sync for userId: ${userId}. Client doesn't exist.`);
			return;
		}
		if (this.puppetsForUser.get(userId)!.size > 0) {
			return;
		}
		const client = this.clientsForUsers.get(userId) as MatrixClient;
		client.stop();
		this.clientsForUsers.delete(userId);
		log.info(`Puppet ${puppetId} for userId: ${userId} removed. Sync was stopped.`);
	}

	private async hacklyPrepareAndStartClient(userId: string) {
		const client = this.clientsForUsers.get(userId) as MatrixClient;
		// @ts-ignore
		const origProcessSync = client.processSync;
		// @ts-ignore
		client.processSync = async (raw, emitFn) => {
			if (!raw) {
				return;
			}
			if (raw.rooms && raw.rooms.join) {
				await this.handleSyncResponse(userId, raw);
			}
			await origProcessSync.call(client, raw, emitFn);
		};

		await client.start(getFilter(userId));
	}

	private async handleSyncResponse(userId: string, res: IMatrixSyncResponse) {
		const { rooms } = res;
		for (const [roomId, events] of Object.entries(rooms.join)) {
			if (!roomId || !events.ephemeral || !events.ephemeral.events) {
				continue;
			}
			const roomParts = await this.bridge.roomSync.getPartsFromMxid(roomId);
			const room: IRemoteRoom | null = await this.bridge.namespaceHandler.getRemoteRoom(roomParts, userId);
			if (!room) {
				continue;
			}
			for (const event of events.ephemeral.events) {
				if (!event || event.type !== "m.receipt") {
					continue;
				}
				await this.handleReceiptEvent(userId, room, event);
			}
		}
	}

	private async handleReceiptEvent(userId: string, room: IRemoteRoom, event: IMatrixReceiptEvent) {
		for (const [eventId, receipts] of Object.entries(event.content)) {
			if (!receipts["m.read"]) {
				continue;
			}
			for (const [senderId, receiptOpt] of Object.entries(receipts["m.read"])) {
				if (senderId !== userId) {
					continue;
				}

				const lastEvent = this.lastReceiptForRoom.get(room.roomId);
				if (!lastEvent || lastEvent.ts < receiptOpt.ts) {
					this.lastReceiptForRoom.set(room.roomId, { ts: receiptOpt.ts, eventId });
				}

				this.bridge.delayedFunction.set(`receipt-${room.roomId}`, async () => {
					const { ts, eventId: lastEventId } = this.lastReceiptForRoom.get(room.roomId)!;
					this.lastReceiptForRoom.delete(room.roomId);
					log.info(`Send receipts for room: ${room.roomId} for event ${lastEventId}`);
					this.bridge.emit("receipt", room, ts, lastEventId);
				}, RECEIPT_TIMEOUT);
			}
		}
	}
}
