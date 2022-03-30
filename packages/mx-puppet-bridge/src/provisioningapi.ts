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

import { PuppetBridge } from "./puppetbridge";
import { Router, Request, Response } from "express";
import { IPuppet } from "./db/puppetstore";
import { createHmac } from "crypto";
import { Log } from "./log";

const log = new Log("ProvisioningAPI");

const OK = 200;
const CREATED = 201;
const NO_CONTENT = 204;
const BAD_REQUEST = 400;
const UNAUTHORIZED = 401;
const FORBIDDEN = 403;
const NOT_IMPLEMENTED = 501;

interface IPuppetWithDescription extends IPuppet {
	description: string | null;
}

export interface IAuthedRequest extends Request {
	userId: string;
	puppetId: number | null;
}

export declare type EndpointFn = (req: IAuthedRequest, res: Response) => Promise<void>;

export class ProvisioningAPI {
	private readonly mainRouter: Router;
	private readonly apiRouterV1: Router;
	private readonly apiSharedSecret: string;
	constructor(
		private readonly bridge: PuppetBridge,
	) {
		this.apiRouterV1 = Router();
		this.apiSharedSecret = bridge.config.provisioning.sharedSecret;

		this.mainRouter = Router();
		this.mainRouter.use(this.checkProvisioningSharedSecret.bind(this));
		this.mainRouter.use("/v1", this.apiRouterV1);

		this.registerProvisioningGetEndpoint("/status", this.status.bind(this));
		this.registerProvisioningPostEndpoint("/link", this.link.bind(this));
		this.registerProvisioningPostEndpoint("/:puppetId(\\d+)/unlink", this.unlink.bind(this));
		this.registerProvisioningPostEndpoint("/:puppetId(\\d+)/relink", this.relink.bind(this))
		this.registerProvisioningGetEndpoint("/:puppetId(\\d+)/users", this.listUsers.bind(this));
		this.registerProvisioningGetEndpoint("/:puppetId(\\d+)/rooms", this.listRooms.bind(this));
	}

	public registerProvisioningAPI() {
		this.bridge.AS.expressAppInstance.use(this.bridge.config.provisioning.apiPrefix, this.mainRouter);
	}

	public registerProvisioningGetEndpoint(path: string, fn: EndpointFn) {
		this.apiRouterV1.get(path, async (req: IAuthedRequest, res: Response) => {
			if (!await this.checkAndGetPuppetId(req, res)) {
				return null;
			}
			return await fn(req, res);
		});
	}

	public registerProvisioningPostEndpoint(path: string, fn: EndpointFn) {
		this.apiRouterV1.post(path, async (req: IAuthedRequest, res: Response) => {
			if (!await this.checkAndGetPuppetId(req, res)) {
				return null;
			}
			return await fn(req, res);
		});
	}

	get v1(): Router {
		return this.apiRouterV1;
	}

	private isValidAuth(auth: string | undefined, user_id: string): boolean {
		if (!auth || !auth.startsWith("Bearer ")) {
			return false;
		}

		auth = auth.slice("Bearer ".length);
		if (auth === this.apiSharedSecret) {
			return true;
		}

		const secret = createHmac("sha512", this.apiSharedSecret)
			.update(Buffer.from(user_id, "utf-8"))
			.digest("hex");
		return auth === secret;
	}

	private async checkProvisioningSharedSecret(req: IAuthedRequest, res: Response, next: () => void) {
		if (!this.apiSharedSecret) {
			res.status(FORBIDDEN).json({
				error: "The provisioning API is disabled",
				errcode: "M_FORBIDDEN",
			});
		} else if (!req.query.user_id) {
			res.status(BAD_REQUEST).json({
				error: "Missing user_id query parameter",
				errcode: "M_BAD_REQUEST",
			});
		} else if (typeof req.query.user_id !== "string") {
			res.status(BAD_REQUEST).json({
				error: "user_id query parameter isn't a string?",
				errcode: "M_BAD_REQUEST",
			});
		} else if (!this.isValidAuth(req.header("Authorization"), req.query.user_id)) {
			res.status(UNAUTHORIZED).json({
				error: "Unknown or missing token",
				errcode: "M_UNKNOWN_TOKEN",
			});
		} else {
			req.userId = req.query.user_id;
			next();
		}
	}

	private async checkAndGetPuppetId(req: IAuthedRequest, res: Response): Promise<boolean> {
		// Verify puppetId if it's in the request params, otherwise set to null and be on our way

		const puppetId = Number(req.params.puppetId);
		if (Number.isNaN(puppetId)) {
			req.puppetId = null;
			return true;
		}

		const data = await this.bridge.provisioner.get(puppetId);
		if (!data || data.puppetMxid !== req.userId) {
			res.status(FORBIDDEN).json({
				error: "You must own the puppet ID",
				errcode: "M_FORBIDDEN",
			});
			return false;
		}

		req.puppetId = puppetId;
		return true;
	}

	private async status(req: IAuthedRequest, res: Response) {
		const puppets = await this.bridge.provisioner.getForMxid(req.userId) as IPuppetWithDescription[];
		if (this.bridge.hooks.getDesc) {
			for (const data of puppets) {
				data.description = await this.bridge.hooks.getDesc(data.puppetId, data.data);
			}
		}
		res.json({
			puppets,
			permissions: {
				create: this.bridge.provisioner.canCreate(req.userId),
				relay: this.bridge.provisioner.canRelay(req.userId),
			},
		});
	}

	private async link(req: IAuthedRequest, res: Response) {
		const puppetId = await this.bridge.provisioner.new(req.userId, req.body.data, req.body.remote_user_id);
		if (!puppetId) {
			res.status(400).json({
				errcode: "M_FORBIDDEN",
				error: "Duplicate puppet already registered",
			});
			return;
		}
		res.status(CREATED).json({ puppet_id: puppetId });
	}

	private async relink(req: IAuthedRequest, res: Response) {
		if (!req.puppetId) {
			return res.status(BAD_REQUEST).json({
				error: "Missing puppet Id",
				errcode: "M_BAD_REQUEST",
			});
		}

		const puppet = await this.bridge.provisioner.get(req.puppetId);
		if (!puppet) {
			return res.status(400).json({
				errcode: "M_FORBIDDEN",
				error: "Puppet cannot be found",
			});
		}

		const token = req.body.data.token.slice(0, 25);
		if (!(typeof puppet.data.token === "string" && puppet.data.token.startsWith(token))){ 
			//compares new token with previous token to ensure they refer to the same account
			return res.status(400).json({
				errcode: "M_FORBIDDEN",
				error: "New credentials don't match the existing account",
			});
		}

		await this.bridge.provisioner.update(req.userId, req.puppetId, req.body.data, req.body.remote_user_id);
		res.status(202).json({ puppet_id: req.puppetId });
	}

	private async unlink(req: IAuthedRequest, res: Response) {
		if (!req.puppetId) {
			return;
		}
		await this.bridge.provisioner.delete(req.userId, req.puppetId);
		res.status(NO_CONTENT).send();
	}

	private async listUsers(req: IAuthedRequest, res: Response) {
		if (!this.bridge.hooks.listUsers) {
			res.status(NOT_IMPLEMENTED).json({
				error: "listUsers hook not implemented",
				errcode: "M_NOT_IMPLEMENTED",
			});
			return;
		}
		if (!req.puppetId) {
			return;
		}
		res.status(OK).json(await this.bridge.hooks.listUsers(req.puppetId));
	}

	private async listRooms(req: IAuthedRequest, res: Response) {
		if (!this.bridge.hooks.listRooms) {
			res.status(NOT_IMPLEMENTED).json({
				error: "listUsers hook not implemented",
				errcode: "M_NOT_IMPLEMENTED",
			});
			return;
		}
		if (!req.puppetId) {
			return;
		}
		res.status(OK).json(await this.bridge.hooks.listRooms(req.puppetId));
	}
}
