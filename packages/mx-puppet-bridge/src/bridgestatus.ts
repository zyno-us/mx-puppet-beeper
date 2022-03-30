import { PuppetBridge } from "./puppetbridge";
import { Log } from "./log";
import got from "got"

const log = new Log("BridgeStatus");

export class BridgeStatus {
    private dedupeDict: {[puppetId: number]: {stateEvent: string, errorCode: string | undefined, timestamp: number, bridged: boolean}}
    private url: string
    constructor(
		private bridge: PuppetBridge,
	) {
        this.dedupeDict = {}//Dictionary to track dupe statuses
        this.url = this.bridge.config.bridge.statusEndpoint //API server endpoint
    }

    public isNotDuplicateStatus(puppetId: number, newStateEvent: string, newError?: string): boolean {
        if (!(puppetId in this.dedupeDict)) {
            return true
        }   
        
        const oldState = this.dedupeDict[puppetId]
        if (oldState.bridged && (oldState.stateEvent != newStateEvent || oldState.errorCode != newError || oldState.timestamp < (Date.now()/1000 - 1800))) {
            return true; //Check for dupes by checking if this is the first status for that puppetId, if that status exists before or if the error exists before
        }
        return false;
    }

    public async send (stateEvent: string, puppetId?: number, errorCode?: string) {
        if (!this.url) {
            log.warn("BRIDGE SERVER STATUS - url not found")
            return;
        }

        if (!puppetId) { //For status updates for bridges as a whole
            log.info("BRIDGE SERVER STATUS - Trying to send bridge status without puppetId for puppet id", puppetId, "ok: false" , "stateEvent", stateEvent, "errorCode:", errorCode)
            if (stateEvent === "UNCONFIGURED" || stateEvent === "STARTING" || stateEvent === "RUNNING") {
                try {
                    const body = {
                        ok: false,
                        timestamp: Date.now() / 1000,
                        ttl: 60,
                        has_error: false,
                        state_event: stateEvent,
                        message: "",
                    }
                    const AStoken = this.bridge.AS.getAsToken(); //Get the AS token and make the request
                    await got(this.url, {
                        method: "POST",
                        headers: {
                          Authorization: `Bearer ${AStoken}`,
                          'content-type': 'application/json'
                        },
                        body: JSON.stringify(body),
                      });
                      log.info("BRIDGE SERVER STATUS -  Successfully sent bridge server, status for puppet id", puppetId, "ok: false" , "stateEvent", stateEvent, "errorCode:", errorCode)
                } catch (e) {
                    log.error("BRIDGE SERVER STATUS - startup request didnt work due to", e); //In case of error just ignore
                }
            }
            return;
        }

        if (!this.isNotDuplicateStatus(puppetId, stateEvent, errorCode)) { //Check if dupe status exists
            return
        }

        log.info("BRIDGE SERVER STATUS - Trying to send bridge status for puppet id", puppetId, "stateEvent", stateEvent, "errorCode:", errorCode) //For status updates for individual workspaces
        const timestamp = Date.now() / 1000
        const bridged = stateEvent === "LOGGED_OUT" ? false: true
        this.dedupeDict[puppetId] = {stateEvent, errorCode, timestamp, bridged}; //dedupe all future events of the same error type/error code
        const puppet = await this.bridge.puppetStore.get(puppetId); //Get puppet connected to the puppetId from the status message

        if (puppet) {
            if (!puppet.userId) {
                log.warn("BRIDGE SERVER STATUS - no userId")
                return;
            }
            if (!puppet.data) {
                log.warn("BRIDGE SERVER STATUS - no data")
                return;
            }
            if (!this.bridge.hooks.getDesc) {
                log.warn("BRIDGE SERVER STATUS - no getDesc hook")
                return;
            }
            const remoteInfo = await this.bridge.hooks.getDesc(puppetId, puppet.data) //Get the remote name of the workspace
            const ok = stateEvent === "CONNECTED"? true: false

            try {
                const body = { //Set up json request with all relevant values
                    ok: ok,
                    timestamp: timestamp,
                    ttl: ok ? 300 : 60,
                    user_id: puppet.puppetMxid,
                    remote_id: puppet.userId,
                    remote_name: remoteInfo,
                    has_error: errorCode? true : false,
                    error: errorCode,
                    state_event: stateEvent,
                    message: stateEvent === "BAD_CREDENTIALS" ? `${remoteInfo} has been signed out, please reconnect and sign in again` : `${remoteInfo} has been disconnected`,
                    error_source: ok? undefined : "bridge"
                }

                const AStoken = this.bridge.AS.getAsToken(); //Get the AS token and make the request
                await got(this.url, {
                    method: "POST",
                    headers: {
                      Authorization: `Bearer ${AStoken}`,
                      'content-type': 'application/json'
                    },
                    body: JSON.stringify(body),
                  });
                  log.info("BRIDGE SERVER STATUS -  Successfully sent bridge server, status for puppet id", puppetId, "ok:", ok, "stateEvent", stateEvent, "errorCode:", errorCode)

            } catch (e) {
                log.error("BRIDGE SERVER STATUS - didnt work due to", e); //In case of error just ignore
            }
        } else {
            log.warn("BRIDGE SERVER STATUS - puppetId:", puppetId, "no puppet found") //If no puppet exists ignore
        }
    }


}
