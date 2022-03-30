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

import { expect } from "chai";
import { DbReactionStore } from "../../src/db/reactionstore";
import { Store } from "../../src/store";

// we are a test file and thus our linting rules are slightly different
// tslint:disable:no-unused-expression max-file-line-count no-any no-magic-numbers no-string-literal

async function getStore(): Promise<DbReactionStore> {
	const store = new Store({
		filename: ":memory:",
	} as any, {} as any);
	await store.init();
	return new DbReactionStore(store.db);
}

describe("DbReactionStore", () => {
	it("should work", async () => {
		const store = await getStore();
		const reaction = {
			puppetId: 1,
			roomId: "!room",
			userId: "@user",
			eventId: "blah",
			reactionMxid: "$event",
			key: "fox",
		};
		expect(await store.exists(reaction)).to.be.false;
		await store.insert(reaction);
		expect(await store.exists(reaction)).to.be.true;
		expect(await store.getFromReactionMxid("$event")).to.eql(reaction);
		expect(await store.getFromReactionMxid("$nonexisting")).to.be.null;
		expect(await store.getFromKey(reaction)).to.eql(reaction);
		expect(await store.getForEvent(1, "blah")).to.eql([reaction]);
		await store.delete("$event");
		expect(await store.exists(reaction)).to.be.false;
		await store.insert(reaction);
		expect(await store.exists(reaction)).to.be.true;
		await store.deleteForEvent(1, "blah");
		expect(await store.exists(reaction)).to.be.false;
	});
});
