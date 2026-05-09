"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { DefaultDatabase, Repository, Model } = require("../lib");

class Counter extends Model {
    onCreated(event) {
        this.value = event.value;
    }

    onIncremented(event) {
        this.value += event.amount;
    }
}

test("repository rebuilds state from events and invokes service handlers", async () => {
    const serviceCalls = [];
    const service = {
        async onIncremented({ instance, event }) {
            serviceCalls.push(event.amount);
            instance.lastIncrement = event.amount;
        }
    };

    const repository = new Repository({
        database: new DefaultDatabase(),
        service,
        modelFactory: (state) => new Counter(state)
    });

    await repository.persist("1", [{ type: "Created", value: 2 }], { expectedVersion: 0 });
    await repository.persist("1", [{ type: "Incremented", amount: 3 }]);
    assert.deepEqual(serviceCalls, [3]);

    const instance = await repository.getInstance("1");

    assert.equal(instance.value, 5);
    assert.equal(instance.lastIncrement, 3);
    assert.deepEqual(serviceCalls, [3, 3]);
});

test("repository publishes events to moleculer channels", async () => {
    const published = [];

    const repository = new Repository({
        database: new DefaultDatabase(),
        service: {
            broker: {
                async sendToChannel(channel, payload) {
                    published.push({ channel, payload });
                }
            }
        }
    });

    await repository.persist("2", [{ type: "Created", value: 1 }], { expectedVersion: 0 });

    assert.equal(published.length, 1);
    assert.equal(published[0].channel, "events.Created");
    assert.equal(published[0].payload.instanceId, "2");
});
