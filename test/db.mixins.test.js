"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { ServiceBroker } = require("moleculer");

const { createPostgresCQRSMixin, createCassandraCQRSMixin } = require("../lib");

test("PostgreSQL mixin connects, initializes schema/tables and disconnects in lifecycle hooks", async () => {
    const calls = {
        connected: 0,
        disconnected: 0,
        queries: []
    };

    const client = {
        async connect() {
            calls.connected += 1;
        },
        async end() {
            calls.disconnected += 1;
        },
        async query(text) {
            calls.queries.push(text.replace(/\s+/g, " ").trim());
            return { rows: [] };
        }
    };

    const broker = new ServiceBroker({ logger: false });
    broker.createService({
        name: "pgLifecycle",
        mixins: [createPostgresCQRSMixin({ client, schema: "unit_test" })]
    });

    await broker.start();

    assert.equal(calls.connected, 1);
    assert.equal(calls.queries.length, 3);
    assert.ok(calls.queries.some((query) => query.startsWith("CREATE SCHEMA IF NOT EXISTS \"unit_test\"")));
    assert.ok(calls.queries.some((query) => query.includes("\"unit_test\".\"cqrs_events\"")));
    assert.ok(calls.queries.some((query) => query.includes("\"unit_test\".\"cqrs_snapshots\"")));

    await broker.stop();

    assert.equal(calls.disconnected, 1);
});

test("Cassandra mixin connects, initializes tables and disconnects in lifecycle hooks", async () => {
    const calls = {
        connected: 0,
        disconnected: 0,
        queries: []
    };

    const client = {
        async connect() {
            calls.connected += 1;
        },
        async shutdown() {
            calls.disconnected += 1;
        },
        async execute(query) {
            calls.queries.push(query.replace(/\s+/g, " ").trim());
            return { rows: [] };
        }
    };

    const broker = new ServiceBroker({ logger: false });
    broker.createService({
        name: "cassandraLifecycle",
        mixins: [createCassandraCQRSMixin({ client, keyspace: "unit_test" })]
    });

    await broker.start();

    assert.equal(calls.connected, 1);
    assert.equal(calls.queries.length, 2);
    assert.ok(calls.queries.some((query) => query.includes("CREATE TABLE IF NOT EXISTS unit_test.cqrs_events")));
    assert.ok(calls.queries.some((query) => query.includes("CREATE TABLE IF NOT EXISTS unit_test.cqrs_snapshots")));

    await broker.stop();

    assert.equal(calls.disconnected, 1);
});
