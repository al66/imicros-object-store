"use strict";

const { ConcurrencyError } = require("../cqrs/cqrs");

class CassandraCQRSDatabase {
    constructor({
        client,
        keyspace,
        eventsTable = "cqrs_events",
        snapshotsTable = "cqrs_snapshots"
    } = {}) {
        this.client = client;
        this.keyspace = keyspace;
        this.eventsTable = eventsTable;
        this.snapshotsTable = snapshotsTable;
    }

    async connect() {
        this.#assertClient();
        if (typeof this.client.connect === "function") {
            await this.client.connect();
        }
    }

    async disconnect() {
        this.#assertClient();
        if (typeof this.client.shutdown === "function") {
            await this.client.shutdown();
        }
    }

    async init() {
        this.#assertClient();
        const eventsTable = this.#tableName(this.eventsTable);
        const snapshotsTable = this.#tableName(this.snapshotsTable);

        await this.client.execute(
            `CREATE TABLE IF NOT EXISTS ${eventsTable} (
                instance_id text,
                version int,
                event text,
                PRIMARY KEY (instance_id, version)
            )`
        );

        await this.client.execute(
            `CREATE TABLE IF NOT EXISTS ${snapshotsTable} (
                instance_id text PRIMARY KEY,
                version int,
                state text
            )`
        );
    }

    async readSnapshot(instanceId) {
        this.#assertClient();
        const table = this.#tableName(this.snapshotsTable);
        const query = `SELECT version, state FROM ${table} WHERE instance_id = ?`;
        const result = await this.client.execute(query, [instanceId], { prepare: true });
        if (!result.rows.length) return null;

        const row = result.rows[0];
        return {
            version: Number(row.version ?? 0),
            state: row.state ? this.#parseJSON(row.state, `Invalid snapshot JSON for instance ${instanceId}`) : {}
        };
    }

    async saveSnapshot(instanceId, state, version) {
        this.#assertClient();
        const table = this.#tableName(this.snapshotsTable);
        const query = `INSERT INTO ${table} (instance_id, version, state) VALUES (?, ?, ?)`;
        await this.client.execute(query, [instanceId, Number(version ?? 0), JSON.stringify(state || {})], { prepare: true });
    }

    async readEvents(instanceId, fromVersion = 0) {
        this.#assertClient();
        const table = this.#tableName(this.eventsTable);
        const query = `SELECT version, event FROM ${table} WHERE instance_id = ? AND version > ?`;
        const result = await this.client.execute(query, [instanceId, Number(fromVersion ?? 0)], { prepare: true });

        return result.rows
            .map((row) => ({
                version: Number(row.version ?? 0),
                ...(row.event ? this.#parseJSON(row.event, `Invalid event JSON for instance ${instanceId} at version ${row.version}`) : {})
            }))
            .sort((a, b) => a.version - b.version);
    }

    async appendEvents(instanceId, events = [], expectedVersion) {
        this.#assertClient();
        const currentEvents = await this.readEvents(instanceId, 0);
        const currentVersion = currentEvents.length ? (currentEvents[currentEvents.length - 1].version || 0) : 0;

        if (expectedVersion !== undefined && Number(expectedVersion) !== Number(currentVersion)) {
            throw new ConcurrencyError(`Expected version ${expectedVersion}, got ${currentVersion}`);
        }

        const table = this.#tableName(this.eventsTable);
        const inserted = [];

        for (let index = 0; index < (events || []).length; index += 1) {
            const version = currentVersion + index + 1;
            const event = events[index] || {};
            const query = `INSERT INTO ${table} (instance_id, version, event) VALUES (?, ?, ?)`;
            await this.client.execute(query, [instanceId, version, JSON.stringify(event)], { prepare: true });
            inserted.push({ ...event, version: Number(version ?? 0) });
        }

        return inserted;
    }

    #assertClient() {
        if (!this.client || typeof this.client.execute !== "function") {
            throw new Error("Missing Cassandra client");
        }
    }

    #tableName(table) {
        return this.keyspace ? `${this.keyspace}.${table}` : table;
    }

    #parseJSON(value, message) {
        try {
            return JSON.parse(value);
        } catch (error) {
            throw new Error(`${message}: ${error.message}`);
        }
    }
}

function createCassandraCQRSMixin(options = {}) {
    return {
        created() {
            this.cassandraCQRS = new CassandraCQRSDatabase({
                client: options.client || this.cassandra,
                keyspace: options.keyspace,
                eventsTable: options.eventsTable,
                snapshotsTable: options.snapshotsTable
            });
        },
        async started() {
            await this.cassandraCQRS.connect();
            await this.cassandraCQRS.init();
        },
        async stopped() {
            await this.cassandraCQRS.disconnect();
        }
    };
}

module.exports = {
    CassandraCQRSDatabase,
    createCassandraCQRSMixin
};
