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

    async readSnapshot(instanceId) {
        this.#assertClient();
        const table = this.#tableName(this.snapshotsTable);
        const query = `SELECT version, state FROM ${table} WHERE instance_id = ?`;
        const result = await this.client.execute(query, [instanceId], { prepare: true });
        if (!result.rows.length) return null;

        const row = result.rows[0];
        return {
            version: Number(row.version || 0),
            state: row.state ? JSON.parse(row.state) : {}
        };
    }

    async saveSnapshot(instanceId, state, version) {
        this.#assertClient();
        const table = this.#tableName(this.snapshotsTable);
        const query = `INSERT INTO ${table} (instance_id, version, state) VALUES (?, ?, ?)`;
        await this.client.execute(query, [instanceId, Number(version || 0), JSON.stringify(state || {})], { prepare: true });
    }

    async readEvents(instanceId, fromVersion = 0) {
        this.#assertClient();
        const table = this.#tableName(this.eventsTable);
        const query = `SELECT version, event FROM ${table} WHERE instance_id = ? AND version > ?`;
        const result = await this.client.execute(query, [instanceId, Number(fromVersion || 0)], { prepare: true });

        return result.rows
            .map((row) => ({
                version: Number(row.version || 0),
                ...(row.event ? JSON.parse(row.event) : {})
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
            inserted.push({ ...event, version });
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
        }
    };
}

module.exports = {
    CassandraCQRSDatabase,
    createCassandraCQRSMixin
};
