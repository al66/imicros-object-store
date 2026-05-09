"use strict";

const { ConcurrencyError } = require("../cqrs/cqrs");

class PostgresCQRSDatabase {
    constructor({
        client,
        schema,
        eventsTable = "cqrs_events",
        snapshotsTable = "cqrs_snapshots"
    } = {}) {
        this.client = client;
        this.schema = schema;
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
        if (typeof this.client.end === "function") {
            await this.client.end();
        }
    }

    async init() {
        this.#assertClient();
        const eventsTable = this.#tableName(this.eventsTable);
        const snapshotsTable = this.#tableName(this.snapshotsTable);

        if (this.schema) {
            await this.client.query(`CREATE SCHEMA IF NOT EXISTS ${this.#quoteIdentifier(this.schema)}`);
        }

        await this.client.query(
            `CREATE TABLE IF NOT EXISTS ${eventsTable} (
                instance_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                event JSONB NOT NULL,
                PRIMARY KEY (instance_id, version)
            )`
        );

        await this.client.query(
            `CREATE TABLE IF NOT EXISTS ${snapshotsTable} (
                instance_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                state JSONB NOT NULL
            )`
        );
    }

    async readSnapshot(instanceId) {
        this.#assertClient();
        const table = this.#tableName(this.snapshotsTable);
        const result = await this.client.query(
            `SELECT version, state FROM ${table} WHERE instance_id = $1`,
            [instanceId]
        );
        if (!result.rows.length) return null;

        const row = result.rows[0];
        return {
            version: Number(row.version ?? 0),
            state: row.state || {}
        };
    }

    async saveSnapshot(instanceId, state, version) {
        this.#assertClient();
        const table = this.#tableName(this.snapshotsTable);
        await this.client.query(
            `INSERT INTO ${table} (instance_id, version, state)
             VALUES ($1, $2, $3)
             ON CONFLICT (instance_id) DO UPDATE SET version = $2, state = $3`,
            [instanceId, Number(version ?? 0), state || {}]
        );
    }

    async readEvents(instanceId, fromVersion = 0) {
        this.#assertClient();
        const table = this.#tableName(this.eventsTable);
        const result = await this.client.query(
            `SELECT version, event FROM ${table} WHERE instance_id = $1 AND version > $2 ORDER BY version ASC`,
            [instanceId, Number(fromVersion ?? 0)]
        );

        return result.rows.map((row) => ({
            version: Number(row.version ?? 0),
            ...(row.event || {})
        }));
    }

    async appendEvents(instanceId, events = [], expectedVersion) {
        this.#assertClient();
        const currentEvents = await this.readEvents(instanceId, 0);
        const currentVersion = currentEvents.length ? currentEvents[currentEvents.length - 1].version : 0;

        if (expectedVersion !== undefined && Number(expectedVersion) !== Number(currentVersion)) {
            throw new ConcurrencyError(`Expected version ${expectedVersion}, got ${currentVersion}`);
        }

        const table = this.#tableName(this.eventsTable);
        const inserted = [];

        for (let index = 0; index < (events || []).length; index += 1) {
            const version = currentVersion + index + 1;
            const event = events[index] || {};
            await this.client.query(
                `INSERT INTO ${table} (instance_id, version, event) VALUES ($1, $2, $3)`,
                [instanceId, version, event]
            );
            inserted.push({ ...event, version: Number(version) });
        }

        return inserted;
    }

    #assertClient() {
        if (!this.client || typeof this.client.query !== "function") {
            throw new Error("Missing PostgreSQL client");
        }
    }

    #tableName(table) {
        return this.schema ? `"${this.schema}"."${table}"` : `"${table}"`;
    }

    #quoteIdentifier(identifier) {
        return `"${String(identifier).replace(/"/g, "\"\"")}"`;
    }
}

function createPostgresCQRSMixin(options = {}) {
    return {
        created() {
            this.postgresCQRS = new PostgresCQRSDatabase({
                client: options.client || this.postgres,
                schema: options.schema,
                eventsTable: options.eventsTable,
                snapshotsTable: options.snapshotsTable
            });
        },
        async started() {
            await this.postgresCQRS.connect();
            await this.postgresCQRS.init();
        },
        async stopped() {
            await this.postgresCQRS.disconnect();
        }
    };
}

module.exports = {
    PostgresCQRSDatabase,
    createPostgresCQRSMixin
};
