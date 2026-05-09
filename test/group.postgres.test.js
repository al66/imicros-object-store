"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { ServiceBroker } = require("moleculer");

const { Model, createRepositoryMixin } = require("../lib");
const { PostgresCQRSDatabase } = require("../lib/classes/db/postgresqlCQRS");

// ---------------------------------------------------------------------------
// Minimal in-memory mock for a PostgreSQL client (pg-compatible interface).
//
// The mock interprets the parameterised queries issued by PostgresCQRSDatabase:
//   - SELECT … state  → snapshot read
//   - INSERT … state  → snapshot upsert
//   - SELECT … event  → events read
//   - INSERT … event  → single event append
//
// pg JSONB columns are returned as parsed JS objects, so the mock stores and
// returns objects directly (no JSON serialisation round-trip needed).
// ---------------------------------------------------------------------------
class MockPgClient {
    constructor() {
        // instance_id → { version, state }
        this._snapshots = new Map();
        // instance_id → Array<{ version: number, event: object }>
        this._events = new Map();
    }

    async query(text, params = []) {
        const lower = text.toLowerCase().replace(/\s+/g, " ").trim();

        // Snapshot read: SELECT version, state FROM …
        if (lower.startsWith("select") && lower.includes("state")) {
            const instanceId = params[0];
            const row = this._snapshots.get(instanceId);
            return { rows: row ? [{ version: row.version, state: row.state }] : [] };
        }

        // Snapshot upsert: INSERT … state … ON CONFLICT …
        if (lower.startsWith("insert") && lower.includes("state")) {
            const [instanceId, version, state] = params;
            this._snapshots.set(instanceId, { version, state });
            return { rows: [] };
        }

        // Events read: SELECT version, event FROM … WHERE … ORDER BY version ASC
        if (lower.startsWith("select") && lower.includes("event")) {
            const [instanceId, fromVersion] = params;
            const events = this._events.get(instanceId) || [];
            const filtered = events
                .filter((e) => e.version > Number(fromVersion))
                .map((e) => ({ version: e.version, event: e.event }));
            return { rows: filtered };
        }

        // Event append: INSERT INTO … (instance_id, version, event) VALUES …
        if (lower.startsWith("insert") && lower.includes("event")) {
            const [instanceId, version, event] = params;
            if (!this._events.has(instanceId)) this._events.set(instanceId, []);
            this._events.get(instanceId).push({ version: Number(version), event });
            return { rows: [] };
        }

        throw new Error(`MockPgClient: unexpected query: ${text}`);
    }
}

// ---------------------------------------------------------------------------
// Group model – mirrors the one in group.test.js.
// ---------------------------------------------------------------------------
class Group extends Model {
    constructor(initialState = {}) {
        super(initialState);
        if (!this.state) this.state = { members: [], agents: {}, services: {} };
    }

    isAdmin({ user }) {
        if (user && user.uid) return this.state.members.find((m) => m.user.uid === user.uid && m.role === "admin");
        return false;
    }

    isMember({ user }) {
        if (user && user.uid) return this.state.members.find((m) => m.user.uid === user.uid);
        return false;
    }

    isLastAdmin({ user }) {
        if (user && user.uid) return !this.state.members.some((m) => m.user.uid !== user.uid && m.role === "admin");
        return false;
    }

    isPersistant() {
        return this.state.createdAt ? true : false;
    }

    getId() {
        return this.state.uid;
    }

    getLabel() {
        return this.state.label;
    }

    getCurrentState() {
        return this.state;
    }

    onGroupCreated(event) {
        this.state.uid = event.groupId;
        this.state.createdAt = event.createdAt;
        this.state.label = event.label;
        this.state.members = [];
        this.state.agents = {};
        this.state.services = {};
    }

    onGroupRenamed(event) {
        this.state.label = event.label;
    }

    onUserInvited(event) {
        if (!this.state.invitations) this.state.invitations = [];
        this.state.invitations.push(event.email);
    }

    onGroupMemberJoined(event) {
        this.state.members.push({ user: event.member, role: event.role });
        if (this.state.invitations) {
            const index = this.state.invitations.indexOf(event.member?.email);
            if (index !== -1) this.state.invitations.splice(index, 1);
        }
    }

    onGroupMemberLeft(event) {
        this.state.members = this.state.members.filter((m) => m.user.uid !== event.member?.uid);
    }

    onGroupMemberRemoved(event) {
        this.state.members = this.state.members.filter((m) => m.user.uid !== event.userId);
    }
}

// ---------------------------------------------------------------------------
// Helper: build a GroupServiceSchema backed by a PostgresCQRSDatabase.
// ---------------------------------------------------------------------------
function buildGroupServiceSchema() {
    return {
        name: "groups",

        mixins: [
            createRepositoryMixin({
                modelFactory: (state) => new Group(state)
            })
        ],

        actions: {
            async create(ctx) {
                const { groupId, label } = ctx.params;
                const group = await this.getInstance(groupId);
                if (group.isPersistant()) throw new Error("Group already exists");
                return this.persist(groupId, [{ type: "GroupCreated", groupId, label, createdAt: new Date().toISOString() }], { expectedVersion: 0 });
            },

            async rename(ctx) {
                const { groupId, label } = ctx.params;
                const group = await this.getInstance(groupId);
                if (!group.isPersistant()) throw new Error("Group not found");
                return this.persist(groupId, [{ type: "GroupRenamed", groupId, label }]);
            },

            async invite(ctx) {
                const { groupId, email } = ctx.params;
                const group = await this.getInstance(groupId);
                if (!group.isPersistant()) throw new Error("Group not found");
                return this.persist(groupId, [{ type: "UserInvited", groupId, email }]);
            },

            async join(ctx) {
                const { groupId, member, role } = ctx.params;
                return this.persist(groupId, [{ type: "GroupMemberJoined", groupId, member, role: role || "member" }]);
            },

            async leave(ctx) {
                const { groupId, member } = ctx.params;
                return this.persist(groupId, [{ type: "GroupMemberLeft", groupId, member }]);
            },

            async removeMember(ctx) {
                const { groupId, userId } = ctx.params;
                return this.persist(groupId, [{ type: "GroupMemberRemoved", groupId, userId }]);
            },

            async get(ctx) {
                const { groupId } = ctx.params;
                return this.getInstance(groupId);
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test("Group service (PostgreSQL): create a new group", async () => {
    const pgClient = new MockPgClient();
    const database = new PostgresCQRSDatabase({ client: pgClient });

    const broker = new ServiceBroker({ logger: false });
    broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker.start();

    const groupId = "pg-group-1";
    const { instance } = await broker.call("groups.create", { groupId, label: "PG Test Group" });

    assert.equal(instance.isPersistant(), true);
    assert.equal(instance.getId(), groupId);
    assert.equal(instance.getLabel(), "PG Test Group");
    assert.deepEqual(instance.state.members, []);

    await broker.stop();
});

test("Group service (PostgreSQL): prevents creating the same group twice", async () => {
    const pgClient = new MockPgClient();
    const database = new PostgresCQRSDatabase({ client: pgClient });

    const broker = new ServiceBroker({ logger: false });
    broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker.start();

    const groupId = "pg-group-2";
    await broker.call("groups.create", { groupId, label: "Duplicate PG Group" });

    await assert.rejects(
        () => broker.call("groups.create", { groupId, label: "Duplicate PG Group" }),
        /Group already exists/
    );

    await broker.stop();
});

test("Group service (PostgreSQL): rename an existing group", async () => {
    const pgClient = new MockPgClient();
    const database = new PostgresCQRSDatabase({ client: pgClient });

    const broker = new ServiceBroker({ logger: false });
    broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker.start();

    const groupId = "pg-group-3";
    await broker.call("groups.create", { groupId, label: "Original PG Label" });
    const { instance } = await broker.call("groups.rename", { groupId, label: "Renamed PG Label" });

    assert.equal(instance.getLabel(), "Renamed PG Label");

    await broker.stop();
});

test("Group service (PostgreSQL): invite a user and have them join", async () => {
    const pgClient = new MockPgClient();
    const database = new PostgresCQRSDatabase({ client: pgClient });

    const broker = new ServiceBroker({ logger: false });
    broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker.start();

    const groupId = "pg-group-4";
    const email = "alice@example.com";
    const user = { uid: "pg-user-alice", email };

    await broker.call("groups.create", { groupId, label: "PG Members Group" });
    await broker.call("groups.invite", { groupId, email });

    let group = await broker.call("groups.get", { groupId });
    assert.equal(group.state.invitations?.includes(email), true);

    await broker.call("groups.join", { groupId, member: user, role: "member" });
    group = await broker.call("groups.get", { groupId });

    assert.ok(group.isMember({ user }));
    assert.equal(group.state.invitations?.includes(email), false);

    await broker.stop();
});

test("Group service (PostgreSQL): member can leave the group", async () => {
    const pgClient = new MockPgClient();
    const database = new PostgresCQRSDatabase({ client: pgClient });

    const broker = new ServiceBroker({ logger: false });
    broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker.start();

    const groupId = "pg-group-5";
    const user = { uid: "pg-user-bob", email: "bob@example.com" };

    await broker.call("groups.create", { groupId, label: "PG Leave Test Group" });
    await broker.call("groups.join", { groupId, member: user, role: "admin" });

    let group = await broker.call("groups.get", { groupId });
    assert.ok(group.isMember({ user }));

    await broker.call("groups.leave", { groupId, member: user });
    group = await broker.call("groups.get", { groupId });

    assert.ok(!group.isMember({ user }));

    await broker.stop();
});

test("Group service (PostgreSQL): admin can remove another member", async () => {
    const pgClient = new MockPgClient();
    const database = new PostgresCQRSDatabase({ client: pgClient });

    const broker = new ServiceBroker({ logger: false });
    broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker.start();

    const groupId = "pg-group-6";
    const admin = { uid: "pg-user-admin", email: "admin@example.com" };
    const member = { uid: "pg-user-member", email: "member@example.com" };

    await broker.call("groups.create", { groupId, label: "PG Admin Group" });
    await broker.call("groups.join", { groupId, member: admin, role: "admin" });
    await broker.call("groups.join", { groupId, member, role: "member" });

    let group = await broker.call("groups.get", { groupId });
    assert.ok(group.isAdmin({ user: admin }));
    assert.ok(group.isMember({ user: member }));

    await broker.call("groups.removeMember", { groupId, userId: member.uid });
    group = await broker.call("groups.get", { groupId });

    assert.ok(!group.isMember({ user: member }));
    assert.ok(group.isMember({ user: admin }));

    await broker.stop();
});

test("Group service (PostgreSQL): rebuilds full state from events across broker restarts", async () => {
    const pgClient = new MockPgClient();
    const database = new PostgresCQRSDatabase({ client: pgClient });

    const groupId = "pg-group-7";
    const alice = { uid: "pg-alice", email: "alice@pg.example.com" };
    const bob = { uid: "pg-bob", email: "bob@pg.example.com" };

    const broker1 = new ServiceBroker({ logger: false });
    broker1.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker1.start();

    await broker1.call("groups.create", { groupId, label: "PG ES Group" });
    await broker1.call("groups.join", { groupId, member: alice, role: "admin" });
    await broker1.call("groups.join", { groupId, member: bob, role: "member" });
    await broker1.call("groups.rename", { groupId, label: "PG ES Group Renamed" });

    await broker1.stop();

    // A fresh broker backed by the same database (same pgClient) must rebuild
    // state from the stored events.
    const broker2 = new ServiceBroker({ logger: false });
    broker2.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker2.start();

    const group = await broker2.call("groups.get", { groupId });

    assert.equal(group.getLabel(), "PG ES Group Renamed");
    assert.ok(group.isMember({ user: alice }));
    assert.ok(group.isAdmin({ user: alice }));
    assert.ok(group.isMember({ user: bob }));
    assert.ok(!group.isAdmin({ user: bob }));
    assert.ok(group.isLastAdmin({ user: alice }));

    await broker2.stop();
});

test("Group service (PostgreSQL): schema prefix is applied to table names", async () => {
    // We capture which table names appear in the queries so we can verify the
    // schema prefix is honoured.
    const queriedTables = new Set();
    const baseClient = new MockPgClient();
    const instrumentedClient = {
        async query(text, params) {
            // Extract the first quoted identifier (table name) from the query.
            const match = text.match(/"([^"]+)"\."([^"]+)"/);
            if (match) queriedTables.add(`${match[1]}.${match[2]}`);
            return baseClient.query(text, params);
        }
    };

    const database = new PostgresCQRSDatabase({ client: instrumentedClient, schema: "myschema" });

    const broker = new ServiceBroker({ logger: false });
    broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => database } });
    await broker.start();

    await broker.call("groups.create", { groupId: "pg-schema-group", label: "Schema Test" });

    assert.ok(queriedTables.has("myschema.cqrs_events"), "events table should use schema prefix");

    await broker.stop();
});

test("Group service (PostgreSQL): throws ConcurrencyError on version mismatch", async () => {
    const pgClient = new MockPgClient();
    const database = new PostgresCQRSDatabase({ client: pgClient });

    const { ConcurrencyError } = require("../lib");

    // Append one event directly so the stored version is 1.
    await database.appendEvents("pg-concurrency", [{ type: "GroupCreated", groupId: "pg-concurrency", label: "x", createdAt: "2024-01-01" }], 0);

    // Trying to append with expectedVersion 0 again should throw ConcurrencyError.
    await assert.rejects(
        () => database.appendEvents("pg-concurrency", [{ type: "GroupRenamed", label: "y" }], 0),
        ConcurrencyError
    );
});
