"use strict";

/**
 * Integration tests for the Group aggregate backed by a real Apache Cassandra
 * database.
 *
 * Prerequisites (provided by the GitHub Actions workflow):
 *   - A Cassandra instance reachable on CASSANDRA_CONTACT_POINTS (default: 127.0.0.1)
 *   - The keyspace specified by CASSANDRA_KEYSPACE (default: imicros_test) must
 *     either already exist or be created by the test setup below.
 *
 * Run locally:
 *   node --test test/group.cassandra.test.js
 */

const test = require("node:test");
const assert = require("node:assert/strict");
const { Client } = require("cassandra-driver");
const { ServiceBroker } = require("moleculer");

const { Model, createRepositoryMixin, ConcurrencyError } = require("../lib");
const { CassandraCQRSDatabase } = require("../lib/classes/db/cassandraCQRS");

// ---------------------------------------------------------------------------
// Configuration from environment variables
// ---------------------------------------------------------------------------
const CONTACT_POINTS = (process.env.CASSANDRA_CONTACT_POINTS || "127.0.0.1").split(",");
const KEYSPACE = process.env.CASSANDRA_KEYSPACE || "imicros_test";
const LOCAL_DC = process.env.CASSANDRA_LOCAL_DC || "datacenter1";

// ---------------------------------------------------------------------------
// One shared Cassandra client for the entire test suite.
// Lifecycle: connect before first test, shutdown after last test.
// ---------------------------------------------------------------------------
let sharedClient;
let sharedDatabase;

// ---------------------------------------------------------------------------
// Group model (mirrors the one in group.test.js)
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
// Helper: build a GroupServiceSchema backed by the shared CassandraCQRSDatabase.
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
// Suite setup / teardown using before/after hooks on the top-level test.
// ---------------------------------------------------------------------------
test("Cassandra integration: Group aggregate", async (t) => {
    // ------------------------------------------------------------------
    // Setup: connect and create keyspace + tables once for the suite.
    // ------------------------------------------------------------------
    sharedClient = new Client({
        contactPoints: CONTACT_POINTS,
        localDataCenter: LOCAL_DC
    });
    await sharedClient.connect();

    // Create keyspace if it does not exist (SimpleStrategy is fine for tests).
    await sharedClient.execute(
        `CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE}
         WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`
    );

    // Build and initialise the database (creates the tables inside the keyspace).
    sharedDatabase = new CassandraCQRSDatabase({ client: sharedClient, keyspace: KEYSPACE });
    await sharedDatabase.init();

    // ------------------------------------------------------------------
    // Sub-tests
    // ------------------------------------------------------------------

    await t.test("create a new group", async () => {
        const groupId = `cass-group-1-${Date.now()}`;
        const broker = new ServiceBroker({ logger: false });
        broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => sharedDatabase } });
        await broker.start();

        const { instance } = await broker.call("groups.create", { groupId, label: "Cassandra Test Group" });

        assert.equal(instance.isPersistant(), true);
        assert.equal(instance.getId(), groupId);
        assert.equal(instance.getLabel(), "Cassandra Test Group");
        assert.deepEqual(instance.state.members, []);

        await broker.stop();
    });

    await t.test("prevents creating the same group twice", async () => {
        const groupId = `cass-group-2-${Date.now()}`;
        const broker = new ServiceBroker({ logger: false });
        broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => sharedDatabase } });
        await broker.start();

        await broker.call("groups.create", { groupId, label: "Duplicate Cassandra Group" });

        await assert.rejects(
            () => broker.call("groups.create", { groupId, label: "Duplicate Cassandra Group" }),
            /Group already exists/
        );

        await broker.stop();
    });

    await t.test("rename an existing group", async () => {
        const groupId = `cass-group-3-${Date.now()}`;
        const broker = new ServiceBroker({ logger: false });
        broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => sharedDatabase } });
        await broker.start();

        await broker.call("groups.create", { groupId, label: "Original Cassandra Label" });
        const { instance } = await broker.call("groups.rename", { groupId, label: "Renamed Cassandra Label" });

        assert.equal(instance.getLabel(), "Renamed Cassandra Label");

        await broker.stop();
    });

    await t.test("invite a user and have them join", async () => {
        const groupId = `cass-group-4-${Date.now()}`;
        const email = "alice@cassandra.example.com";
        const user = { uid: "cass-user-alice", email };
        const broker = new ServiceBroker({ logger: false });
        broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => sharedDatabase } });
        await broker.start();

        await broker.call("groups.create", { groupId, label: "Cassandra Members Group" });
        await broker.call("groups.invite", { groupId, email });

        let group = await broker.call("groups.get", { groupId });
        assert.equal(group.state.invitations?.includes(email), true);

        await broker.call("groups.join", { groupId, member: user, role: "member" });
        group = await broker.call("groups.get", { groupId });

        assert.ok(group.isMember({ user }));
        assert.equal(group.state.invitations?.includes(email), false);

        await broker.stop();
    });

    await t.test("member can leave the group", async () => {
        const groupId = `cass-group-5-${Date.now()}`;
        const user = { uid: "cass-user-bob", email: "bob@cassandra.example.com" };
        const broker = new ServiceBroker({ logger: false });
        broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => sharedDatabase } });
        await broker.start();

        await broker.call("groups.create", { groupId, label: "Cassandra Leave Test Group" });
        await broker.call("groups.join", { groupId, member: user, role: "admin" });

        let group = await broker.call("groups.get", { groupId });
        assert.ok(group.isMember({ user }));

        await broker.call("groups.leave", { groupId, member: user });
        group = await broker.call("groups.get", { groupId });

        assert.ok(!group.isMember({ user }));

        await broker.stop();
    });

    await t.test("admin can remove another member", async () => {
        const groupId = `cass-group-6-${Date.now()}`;
        const admin = { uid: "cass-user-admin", email: "admin@cassandra.example.com" };
        const member = { uid: "cass-user-member", email: "member@cassandra.example.com" };
        const broker = new ServiceBroker({ logger: false });
        broker.createService({ ...buildGroupServiceSchema(), settings: { database: () => sharedDatabase } });
        await broker.start();

        await broker.call("groups.create", { groupId, label: "Cassandra Admin Group" });
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

    await t.test("rebuilds full state from events across broker restarts", async () => {
        const groupId = `cass-group-7-${Date.now()}`;
        const alice = { uid: "cass-alice", email: "alice@cassandra.example.com" };
        const bob = { uid: "cass-bob", email: "bob@cassandra.example.com" };

        const broker1 = new ServiceBroker({ logger: false });
        broker1.createService({ ...buildGroupServiceSchema(), settings: { database: () => sharedDatabase } });
        await broker1.start();

        await broker1.call("groups.create", { groupId, label: "Cassandra ES Group" });
        await broker1.call("groups.join", { groupId, member: alice, role: "admin" });
        await broker1.call("groups.join", { groupId, member: bob, role: "member" });
        await broker1.call("groups.rename", { groupId, label: "Cassandra ES Group Renamed" });

        await broker1.stop();

        // A fresh broker backed by the same database must rebuild state from events.
        const broker2 = new ServiceBroker({ logger: false });
        broker2.createService({ ...buildGroupServiceSchema(), settings: { database: () => sharedDatabase } });
        await broker2.start();

        const group = await broker2.call("groups.get", { groupId });

        assert.equal(group.getLabel(), "Cassandra ES Group Renamed");
        assert.ok(group.isMember({ user: alice }));
        assert.ok(group.isAdmin({ user: alice }));
        assert.ok(group.isMember({ user: bob }));
        assert.ok(!group.isAdmin({ user: bob }));
        assert.ok(group.isLastAdmin({ user: alice }));

        await broker2.stop();
    });

    await t.test("throws ConcurrencyError on version mismatch", async () => {
        const instanceId = `cass-concurrency-${Date.now()}`;

        // Append one event directly so the stored version is 1.
        await sharedDatabase.appendEvents(instanceId, [{ type: "GroupCreated", groupId: instanceId, label: "x", createdAt: new Date().toISOString() }], 0);

        // Trying to append with expectedVersion 0 again should throw ConcurrencyError.
        await assert.rejects(
            () => sharedDatabase.appendEvents(instanceId, [{ type: "GroupRenamed", label: "y" }], 0),
            ConcurrencyError
        );
    });

    // ------------------------------------------------------------------
    // Teardown: drop test tables and disconnect.
    // ------------------------------------------------------------------
    await sharedClient.execute(`DROP TABLE IF EXISTS ${KEYSPACE}.cqrs_events`);
    await sharedClient.execute(`DROP TABLE IF EXISTS ${KEYSPACE}.cqrs_snapshots`);
    await sharedClient.shutdown();
});
