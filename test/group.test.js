"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { ServiceBroker } = require("moleculer");

const { DefaultDatabase, Model, createRepositoryMixin } = require("../lib");

/**
 * Group model - following the pattern of lib/classes/repositories/group.js in imicros-core.
 *
 * State is stored under `this.state` so the model can be reconstructed cleanly
 * from snapshots: the Repository saves the full model instance (which includes
 * `this.state`) and restores it via the modelFactory.
 */
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

/**
 * Moleculer service schema for the Group aggregate.
 *
 * The service uses `createRepositoryMixin` which provides:
 *   - `created()` lifecycle hook that wires up the Repository with `service: this`
 *   - `getInstance(instanceId)` and `persist(instanceId, events, options)` methods
 *
 * An optional `settings.database` can be supplied when registering the service
 * to inject a custom database backend (useful in tests that share state across
 * multiple broker instances).
 */
const GroupServiceSchema = {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test("Group service: create a new group", async () => {
    const broker = new ServiceBroker({ logger: false });
    broker.createService(GroupServiceSchema);
    await broker.start();

    const groupId = "group-1";
    const { instance } = await broker.call("groups.create", { groupId, label: "Test Group" });

    assert.equal(instance.isPersistant(), true);
    assert.equal(instance.getId(), groupId);
    assert.equal(instance.getLabel(), "Test Group");
    assert.deepEqual(instance.state.members, []);

    await broker.stop();
});

test("Group service: prevents creating the same group twice", async () => {
    const broker = new ServiceBroker({ logger: false });
    broker.createService(GroupServiceSchema);
    await broker.start();

    const groupId = "group-2";
    await broker.call("groups.create", { groupId, label: "Duplicate Group" });

    await assert.rejects(
        () => broker.call("groups.create", { groupId, label: "Duplicate Group" }),
        /Group already exists/
    );

    await broker.stop();
});

test("Group service: rename an existing group", async () => {
    const broker = new ServiceBroker({ logger: false });
    broker.createService(GroupServiceSchema);
    await broker.start();

    const groupId = "group-3";
    await broker.call("groups.create", { groupId, label: "Original Label" });
    const { instance } = await broker.call("groups.rename", { groupId, label: "Renamed Label" });

    assert.equal(instance.getLabel(), "Renamed Label");

    await broker.stop();
});

test("Group service: invite a user and have them join", async () => {
    const broker = new ServiceBroker({ logger: false });
    broker.createService(GroupServiceSchema);
    await broker.start();

    const groupId = "group-4";
    const email = "alice@example.com";
    const user = { uid: "user-alice", email };

    await broker.call("groups.create", { groupId, label: "Members Group" });
    await broker.call("groups.invite", { groupId, email });

    // Invitation is tracked before the user joins.
    let group = await broker.call("groups.get", { groupId });
    assert.equal(group.state.invitations?.includes(email), true);

    // User joins; the pending invitation is cleared.
    await broker.call("groups.join", { groupId, member: user, role: "member" });
    group = await broker.call("groups.get", { groupId });

    assert.ok(group.isMember({ user }));
    assert.equal(group.state.invitations?.includes(email), false);

    await broker.stop();
});

test("Group service: member can leave the group", async () => {
    const broker = new ServiceBroker({ logger: false });
    broker.createService(GroupServiceSchema);
    await broker.start();

    const groupId = "group-5";
    const user = { uid: "user-bob", email: "bob@example.com" };

    await broker.call("groups.create", { groupId, label: "Leave Test Group" });
    await broker.call("groups.join", { groupId, member: user, role: "admin" });

    let group = await broker.call("groups.get", { groupId });
    assert.ok(group.isMember({ user }));

    await broker.call("groups.leave", { groupId, member: user });
    group = await broker.call("groups.get", { groupId });

    assert.ok(!group.isMember({ user }));

    await broker.stop();
});

test("Group service: admin can remove another member", async () => {
    const broker = new ServiceBroker({ logger: false });
    broker.createService(GroupServiceSchema);
    await broker.start();

    const groupId = "group-6";
    const admin = { uid: "user-admin", email: "admin@example.com" };
    const member = { uid: "user-member", email: "member@example.com" };

    await broker.call("groups.create", { groupId, label: "Admin Group" });
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

test("Group service: publishes events to Moleculer channels via broker.sendToChannel", async () => {
    const published = [];
    const broker = new ServiceBroker({ logger: false });
    broker.sendToChannel = async (channel, payload) => {
        published.push({ channel, payload });
    };
    broker.createService(GroupServiceSchema);
    await broker.start();

    const groupId = "group-7";
    await broker.call("groups.create", { groupId, label: "Event Group" });

    assert.equal(published.length, 1);
    assert.equal(published[0].channel, "events.GroupCreated");
    assert.equal(published[0].payload.instanceId, groupId);

    await broker.stop();
});

test("Group service: rebuilds full state from events (event sourcing)", async () => {
    const database = new DefaultDatabase();
    const groupId = "group-8";
    const alice = { uid: "user-alice2", email: "alice2@example.com" };
    const bob = { uid: "user-bob2", email: "bob2@example.com" };

    const broker1 = new ServiceBroker({ logger: false });
    broker1.createService({ ...GroupServiceSchema, settings: { database: () => database } });
    await broker1.start();

    await broker1.call("groups.create", { groupId, label: "ES Group" });
    await broker1.call("groups.join", { groupId, member: alice, role: "admin" });
    await broker1.call("groups.join", { groupId, member: bob, role: "member" });
    await broker1.call("groups.rename", { groupId, label: "ES Group Renamed" });

    await broker1.stop();

    // A fresh broker backed by the same database must rebuild state from events.
    const broker2 = new ServiceBroker({ logger: false });
    broker2.createService({ ...GroupServiceSchema, settings: { database: () => database } });
    await broker2.start();

    const group = await broker2.call("groups.get", { groupId });

    assert.equal(group.getLabel(), "ES Group Renamed");
    assert.ok(group.isMember({ user: alice }));
    assert.ok(group.isAdmin({ user: alice }));
    assert.ok(group.isMember({ user: bob }));
    assert.ok(!group.isAdmin({ user: bob }));
    assert.ok(group.isLastAdmin({ user: alice }));

    await broker2.stop();
});

test("Group service: handles multiple instances in parallel without cross-contamination", async () => {
    const broker = new ServiceBroker({ logger: false });
    broker.createService(GroupServiceSchema);
    await broker.start();

    // Create several groups concurrently.
    const groups = [
        { groupId: "parallel-1", label: "Alpha" },
        { groupId: "parallel-2", label: "Beta" },
        { groupId: "parallel-3", label: "Gamma" }
    ];

    await Promise.all(groups.map(({ groupId, label }) => broker.call("groups.create", { groupId, label })));

    // Each group must reflect only its own label (no state bleed between instances).
    for (const { groupId, label } of groups) {
        const instance = await broker.call("groups.get", { groupId });
        assert.equal(instance.isPersistant(), true);
        assert.equal(instance.getLabel(), label);
    }

    // Rename one group concurrently with reads of the others; verify isolation.
    await Promise.all([
        broker.call("groups.rename", { groupId: "parallel-1", label: "Alpha Renamed" }),
        broker.call("groups.get", { groupId: "parallel-2" }),
        broker.call("groups.get", { groupId: "parallel-3" })
    ]);

    const alpha = await broker.call("groups.get", { groupId: "parallel-1" });
    const beta = await broker.call("groups.get", { groupId: "parallel-2" });
    const gamma = await broker.call("groups.get", { groupId: "parallel-3" });

    assert.equal(alpha.getLabel(), "Alpha Renamed");
    assert.equal(beta.getLabel(), "Beta");
    assert.equal(gamma.getLabel(), "Gamma");

    await broker.stop();
});
