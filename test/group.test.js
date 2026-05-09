"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { DefaultDatabase, Repository, Model, createRepositoryMixin } = require("../lib");

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
 * Example Moleculer service for the Group model.
 *
 * The service schema uses `createRepositoryMixin` which provides:
 *   - `created()` lifecycle hook that wires up the Repository with `service: this`
 *   - `getInstance(instanceId)` and `persist(instanceId, events, options)` methods
 *
 * In a real Moleculer application the mixin is listed in the `mixins` array and
 * the broker starts the service.  Here we simulate that lifecycle with a plain
 * object so the test has no external dependencies.
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

/**
 * Utility: instantiate the service from its schema, optionally injecting a
 * custom database and/or broker (for channel-publishing assertions).
 */
function createService({ database, broker } = {}) {
    // Merge mixin methods and actions into a plain service object.
    const mixin = GroupServiceSchema.mixins[0];

    const service = {
        name: GroupServiceSchema.name,
        broker,
        ...mixin.methods
    };

    // Bind mixin methods so `this` refers to the service.
    for (const key of Object.keys(mixin.methods)) {
        service[key] = service[key].bind(service);
    }

    // Add service actions, also bound to the service.
    service.actions = {};
    for (const [key, fn] of Object.entries(GroupServiceSchema.actions)) {
        service.actions[key] = fn.bind(service);
    }

    // Simulate the Moleculer `created` lifecycle, injecting an optional database.
    const repositoryOptions = { modelFactory: (state) => new Group(state) };
    if (database) repositoryOptions.database = database;

    service.repository = new Repository({ ...repositoryOptions, service });
    service.getInstance = (id) => service.repository.getInstance(id);
    service.persist = (id, events, opts) => service.repository.persist(id, events, opts);

    return service;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test("Group service: create a new group", async () => {
    const service = createService();

    const groupId = "group-1";
    const { instance } = await service.actions.create({ params: { groupId, label: "Test Group" } });

    assert.equal(instance.isPersistant(), true);
    assert.equal(instance.getId(), groupId);
    assert.equal(instance.getLabel(), "Test Group");
    assert.deepEqual(instance.state.members, []);
});

test("Group service: prevents creating the same group twice", async () => {
    const service = createService();

    const groupId = "group-2";
    await service.actions.create({ params: { groupId, label: "Duplicate Group" } });

    await assert.rejects(
        () => service.actions.create({ params: { groupId, label: "Duplicate Group" } }),
        /Group already exists/
    );
});

test("Group service: rename an existing group", async () => {
    const service = createService();
    const groupId = "group-3";

    await service.actions.create({ params: { groupId, label: "Original Label" } });
    const { instance } = await service.actions.rename({ params: { groupId, label: "Renamed Label" } });

    assert.equal(instance.getLabel(), "Renamed Label");
});

test("Group service: invite a user and have them join", async () => {
    const service = createService();
    const groupId = "group-4";
    const email = "alice@example.com";
    const user = { uid: "user-alice", email };

    await service.actions.create({ params: { groupId, label: "Members Group" } });
    await service.actions.invite({ params: { groupId, email } });

    // Invitation is tracked before the user joins.
    let group = await service.getInstance(groupId);
    assert.equal(group.state.invitations?.includes(email), true);

    // User joins; the pending invitation is cleared.
    await service.actions.join({ params: { groupId, member: user, role: "member" } });
    group = await service.getInstance(groupId);

    assert.ok(group.isMember({ user }));
    assert.equal(group.state.invitations?.includes(email), false);
});

test("Group service: member can leave the group", async () => {
    const service = createService();
    const groupId = "group-5";
    const user = { uid: "user-bob", email: "bob@example.com" };

    await service.actions.create({ params: { groupId, label: "Leave Test Group" } });
    await service.actions.join({ params: { groupId, member: user, role: "admin" } });

    let group = await service.getInstance(groupId);
    assert.ok(group.isMember({ user }));

    await service.actions.leave({ params: { groupId, member: user } });
    group = await service.getInstance(groupId);

    assert.ok(!group.isMember({ user }));
});

test("Group service: admin can remove another member", async () => {
    const service = createService();
    const groupId = "group-6";
    const admin = { uid: "user-admin", email: "admin@example.com" };
    const member = { uid: "user-member", email: "member@example.com" };

    await service.actions.create({ params: { groupId, label: "Admin Group" } });
    await service.actions.join({ params: { groupId, member: admin, role: "admin" } });
    await service.actions.join({ params: { groupId, member, role: "member" } });

    let group = await service.getInstance(groupId);
    assert.ok(group.isAdmin({ user: admin }));
    assert.ok(group.isMember({ user: member }));

    await service.actions.removeMember({ params: { groupId, userId: member.uid } });
    group = await service.getInstance(groupId);

    assert.ok(!group.isMember({ user: member }));
    assert.ok(group.isMember({ user: admin }));
});

test("Group service: publishes events to Moleculer channels via broker.sendToChannel", async () => {
    const published = [];
    const broker = {
        async sendToChannel(channel, payload) {
            published.push({ channel, payload });
        }
    };

    const service = createService({ broker });
    const groupId = "group-7";

    await service.actions.create({ params: { groupId, label: "Event Group" } });

    assert.equal(published.length, 1);
    assert.equal(published[0].channel, "events.GroupCreated");
    assert.equal(published[0].payload.instanceId, groupId);
});

test("Group service: rebuilds full state from events (event sourcing)", async () => {
    const database = new DefaultDatabase();
    const service = createService({ database });
    const groupId = "group-8";
    const alice = { uid: "user-alice2", email: "alice2@example.com" };
    const bob = { uid: "user-bob2", email: "bob2@example.com" };

    await service.actions.create({ params: { groupId, label: "ES Group" } });
    await service.actions.join({ params: { groupId, member: alice, role: "admin" } });
    await service.actions.join({ params: { groupId, member: bob, role: "member" } });
    await service.actions.rename({ params: { groupId, label: "ES Group Renamed" } });

    // A fresh service backed by the same database must rebuild state from events.
    const service2 = createService({ database });
    const group = await service2.getInstance(groupId);

    assert.equal(group.getLabel(), "ES Group Renamed");
    assert.ok(group.isMember({ user: alice }));
    assert.ok(group.isAdmin({ user: alice }));
    assert.ok(group.isMember({ user: bob }));
    assert.ok(!group.isAdmin({ user: bob }));
    assert.ok(group.isLastAdmin({ user: alice }));
});
