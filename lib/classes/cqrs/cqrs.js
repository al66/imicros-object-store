"use strict";

class ConcurrencyError extends Error {
    constructor(message = "Concurrency conflict") {
        super(message);
        this.name = "ConcurrencyError";
    }
}

function clone(value) {
    if (value === undefined || value === null) return value;
    return JSON.parse(JSON.stringify(value));
}

function getEventName(event) {
    if (!event) return "";
    return event.name || event.type || event.event || (event.constructor && event.constructor.name) || "";
}

class Model {
    constructor(initialState = {}) {
        Object.assign(this, clone(initialState) || {});
    }

    apply(event) {
        const handler = this.#handlerName(event);
        if (handler && typeof this[handler] === "function") {
            this[handler](event);
        }
        return this;
    }

    #handlerName(event) {
        const name = getEventName(event);
        return name ? `on${name}` : "";
    }
}

class DefaultDatabase {
    constructor() {
        this.events = new Map();
        this.snapshots = new Map();
    }

    async readSnapshot(instanceId) {
        return clone(this.snapshots.get(instanceId)) || null;
    }

    async saveSnapshot(instanceId, state, version) {
        this.snapshots.set(instanceId, {
            version,
            state: clone(state)
        });
    }

    async readEvents(instanceId, fromVersion = 0) {
        const list = this.events.get(instanceId) || [];
        return clone(list.filter((event) => (event.version || 0) > fromVersion));
    }

    async appendEvents(instanceId, events = [], expectedVersion) {
        const current = this.events.get(instanceId) || [];
        const currentVersion = current.length ? current[current.length - 1].version || 0 : 0;

        if (expectedVersion !== undefined && expectedVersion !== currentVersion) {
            throw new ConcurrencyError(`Expected version ${expectedVersion}, got ${currentVersion}`);
        }

        const appended = (events || []).map((event, index) => ({
            ...clone(event),
            version: currentVersion + index + 1
        }));

        this.events.set(instanceId, [...current, ...appended]);
        return clone(appended);
    }
}

class Repository {
    constructor({
        database = new DefaultDatabase(),
        service,
        modelFactory = (state = {}) => new Model(state),
        snapshotEvery = 0,
        channelPrefix = "events"
    } = {}) {
        this.database = database;
        this.service = service;
        this.modelFactory = modelFactory;
        this.snapshotEvery = snapshotEvery;
        this.channelPrefix = channelPrefix;
    }

    async getInstance(instanceId) {
        const snapshot = (await this.database.readSnapshot(instanceId)) || { version: 0, state: {} };
        const instance = this.modelFactory(clone(snapshot.state) || {}, instanceId);
        if (instance && instance.id === undefined) instance.id = instanceId;

        const events = await this.database.readEvents(instanceId, snapshot.version || 0);
        for (const event of events) {
            await this.apply(instance, event);
        }

        const versionFromEvents = events.length ? (events[events.length - 1].version || 0) : (snapshot.version || 0);
        instance._version = versionFromEvents;

        return instance;
    }

    async persist(instanceId, events = [], options = {}) {
        const current = await this.getInstance(instanceId);
        const expectedVersion = options.expectedVersion !== undefined ? options.expectedVersion : current._version || 0;
        const storedEvents = await this.database.appendEvents(instanceId, events, expectedVersion);

        for (const event of storedEvents) {
            await this.apply(current, event);
            await this.publish(instanceId, event);
        }

        const currentVersion = storedEvents.length ? (storedEvents[storedEvents.length - 1].version || 0) : expectedVersion;
        current._version = currentVersion;

        if (this.snapshotEvery > 0 && currentVersion > 0 && currentVersion % this.snapshotEvery === 0) {
            await this.database.saveSnapshot(instanceId, current, currentVersion);
        }

        return {
            instance: current,
            version: currentVersion,
            events: storedEvents
        };
    }

    async apply(instance, event) {
        if (instance && typeof instance.apply === "function") {
            instance.apply(event);
        }

        const eventName = getEventName(event);
        const handlerName = eventName ? `on${eventName}` : "";

        if (handlerName && this.service && typeof this.service[handlerName] === "function") {
            await this.service[handlerName]({ instance, event });
        }

        return instance;
    }

    async publish(instanceId, event) {
        if (!this.service || !this.service.broker || typeof this.service.broker.sendToChannel !== "function") return;

        const eventName = getEventName(event) || "event";
        await this.service.broker.sendToChannel(`${this.channelPrefix}.${eventName}`, {
            instanceId,
            event
        });
    }
}

function createRepositoryMixin(options = {}) {
    return {
        created() {
            this.repository = new Repository({
                ...options,
                service: this
            });
        },
        methods: {
            async getInstance(instanceId) {
                return this.repository.getInstance(instanceId);
            },
            async persist(instanceId, events, persistOptions) {
                return this.repository.persist(instanceId, events, persistOptions);
            },
            async apply(instance, event) {
                return this.repository.apply(instance, event);
            }
        }
    };
}

module.exports = {
    ConcurrencyError,
    Model,
    DefaultDatabase,
    Repository,
    createRepositoryMixin,
    getEventName
};
