# imicros-object-store

Moleculer Object Store mit Event-Sourcing-Bausteinen.

## Enthaltene Bausteine

- `lib/classes/cqrs/cqrs.js`
  - `Repository` als Basisklasse für Laden, Persistieren und Rehydrieren (Snapshot + Events)
  - `createRepositoryMixin(...)` für einfache Integration in Moleculer Services
  - `Model` Basisklasse mit `apply(event)` → `on<Event>(event)`
  - `DefaultDatabase` InMemory-Implementierung
- `lib/classes/db/cassandraCQRS.js`
  - `CassandraCQRSDatabase` als Cassandra-basierte DB-Implementierung
  - `createCassandraCQRSMixin(...)` als Mixin

## Event-Handling

- Model-Handler: `on<Event>(event)`
- Service-Handler: `on<Event>({ instance, event })`
- Event-Publishing über Moleculer Channels via `broker.sendToChannel("events.<Event>", { instanceId, event })`
