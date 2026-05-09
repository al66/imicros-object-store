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
  - `createCassandraCQRSMixin(...)` als Mixin (verbindet im `started` Hook, initialisiert Tabellen, schließt im `stopped` Hook)
- `lib/classes/db/postgresqlCQRS.js`
  - `PostgresCQRSDatabase` als PostgreSQL-basierte DB-Implementierung
  - `createPostgresCQRSMixin(...)` als Mixin (verbindet im `started` Hook, initialisiert Schema/Tabellen, schließt im `stopped` Hook)

## Event-Handling

- Model-Handler: `on<Event>(event)`
- Service-Handler: `on<Event>({ instance, event })`
- Event-Publishing über Moleculer Channels via `broker.sendToChannel("events.<Event>", { instanceId, event })`
