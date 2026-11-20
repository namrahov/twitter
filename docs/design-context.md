# Design context

Source: reference/1_Twitter_System_Design_Etrafli_Izah.pdf (provided by the user).
The original PDF is preserved; this note records direction, not authorization to build future features.

- Spring Boot application, PostgreSQL as durable storage.
- Core schema: posts(id, user_id, content, created_at), follows(follower_id, followee_id).
- Timeline starts with a SQL join, ordered by created_at DESC and id DESC.
- Future direction: hybrid fan-out (push for regular users, pull for celebrities), Redis sorted sets, Caffeine local cache, bounded database fallback, cache invalidation with reconnect handling.
- Future storage: user_id-based sharding; application-generated Snowflake BIGINT IDs.
- Future events: post and outbox written in one transaction; publisher to Kafka; at-least-once delivery and idempotent consumers.
- Duplicate-event handling was left open in the PDF and remains a discussion topic.
- PDF scale and latency numbers are interview assumptions, not measured requirements of this starter.

## Initial implementation choices

- One database, one application; no sharding, Kafka, Redis, workers or ID generator yet.
- users adds minimal local identities for foreign keys; authentication is not implemented.
- BIGINT user and post IDs have no database sequence: a future Snowflake implementation can supply them.
- Posts currently allow 1-280 characters with a nonblank check. The 280 limit is an initial project choice, not an explicit requirement from the PDF.
- Composite follows primary key supports following lookup; reverse index supports follower enumeration.
- Outbox includes user_id for future shard locality, JSONB payload, publish status, attempts and timestamps. No event publisher is implemented.
- Outbox IDs have no foreign key to posts/users so durable events can survive later aggregate deletion.
- Foreign keys are local to this single database. Revisit them when actual sharding is designed.
- Hibernate validates; only Liquibase changes the schema.

## Collaboration preference

Discuss by default; write code only when the user explicitly requests code or a concrete implementation.
