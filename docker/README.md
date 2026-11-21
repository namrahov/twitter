# Local infrastructure

Kafka and Redis for the twitter app. PostgreSQL is not here: it runs on the host at `localhost:5433`.

| Service | Image | Host port |
|---|---|---|
| Kafka (KRaft, single node) | `apache/kafka:3.9.1` | `9092` |
| Redis | `redis:7.4.2` | `6380` |

Redis uses host port 6380 so it does not clash with a Redis already installed on the machine.
It is capped at 256 MB and evicts least recently used keys when full (`allkeys-lru`): it only holds caches.
The app defaults match these ports (`KAFKA_BOOTSTRAP_SERVERS=localhost:9092`, `REDIS_PORT=6380`).

## Run

Needs Docker Desktop. From the project root:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Check:

```bash
docker compose -f docker/docker-compose.yml ps
```

```bash
docker exec twitter-redis redis-cli ping
```

## Stop

Keeps the data (volumes `twitter_kafka-data`, `twitter_redis-data`):

```bash
docker compose -f docker/docker-compose.yml down
```

Wipe Kafka topics/offsets and Redis timelines too:

```bash
docker compose -f docker/docker-compose.yml down -v
```

After a wipe the app still works: Kafka topics are recreated on startup and empty timelines fall back to SQL.

## Useful commands

```bash
docker exec twitter-redis redis-cli KEYS 'timeline:*'
```

```bash
docker exec twitter-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tweet-created --from-beginning
```
