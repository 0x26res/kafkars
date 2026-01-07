# Bench

Util to test the library

## Set up Kafka

Best to set up with kafka-kraft:

```shell
docker run --name=simple_kafka -p 9092:9092 -d bashj79/kafka-kraft
docker stop simple_kafka
docker start simplle_kafka
```

## Set up topics

```shell
docker exec simple_kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic=topic_1 \
  --partitions=1 \
  --bootstrap-server=localhost:9092 \
  --replication-factor=1
```

## List topics

```shell
docker exec simple_kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server=localhost:9092
```

## Publish to topic

```shell
docker exec -it simple_kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server=localhost:9092 \
  --topic=topic_1 \
  --property parse.key=true \
  --property key.separator=:
```

Then type messages as `key:value`, e.g. `mykey:myvalue`.

## Stream topic

```shell
docker exec simple_kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic=topic_1 \
  --bootstrap-server=localhost:9092 \
  --property print.key=true \
  --from-beginning
  ```

## Run the test consumer

Topic format: `topic_name:policy[:time_ms]`

Policies:

- `latest` - start from latest offset
- `earliest` - start from beginning
- `committed` - start from committed offset
- `relative_time:ms` - start from relative time (ms ago)
- `absolute_time:ms` - start from absolute timestamp (epoch ms)

```shell
# Poll from latest offset
uv run python bench/poll_messages.py localhost:9092 topic_1:latest

# Poll from the beginning
uv run python bench/poll_messages.py localhost:9092 topic_1:earliest

# Poll multiple topics with different policies
uv run python bench/poll_messages.py localhost:9092 topic_2:earliest topic_1:earliest

# Poll from 1 hour ago (3600000 ms)
uv run python bench/poll_messages.py localhost:9092 topic_1:relative_time:3600000

# With custom timeout and max batches
uv run python bench/poll_messages.py localhost:9092 topic_1:latest -t 2000 -n 10
```
