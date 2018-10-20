# Kafka latest versions

Print latest versions of each application seen

## Run

```bash
go run main.go \
-kafka-brokers=kafka:9092 \
-kafka-topic=application-version-available \
-datadir=/tmp \
-v=2
```
