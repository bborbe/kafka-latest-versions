# Kafka latest versions

Print latest versions of each application seen

## Run

```bash
go run main.go \
-kafka-brokers=kafka:9092 \
-kafka-schema-registry-url=http://schema-registry:8081 \
-kafka-available-version-topic=application-version-available \
-kafka-latest-version-topic=application-version-latest \
-datadir=/tmp \
-v=2
```
