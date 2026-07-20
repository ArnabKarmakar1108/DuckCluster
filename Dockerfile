# syntax=docker/dockerfile:1

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY . .
RUN mvn -q -pl coordinator,worker -am package -DskipTests

FROM eclipse-temurin:17-jre-jammy AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3 python3-pip \
    && pip3 install --no-cache-dir duckdb \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/coordinator/target/duckcluster-coordinator-0.1.0-SNAPSHOT.jar /app/coordinator.jar
COPY --from=build /build/worker/target/duckcluster-worker-0.1.0-SNAPSHOT.jar /app/worker.jar
COPY --from=build /build/common/target/classes /app/common/target/classes

COPY scripts/prepare-demo-shards.py /app/scripts/prepare-demo-shards.py
COPY tests/integration/data/demo-events.csv /app/tests/integration/data/demo-events.csv
COPY docker/prepare-data.sh /app/docker/prepare-data.sh

RUN chmod +x /app/docker/prepare-data.sh

EXPOSE 8080 9090 9101
