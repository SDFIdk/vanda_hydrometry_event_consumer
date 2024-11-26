FROM vanda_hydrometry_event_consumer_temp AS builder

FROM eclipse-temurin:21.0.4_7-jdk-alpine

RUN apk upgrade --available --no-cache

COPY --from=builder /vanda_hydrometry_event_consumer/target/vanda-hydrometry-event-consumer.jar ./vanda-hydrometry-event-consumer.jar

ENTRYPOINT ["java", "-jar", "vanda-hydrometry-event-consumer.jar"]