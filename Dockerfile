# https://www.keycloak.org/server/containers
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM quay.io/keycloak/keycloak:26.5.5
COPY --from=builder /build/target/keycloak-kafka-*-jar-with-dependencies.jar /opt/keycloak/providers/
RUN /opt/keycloak/bin/kc.sh build

ENTRYPOINT ["/opt/keycloak/bin/kc.sh"]