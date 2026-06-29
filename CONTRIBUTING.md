# Contributing

Thanks for contributing! This is a maintained fork of
[SnuK87/keycloak-kafka](https://github.com/SnuK87/keycloak-kafka) (Apache License 2.0).

## Prerequisites

- JDK 21 (the build targets `--release 21`)
- Maven 3.9+ **or** Docker (the project ships a containerized build, no local Maven needed)

## Build

```bash
mvn clean package
```

This produces the deployable fat jar `target/keycloak-kafka-<version>-jar-with-dependencies.jar`
(via the `maven-assembly-plugin`). `keycloak-*` and `jboss-logging` are `provided` and supplied by the
Keycloak server at runtime, so they are intentionally **not** bundled; `kafka-clients` **is** bundled.

## Tests

Unit tests (`*Tests`) run under Surefire; the Testcontainers integration test (`*IT`) runs under Failsafe.

```bash
# Unit tests only
mvn test

# Unit + integration tests (needs a reachable Docker daemon; the IT skips itself if Docker is absent)
mvn verify

# No local Maven? Run the unit tests in a container:
docker compose --profile test run --rm test
```

The integration test starts a real Kafka broker via Testcontainers, so a working Docker socket is
required for it to actually execute (it is skipped, not failed, when Docker is unavailable).

## Coding style

- Java sources use **tabs** for indentation (see `.editorconfig`).
- Use the `jboss-logging` `Logger` with parameterized (`debugf`/`warnf`/`errorf`) calls — avoid string
  concatenation in log statements.
- Keep the public configuration contract stable: the `KAFKA_*` env vars and
  `--spi-events-listener-kafka-*` parameters are user-facing.

## Releasing

Releases are published automatically by `.github/workflows/release.yml` when a version tag is pushed:

```bash
# 1. bump <version> in pom.xml and commit
# 2. tag and push
git tag vX.Y.Z
git push origin vX.Y.Z
```

The workflow builds the fat jar and attaches it to a GitHub Release with auto-generated notes.
Keep the `keycloak.version` in `pom.xml` aligned with the Keycloak server image in `Dockerfile`.
