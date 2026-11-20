# Twitter

Java 26 / Spring Boot 4.1.3 / Gradle 9.7.1 Wrapper / PostgreSQL / Liquibase.

## IntelliJ IDEA

Open this directory as a Gradle project. Select a JDK 26 as Project SDK and Gradle JVM.
The locally downloaded JDK location is recorded in docs/local-setup.md.
Run `az.twitter.TwitterApplication` with `DB_PASSWORD` set in the run configuration.
Optional environment variables: `DB_URL`, `DB_USERNAME`, `SERVER_PORT`.
The default URL is `jdbc:postgresql://localhost:5433/twitter`.
The password is required and has no source-code default. `.env.example` is a template; it is not automatically loaded.

## PowerShell

```powershell
$env:JAVA_HOME = '<path-to-jdk-26>'
$env:DB_USERNAME = 'postgres'
$dbCredential = Get-Credential -UserName 'postgres' -Message 'Local PostgreSQL password'
$env:DB_PASSWORD = $dbCredential.GetNetworkCredential().Password
.\gradlew.bat bootRun
```

Liquibase automatically applies four initial changesets at startup.
Tables: `users`, `posts`, `follows`, `outbox`, plus Liquibase history and lock tables.
Hibernate uses `ddl-auto=validate` and never creates tables.
There are no application REST endpoints yet. Health: `http://localhost:8080/actuator/health`.

## Verification

```powershell
.\gradlew.bat clean build
# Requires a separate, empty twitter_test database and the same password environment variable:
.\gradlew.bat integrationTest
```

Integration tests use `twitter_test`, roll back their data and check actual PostgreSQL constraints, timeline ordering and post/outbox transaction rollback.
Override `TEST_DB_URL`, `TEST_DB_USERNAME`, `TEST_DB_PASSWORD` if needed.
Run `.\gradlew.bat integrationTest --rerun-tasks` to repeat verification against an existing schema.
Integration tests run separately from `build`; use `.\gradlew.bat clean build integrationTest` for full verification.
The runnable application JAR is created in `build/libs/`.

See docs/design-context.md for the current scope and the preserved PDF in docs/reference/.
