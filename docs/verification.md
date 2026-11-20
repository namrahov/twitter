# Current build: Gradle — 2026-11-20

The user requested Gradle. The project now uses Gradle 9.7.1 Wrapper, Groovy DSL and a Java 26 toolchain. Maven build and wrapper files and the old target directory were removed.

- Baseline before migration: 8 PostgreSQL integration tests passed with Maven.
- Gradle validation: gradlew.bat --no-daemon --console=plain clean build integrationTest passed.
- Integration report: 8 tests, 0 failures, 0 errors.
- Gradle-built executable JAR: build/libs/twitter-0.0.1-SNAPSHOT.jar; HTTP health returned UP.
- Default test task has no unit tests; PostgreSQL tests run explicitly with integrationTest.
- Existing Liquibase changelog and application source files were not changed.
- Reviewed build.gradle and settings.gradle inline: no outstanding findings.
- README, IntelliJ setup instructions and AGENTS.md now specify Gradle.
- No commit or push.

The earlier verification record below describes the initial scaffold before the Gradle migration.

---
# Initial verification — 2026-11-20

- Java: Eclipse Temurin 26.0.2.1+1, downloaded from Adoptium and SHA-256 verified.
- Build: Maven Wrapper clean package passed.
- Integration: 8 PostgreSQL tests passed, repeated successfully against the existing schema.
- Application: started twice, GET /actuator/health returned UP on both runs.
- Liquibase: 4 changesets applied; repeat startup executed 0 and recognized all 4 as previously applied.
- twitter database: users, posts, follows, outbox plus databasechangelog and databasechangeloglock.
- All application tables are empty; integration fixtures were rolled back in twitter_test.
- Original and preserved reference PDF hashes match.

| Verification stage | Result |
| --- | --- |
| Compile | Passed |
| Static analysis | Not configured |
| Tests | Integration: 8 passed, twice; default Maven test phase has no unit tests |
| Coverage | Not configured |
| Dependency scanning | Not configured |
| Secrets | APM Git scan unavailable: no Git checkout; project text manually checked for the supplied password |

Code review: Java entry point, integration tests, datasource configuration and initial schema reviewed inline. Field injection in the integration test was replaced with constructor injection. No outstanding findings. apm review cannot produce a Git diff because this is a new directory without a Git repository.

The first HTTP smoke run encountered a Java NIO Unix-domain socket path error in the Codex Windows process environment. Validation succeeded using a temporary JVM-only setting, -Djdk.net.unixdomain.tmpdir, pointing to the task scratch directory. No application setting was changed for this. If reproduced in a Codex-launched process, choose a writable local socket directory using that JVM property. The application was stopped after verification.

No commit, push or deployment was performed.

