# Local setup (2026-11-20)

- Project: `C:\Users\AZINTELECOM\IdeaProjects\twitter`
- JDK 26: `C:\Users\AZINTELECOM\.jdks\temurin-26.0.2.1`
- Database: `jdbc:postgresql://localhost:5433/twitter`
- Integration database: `jdbc:postgresql://localhost:5433/twitter_test`
- Local database user: `postgres`
- Password: supply `DB_PASSWORD` through the terminal or IntelliJ run configuration. No password is saved in the project.

In IntelliJ, add the JDK above if it is not listed, select it as Project SDK and Gradle JVM, and import `build.gradle`.
The project uses a local development database account; choose a dedicated least-privilege account when deployment is discussed.
