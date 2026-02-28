# Contributing to RayDP

Thank you for your interest in contributing to RayDP!

## Development Environment Setup

RayDP is a mixed-language project (Scala + Python). To set up your development environment correctly, please ensure your system meets the following requirements.

### Prerequisites
* **Java:** JDK 8 or JDK 17. The project compiles with Java 8 source/target compatibility, but the CI environment runs on **JDK 17**. Using JDK 17 locally is recommended to match CI.
* **Python:** Python 3.10+ (Recommended).
* **Maven:** 3.6+

### Project Configuration
Regardless of your IDE (IntelliJ, VS Code, Eclipse), your project structure must be configured as follows:

1.  **Maven Root:** The build system is rooted in the `core` directory. You should import `core/pom.xml` as your Maven project.
2.  **Java SDK:** Set the project SDK to **Java 8** or **Java 17** (17 recommended to match CI).
3.  **Python Sources:** The `python/` directory in the root must be marked as a source root so your IDE can resolve the `raydp` package.

### Build & Verification

The easiest way to build is with the project's `build.sh` script, which builds both the core (Scala/Java) and the Python wheel in one step:
```bash
./build.sh
```
This script runs `mvn clean package -DskipTests` inside `core/`, then builds the Python wheel from `python/` and copies it to `dist/`.

If you prefer to run each step manually:

#### 1. Build the Core (Scala/Java)
The core logic resides in Scala. You must build this first to generate the necessary JARs.
```bash
cd core
mvn clean package -DskipTests