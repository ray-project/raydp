# Contributing to RayDP

Thank you for your interest in contributing to RayDP!

## Development Environment Setup

RayDP is a mixed-language project (Scala + Python). To set up your development environment correctly, please ensure your system meets the following requirements.

### Prerequisites
* **Java:** JDK 8 (Required for Spark 3.x compatibility).
* **Python:** Python 3.10+ (Recommended).
* **Maven:** 3.6+

### Project Configuration
Regardless of your IDE (IntelliJ, VS Code, Eclipse), your project structure must be configured as follows:

1.  **Maven Root:** The build system is rooted in the `core` directory. You should import `core/pom.xml` as your Maven project.
2.  **Java SDK:** Ensure the project SDK is set to **Java 8**.
3.  **Python Sources:** The `python/` directory in the root must be marked as a source root so your IDE can resolve the `raydp` package.

### Build & Verification

#### 1. Build the Core (Scala/Java)
The core logic resides in Scala. You must build this first to generate the necessary JARs.
```bash
cd core
mvn clean package -DskipTests