# Contributing to RayDP

Thank you for your interest in contributing to RayDP!

## Development Environment Setup

RayDP is a mixed-language project (Scala + Python). To set up your development environment correctly in IntelliJ IDEA, please follow these steps.

### Prerequisites
* **Java:** JDK 17 (Recommended to match CI).
* **Python:** Python 3.8+ (Recommended: Use Conda or virtualenv).
* **Maven:** 3.6+

### IntelliJ IDEA Configuration

1.  **Import the Project**
    * Open the root `raydp` folder in IntelliJ.
    * If prompted, choose **"Trust Project"**.
    * If the Maven project is not auto-detected, right-click `core/pom.xml` in the Project view and select **"Add as Maven Project"**.

2.  **Project Structure** (`Cmd + ;` on macOS)
    * Go to **Project Settings > Project**.
    * Set **SDK** to **17** (Java 17).
    * Set **Language Level** to **17**.

3.  **Module Configuration**
    To ensure code completion works for both Scala and Python:
    * Go to **Project Settings > Modules**.
    * **Scala:** Select the `raydp-core` module. Ensure `src/main/scala` is marked as **Sources** (Blue) and `src/test/scala` as **Tests** (Green).
    * **Python:** Click `+` > **New Module** (or "Import Module").
        * Select the `python/` directory in your project root.
        * Set the **Module SDK** to your local Python environment (e.g., Conda `raydp-dev`).
        * Mark `python/raydp` as **Sources** if not automatically detected.

### Verification
To verify your setup, run the build from the terminal (or the Maven side panel):

```bash
mvn clean package -DskipTests
