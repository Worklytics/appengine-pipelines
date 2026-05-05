# AI Agents Style Guide

When modifying Java code in this repository, please adhere to the following guidelines:

## Code Style

- **Avoid Fully Qualified Names (FQNs)**: Do not use fully qualified class names directly in code (e.g., `java.util.Base64`). Always use standard `import` statements at the top of the file to import the necessary classes and refer to them by their simple names.
- **Dependency Injection**: Favor Dependency Injection (Dagger) over static singletons or utility classes. Create injectable services with `@Singleton` and `@Inject` constructors where applicable.
- **Constants**: Extract reused string literals and configuration keys into `public static final String` constants rather than hardcoding them multiple times.
- **Validation**: Ensure settings and inputs are appropriately validated, providing standard exception types (`IllegalArgumentException`) for malformed inputs, such as GCP resource names.
- **Documentation**: Provide examples for complex configuration values or properties (like GCP KMS keys or URIs) inside JavaDocs.
