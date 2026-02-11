# Dependencies

PUM allows you to declare Python package dependencies that your application scripts (e.g., migration hooks) require. Before running migrations, PUM checks that all declared dependencies are installed and satisfy the specified version constraints. If a dependency is missing, PUM can download it into a temporary directory so that your scripts can import it.

This is especially useful when running inside environments like QGIS, where you don't control the system Python packages.

## How it works

When PUM resolves dependencies:

1. If there are dependencies to resolve, a temporary directory is created and inserted at position 0 of `sys.path`, so it takes precedence over system-wide packages.
2. Each declared dependency is checked:
      - If already installed and the version satisfies the constraints, it is used as-is.
      - If not installed, PUM runs `pip install <package> --target <tmpdir>` to download it into the temporary directory.
      - If installed but the version does **not** satisfy the constraints, PUM raises an error.
3. Migration hooks and application scripts can then import the dependency normally.
4. When the PUM session ends, the temporary directory is cleaned up and removed from `sys.path`.

## Configuration

Dependencies are defined in the `dependencies` section of your `.pum.yaml` [configuration file](configuration/configuration.md), using the [`DependencyModel`](configuration/models/dependency_model.md).

Each dependency accepts:

- **name**: The Python package name (as it appears on PyPI)
- **minimum_version** *(optional)*: The minimum required version
- **maximum_version** *(optional)*: The maximum allowed version

### Example

```yaml
dependencies:
  - name: pirogue
    minimum_version: 3.0.0
```

### Version constraints

You can specify either or both version bounds:

```yaml
dependencies:
  - name: some-package
    minimum_version: 1.2.0
    maximum_version: 2.0.0
```

If the installed version falls outside the specified range, PUM will raise an error.
