# Dependencies

PUM allows you to declare Python package dependencies that your application scripts (e.g., migration hooks) require. Before running migrations, PUM checks that all declared dependencies are installed and satisfy the specified version constraints. If a dependency is missing, PUM can install it into a cache directory so that your scripts can import it.

This is especially useful when running inside environments like QGIS, where you don't control the system Python packages.

## How it works

When PUM resolves dependencies:

1. If there are dependencies to resolve, a cache directory is picked for this set of dependencies and its `site-packages` directories are inserted at the front of `sys.path`, so they take precedence over system-wide packages.
2. Each declared dependency is checked:
      - If already installed — system-wide or from the cache — and the version satisfies the constraints, it is used as-is.
      - If not installed, PUM runs `pip install <package> --prefix <cachedir>` to install it into the cache.
      - If installed but the version does **not** satisfy the constraints, PUM either installs a matching version into the cache, or raises an error when installation is not enabled.
3. Migration hooks and application scripts can then import the dependency normally.

`--prefix` is used rather than `--target` so that pip does not reinstall the whole dependency closure: with `--target` it forces `--ignore-installed`, which would shadow packages the host application already provides.

## Cache

Installed dependencies are kept between runs, so a module is not reinstalled every time it is loaded. One directory is used per set of dependencies, keyed by the declared version constraints, the Python version and the platform.

The location follows the platform convention, and can be overridden with the `PUM_CACHE_DIR` environment variable:

| Platform | Default location |
| --- | --- |
| Linux | `$XDG_CACHE_HOME/pum` (`~/.cache/pum`) |
| macOS | `~/Library/Caches/pum` |
| Windows | `%LOCALAPPDATA%\pum\Cache` |

Nothing is removed automatically: a directory another process has already imported from cannot be deleted safely. Use the [`cache` command](cli/cache.md) to inspect it, or to reset it if an install was interrupted and left it in a broken state:

```bash
pum cache path     # print the cache directory
pum cache list     # show the cached dependency sets and their size
pum cache clear    # delete them all
```

Run `pum cache clear` only while no module is loaded.

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
