# Version

The installed PUM version is exposed at the package root, following PEP 396.

```python
import pum

pum.__version__   # str, e.g. "1.7.2"
pum.VERSION       # packaging.version.Version, convenient for comparisons
```

`pum.VERSION` is a parsed [`packaging.version.Version`](https://packaging.pypa.io/en/stable/version.html)
object, which is useful for programmatic comparisons:

```python
import pum
import packaging.version

if pum.VERSION >= packaging.version.Version("1.7"):
    ...
```

The CLI exposes the same value via:

```sh
pum --version
```

## Resolution order

The version is resolved at import time using the first source available:

1. A bundled `pum-*.dist-info/METADATA` directory next to the `pum` package.
   This ensures the reported version always matches the code being executed,
   even when PUM is vendored inside another package and a different `pum`
   happens to be installed elsewhere on `sys.path`.
2. `git describe --tags --always --dirty` when running from a source checkout.
3. `importlib.metadata.version("pum")` for a regular installed package.
4. `"0.0.0"` as an ultimate fallback.
