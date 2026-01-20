# Backward Compatibility for Hook Configuration

## Overview

PUM now supports reading legacy `.pum.yaml` files that use the old field names `migration_hooks`, `pre`, and `post`. This ensures smooth migration for users upgrading to the new terminology.

## What Changed

The refactoring renamed hook-related fields for better clarity:
- **Old**: `migration_hooks` with `pre`/`post` hooks
- **New**: `application` with `drop`/`create` hooks

## Future Considerations

While backward compatibility is fully supported, we recommend updating `.pum.yaml` files to the new format at your convenience for clarity and consistency with documentation.
