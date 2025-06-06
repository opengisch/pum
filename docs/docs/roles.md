# Roles and Permissions

PUM supports an opiniated but flexible role and permission system for managing database access and privileges. Roles and permissions can be defined in your configuration file using the following schema and logic.

## Permission Model

A **Permission** defines an allowed action (such as `read` or `write`) on one or more database schemas.

**Pydantic Model:**

- `type`: The type of permission. Must be either `"read"` or `"write"` (see `PermissionType` enum in code).
- `schemas`: A list of schema names (strings) this permission applies to.

**Example:**

```yaml
permissions:
  - type: read
    schemas: [public, data]
  - type: write
    schemas: [data]
```

## Role Model

A **Role** groups permissions and can optionally inherit from another role.

**Pydantic Model:**

- `name`: Name of the role (string).
- `permissions`: List of `PermissionModel` objects.
- `inherit`: (Optional) Name of another role to inherit permissions from.
- `description`: (Optional) Description of the role.

**Example:**
```yaml
roles:
  - name: reader
    permissions:
      - type: read
        schemas: [public]
    description: Read-only access to public schema
  - name: editor
    permissions:
      - type: write
        schemas: [data]
    inherit: reader
    description: Can write to data schema, inherits read from reader
```

## RoleManager Logic

- The `RoleManager` class can be initialized with a list of roles (as dicts or `Role` objects).
- It checks that inherited roles exist and builds a mapping of role names to `Role` objects.
- The `create` method can be used to create roles and grant permissions in the database.
- Permissions are enforced by granting the specified actions on the listed schemas to the role.

**PermissionType Enum:**

- `read`: Grants `USAGE` and `SELECT` privileges.
- `write`: Grants `INSERT`, `UPDATE`, and `DELETE` privileges.

## Summary

- Define roles and permissions in your config YAML under the `roles` key.
- Use inheritance to avoid duplication and build role hierarchies.
- Each permission specifies a type and a list of schemas.
- The system ensures only valid roles and permissions are created and applied.

For more details, see the [configuration](./configuration.md) page or the [RoleManager](./api/role_manager.md) class.
