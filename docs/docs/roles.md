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
- The `create_roles` method creates roles and optionally grants permissions in the database.
- Permissions are enforced by granting the specified actions on the listed schemas to the role.

**PermissionType Enum:**

- `read`: Grants `USAGE` and `SELECT` privileges.
- `write`: Grants `INSERT`, `UPDATE`, and `DELETE` privileges.

## DB-Specific Roles

When you have several database instances of the same module in a single PostgreSQL cluster, the roles defined in the configuration would be shared across all databases. To isolate permissions per database, you can create **DB-specific roles** by providing a `suffix`.

For example, with a role `tww_user` and suffix `lausanne`:

1. A specific role `tww_user_lausanne` is created and granted the configured permissions.
2. The generic role `tww_user` is also created (unless `create_generic=False` / `--no-create-generic`).
3. The generic role is granted membership of the specific role, so that `tww_user` inherits `tww_user_lausanne`'s permissions.

This way, users assigned to `tww_user` automatically get access to the Lausanne database, and you can repeat the process for other databases (e.g. `tww_user_zurich`).

### CLI Usage

```bash
# Create specific roles with suffix, plus generic roles with inheritance
pum -p mydb role create --suffix lausanne

# Create only the specific roles (no generic roles)
pum -p mydb role create --suffix lausanne --no-create-generic
```

### Python API

```python
role_manager.create_roles(
    connection=conn,
    suffix="lausanne",         # creates <role>_lausanne
    create_generic=True,       # also create the base roles (default)
    grant=True,                # grant configured permissions
    commit=True,
)
```

## Checking Roles

You can audit whether the database roles match the configuration using the `check` action. This verifies:

- Each expected role exists in the database.
- Each role has the expected permissions (read/write) on the configured schemas.
- No unknown roles have access to the configured schemas.

### CLI Usage

```bash
# Check that roles match the config
pum -p mydb role check
```

The check automatically discovers all matching roles, including both generic roles
and any DB-specific (suffixed) variants.

The output uses colored markers to indicate status:

- **✓** role/permission matches the configuration
- **✗** role is missing or permission doesn't match
- **?** unknown role with access to a configured schema

### Python API

```python
result = role_manager.check_roles(connection=conn)

if result.ok:
    print("All roles match the configuration")
else:
    for name in result.missing_roles:
        print(f"Missing role: {name}")

    for role_status in result.configured_roles:
        for sp in role_status.schema_permissions:
            if not sp.ok:
                print(f"  {role_status.name}/{sp.schema}: expected {sp.expected.value}, "
                      f"has_read={sp.has_read}, has_write={sp.has_write}")

    for unknown in result.unknown_roles:
        print(f"Unknown role {unknown.name} on schemas: {unknown.schemas}")
```

## Summary

- Define roles and permissions in your config YAML under the `roles` key.
- Use inheritance to avoid duplication and build role hierarchies.
- Each permission specifies a type and a list of schemas.
- The system ensures only valid roles and permissions are created and applied.
- Use `role check` to audit whether the database matches the configuration.

For more details, see the [configuration](./configuration.md) page or the [RoleManager](./api/role_manager.md) class.
