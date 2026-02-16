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
2. The generic role `tww_user` is also created.
3. The generic role is granted membership of the specific role, so that `tww_user` inherits `tww_user_lausanne`'s permissions.

This way, users assigned to `tww_user` automatically get access to the Lausanne database, and you can repeat the process for other databases (e.g. `tww_user_zurich`).

### CLI Usage

```bash
# Create specific roles with suffix, plus generic roles with inheritance
pum -p mydb role create --suffix lausanne
```

### Python API

```python
role_manager.create_roles(
    connection=conn,
    suffix="lausanne",         # creates <role>_lausanne + base roles
    grant=True,                # grant configured permissions
    commit=True,
)
```

## Listing Roles

You can list all database roles related to the module's schemas using the `list` action. This shows:

- Each configured role (generic and DB-specific/suffixed variants).
- Which schemas each role can read or write, and whether this matches expectations.
- Any other (unconfigured) roles that have access to the module's schemas.
- Other login roles (non-superuser) that exist but have no access to any configured schema.
- Whether each role is a superuser or can log in.

### CLI Usage

```bash
# List roles related to the module
pum -p mydb role list
```

The listing automatically discovers all matching roles, including both generic roles
and any DB-specific (suffixed) variants.

The output uses colored markers to indicate status:

- **✓** permission matches the configuration
- **✗** permission doesn't match
- **?** other role with access to a configured schema

### Python API

```python
result = role_manager.roles_inventory(connection=conn)

for name in result.missing_roles:
    print(f"Missing role: {name}")

for role_status in result.configured_roles:
    for sp in role_status.schema_permissions:
        if not sp.satisfied:
            print(f"  {role_status.name}/{sp.schema}: expected {sp.expected.value}, "
                  f"has_read={sp.has_read}, has_write={sp.has_write}")

for other in result.unknown_roles:
    print(f"Other role {other.name} on schemas: {other.schemas}")
    if other.login:
        print("  (can log in)")

for name in result.other_login_roles:
    print(f"Login role with no schema access: {name}")
```

## Login Roles

PUM provides utilities to manage login roles (database users) independently of
the module's configured roles.

### Creating a Login Role

```bash
# Create a login role
pum -p mydb role create-login --name john
```

```python
RoleManager.create_login_role(connection=conn, name="john", commit=True)
```

### Dropping a Login Role

```bash
# Drop a login role
pum -p mydb role drop-login --name john
```

```python
RoleManager.drop_login_role(connection=conn, name="john", commit=True)
```

### Listing All Login Roles

List all non-superuser login roles in the database (regardless of the module configuration):

```bash
pum -p mydb role login-roles
```

```python
for name in RoleManager.login_roles(connection=conn):
    print(name)
```

### Listing Members of a Role

Show which login users are members of a given role:

```bash
pum -p mydb role members --roles tww_viewer
```

```python
for name in RoleManager.members_of(connection=conn, role_name="tww_viewer"):
    print(name)
```

## Summary

- Define roles and permissions in your config YAML under the `roles` key.
- Use inheritance to avoid duplication and build role hierarchies.
- Each permission specifies a type and a list of schemas.
- The system ensures only valid roles and permissions are created and applied.
- Use `role list` to audit which roles have access to the module's schemas.
- Use `role create-login`, `role drop-login`, `role login-roles`, and `role members` to manage login users.

For more details, see the [configuration](./configuration.md) page or the [RoleManager](./api/role_manager.md) class.
