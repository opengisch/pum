#!/usr/bin/env python
"""Demo script showing version switching with cleanup."""

from pathlib import Path
from unittest.mock import Mock
from pum.pum_config import PumConfig
import sys

print("=== Testing Version Switch Scenario ===\n")

# Clear modules
for key in list(sys.modules.keys()):
    if "view" in key or "helper" in key:
        del sys.modules[key]

print("1. Loading Version 1...")
v1_path = Path("test") / "data" / "hook_version_switch" / "v1"
cfg_v1 = PumConfig(
    base_path=v1_path,
    pum={"module": "test_v1"},
    application={"create": [{"file": "app/create_hook.py"}]},
    validate=False,
)

handlers_v1 = cfg_v1.create_app_handlers()
mock_conn = Mock()
handlers_v1[0].execute(connection=mock_conn, parameters={})
print("   ✓ Version 1 loaded and executed successfully")
print(f"   Cached handlers: {len(cfg_v1._cached_handlers)}")

print("\n2. Cleaning up Version 1...")
cfg_v1.cleanup_hook_imports()
print("   ✓ Version 1 cleaned up")
print(f"   Cached handlers after cleanup: {len(cfg_v1._cached_handlers)}")

print("\n3. Loading Version 2...")
v2_path = Path("test") / "data" / "hook_version_switch" / "v2"
cfg_v2 = PumConfig(
    base_path=v2_path,
    pum={"module": "test_v2"},
    application={"create": [{"file": "app/create_hook.py"}]},
    validate=False,
)

handlers_v2 = cfg_v2.create_app_handlers()
handlers_v2[0].execute(connection=mock_conn, parameters={})
print("   ✓ Version 2 loaded and executed successfully")
print(f"   Cached handlers: {len(cfg_v2._cached_handlers)}")

print("\n4. Cleaning up Version 2...")
cfg_v2.cleanup_hook_imports()
print("   ✓ Version 2 cleaned up")

print("\n=== SUCCESS: Version switching works correctly! ===")
