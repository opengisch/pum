import psycopg

from pum import HookBase


class Hook(HookBase):
    def run_hook(self, connection: psycopg.Connection, lang_code: str = "en") -> None:
        """Run hook with an optional argument that has a default value."""
        pass
