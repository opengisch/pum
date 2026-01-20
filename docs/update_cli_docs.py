#!/usr/bin/env python3
"""Generate Markdown documentation from the CLI argparse help output.

This script renders the `pum` CLI help (and each subcommand help) into Markdown.
It also normalizes wrapped help text so option/parameter descriptions are
single-line in the generated docs.
"""

import argparse
import io
import contextlib
import re

from pum.cli import create_parser
from pathlib import Path


def parser_to_markdown(parser: argparse.ArgumentParser) -> str:
    """Convert an argparse parser's help text to Markdown.

    Args:
        parser: The argparse parser to render.

    Returns:
        The generated Markdown string.

    """
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        parser.print_help()
    help_text = buf.getvalue()

    # Simple Markdown formatting
    lines = help_text.strip().splitlines()
    md_lines = []
    entry_re = re.compile(r"^\s{2,}(?P<key>\S.*?)\s{2,}(?P<desc>.+)$")

    def norm(text: str) -> str:
        # Collapse any wrapping/newlines/tabs into single spaces.
        return " ".join(text.split())

    last_entry_index: int | None = None
    for line in lines:
        stripped = line.rstrip("\n")
        if not stripped:
            continue

        indent = len(stripped) - len(stripped.lstrip(" "))

        # Section headers from argparse typically end with ':' and are not indented.
        if stripped.strip().endswith(":") and not stripped.startswith(" "):
            md_lines.append(f"### {stripped.strip()}")
            last_entry_index = None
            continue

        match = entry_re.match(stripped)
        if match:
            key = match.group("key").strip()
            desc = norm(match.group("desc"))
            md_lines.append(f"- `{key}`: {desc}")
            last_entry_index = len(md_lines) - 1
            continue

        # Some argparse formats print the option/arg key on its own line, with the
        # description starting on the next (more-indented) line.
        if indent >= 2 and indent <= 4:
            key_only = stripped.strip()
            if key_only.startswith("-") and not key_only.endswith(":"):
                md_lines.append(f"- `{key_only}`:")
                last_entry_index = len(md_lines) - 1
                continue

        # Wrapped description lines are indented, but don't start a new entry.
        if stripped.startswith(" ") and last_entry_index is not None:
            addition = norm(stripped)
            if md_lines[last_entry_index].endswith(":"):
                md_lines[last_entry_index] = f"{md_lines[last_entry_index]} {addition}"
            else:
                md_lines[last_entry_index] = f"{md_lines[last_entry_index]} {addition}"
            continue

        # Plain text paragraph.
        md_lines.append(stripped.strip())
        last_entry_index = None
    return "\n".join(md_lines)


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    parser = create_parser()
    markdown_help = parser_to_markdown(parser)
    with open(current_dir / "docs/cli.md", "w") as f:
        f.write(markdown_help)

    subparsers_action = None
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            subparsers_action = action
            break
    if not subparsers_action:
        raise ValueError("No subparsers found in the parser.")
    for cmd, subparser in subparsers_action.choices.items():
        if cmd == "help":
            continue
        markdown_help = parser_to_markdown(subparser)
        with open(current_dir / "docs/cli" / f"{cmd}.md", "w") as f:
            f.write(markdown_help)
