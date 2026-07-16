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

    # Extract the leading usage block (until blank line or section header) and
    # render it as a fenced code block so its square brackets don't confuse
    # Markdown parsers as reference-style links.
    usage_lines: list[str] = []
    body_start = 0
    if lines and lines[0].lstrip().lower().startswith("usage:"):
        for i, line in enumerate(lines):
            if not line.strip():
                body_start = i + 1
                break
            stripped_line = line.strip()
            if i > 0 and stripped_line.endswith(":") and not line.startswith(" "):
                body_start = i
                break
            usage_lines.append(stripped_line)
        else:
            body_start = len(lines)
    if usage_lines:
        md_lines.append("```text")
        md_lines.extend(usage_lines)
        md_lines.append("```")
        md_lines.append("")
    lines = lines[body_start:]

    # Pattern for the bare argparse subcommand metavar line, e.g.
    # "{info,install,upgrade,...}". We drop it because the same commands are
    # immediately re-listed underneath as proper bullet entries.
    metavar_re = re.compile(r"^\s*\{[\w,\-]+\}\s*$")

    last_entry_index: int | None = None
    in_list = False

    def end_list_block() -> None:
        nonlocal in_list, last_entry_index
        if in_list:
            md_lines.append("")
            in_list = False
        last_entry_index = None

    for line in lines:
        stripped = line.rstrip("\n")
        if not stripped:
            continue

        indent = len(stripped) - len(stripped.lstrip(" "))

        # Section headers from argparse typically end with ':' and are not indented.
        if stripped.strip().endswith(":") and not stripped.startswith(" "):
            end_list_block()
            if md_lines and md_lines[-1] != "":
                md_lines.append("")
            md_lines.append(f"### {stripped.strip()}")
            md_lines.append("")
            continue

        # Skip the bare subcommand metavar line under "commands:".
        if metavar_re.match(stripped):
            continue

        match = entry_re.match(stripped)
        if match:
            key = match.group("key").strip()
            desc = norm(match.group("desc"))
            md_lines.append(f"- `{key}`: {desc}")
            last_entry_index = len(md_lines) - 1
            in_list = True
            continue

        # Some argparse formats print the option/arg key on its own line, with the
        # description starting on the next (more-indented) line.
        if indent >= 2 and indent <= 4:
            key_only = stripped.strip()
            if key_only.startswith("-") and not key_only.endswith(":"):
                md_lines.append(f"- `{key_only}`:")
                last_entry_index = len(md_lines) - 1
                in_list = True
                continue

        # Wrapped description lines are indented, but don't start a new entry.
        if stripped.startswith(" ") and last_entry_index is not None:
            addition = norm(stripped)
            md_lines[last_entry_index] = f"{md_lines[last_entry_index]} {addition}"
            continue

        # Plain text paragraph (e.g. "valid pum commands" below a section header).
        end_list_block()
        md_lines.append(stripped.strip())
        md_lines.append("")

    # Trim trailing blank lines.
    while md_lines and md_lines[-1] == "":
        md_lines.pop()
    return "\n".join(md_lines)


if __name__ == "__main__":
    current_dir = Path(__file__).parent
    parser = create_parser(width=200, max_help_position=40)
    markdown_help = parser_to_markdown(parser)
    with open(current_dir / "docs/cli.md", "w") as f:
        f.write(markdown_help + "\n")

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
            f.write(markdown_help + "\n")
