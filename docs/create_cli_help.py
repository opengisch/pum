#!/usr/bin/env python3
import argparse
import io
import contextlib

from pum.cli import create_parser
from pathlib import Path


def parser_to_markdown(parser: argparse.ArgumentParser) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        parser.print_help()
    help_text = buf.getvalue()

    # Simple Markdown formatting
    lines = help_text.strip().splitlines()
    md_lines = []
    for line in lines:
        if line.strip().endswith(":"):
            md_lines.append(f"### {line.strip()}")
        elif line.startswith("  "):  # typically an argument line
            parts = line.strip().split("  ", 1)
            if len(parts) == 2:
                md_lines.append(f"- `{parts[0]}`: {parts[1].strip()}")
            else:
                md_lines.append(f"- `{line.strip()}`")
        else:
            md_lines.append(line.strip())
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
