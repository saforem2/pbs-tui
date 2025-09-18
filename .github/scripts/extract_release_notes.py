from __future__ import annotations

import argparse
from pathlib import Path

from keepachangelog import to_dict


KNOWN_SECTIONS = (
    "added",
    "changed",
    "deprecated",
    "removed",
    "fixed",
    "security",
)


def build_release_notes(version: str, changelog_path: Path) -> list[str]:
    entries = to_dict(str(changelog_path))
    entry = entries.get(version)
    if entry is None:
        raise SystemExit(f"No release notes found for version {version} in {changelog_path}")

    lines: list[str] = []
    metadata = entry.get("metadata", {})
    header = version
    release_date = metadata.get("release_date")
    if release_date:
        header = f"{header} - {release_date}"
    lines.append(f"## {header}")

    def render_section(section_name: str) -> None:
        items = entry.get(section_name)
        if not items:
            return
        title = section_name.replace("_", " ").title()
        lines.append("")
        lines.append(f"### {title}")
        for item in items:
            lines.append(f"- {item}")

    for section in KNOWN_SECTIONS:
        render_section(section)

    for section in sorted(key for key in entry.keys() if key not in {"metadata", *KNOWN_SECTIONS}):
        render_section(section)

    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, default=Path(".github/current_release_notes.md"))
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    args = parser.parse_args()

    lines = build_release_notes(version=args.version, changelog_path=args.changelog)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
