from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

import tomlkit


def write_outputs(name: str, version: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    path = Path(output_path)
    with path.open("a", encoding="utf-8") as file:
        file.write(f"name={name}\n")
        file.write(f"version={version}\n")


def check_pypi(name: str, version: str, token: str | None) -> None:
    if not token:
        return

    url = f"https://pypi.org/pypi/{name}/json"
    try:
        with urllib.request.urlopen(url) as response:  # noqa: S310 - HTTPS endpoint
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return
        raise
    except urllib.error.URLError:
        # Network errors should not block the release entirely.
        return

    if version in payload.get("releases", {}):
        message = f"Version {version} of {name} already exists on PyPI."
        print(message)
        sys.exit(1)


def main() -> None:
    pyproject_path = Path("pyproject.toml")
    data = tomlkit.parse(pyproject_path.read_text(encoding="utf-8"))

    project = data["project"]
    name = project["name"]
    version = project["version"]

    write_outputs(name=name, version=version)
    check_pypi(name=name, version=version, token=os.environ.get("PYPI_TOKEN"))


if __name__ == "__main__":
    main()
