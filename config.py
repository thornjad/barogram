import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    input_db: str
    output_db: str


def load(path: str | Path) -> Config:
    path = Path(path)
    if not path.exists():
        sys.exit(
            f"config file not found: {path}\n"
            f"create it with:\n\n"
            f"[barogram]\n"
            f'input_db = "/path/to/wxlog-read-only.db"\n'
            f'output_db = "/path/to/barogram.db"\n'
        )
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        sys.exit(f"config parse error in {path}: {e}")

    section = data.get("barogram", {})
    input_db = section.get("input_db")
    output_db = section.get("output_db")
    missing = [k for k, v in [("input_db", input_db), ("output_db", output_db)] if not v]
    if missing:
        sys.exit(f"config missing required keys in [barogram]: {', '.join(missing)}")

    return Config(input_db=input_db, output_db=output_db)
