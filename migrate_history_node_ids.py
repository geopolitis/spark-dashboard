#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Atomically rename dashboard node IDs in JSONL history.")
    parser.add_argument("history", type=Path)
    parser.add_argument("--map", action="append", required=True, metavar="OLD=NEW")
    args = parser.parse_args()

    mapping = dict(item.split("=", 1) for item in args.map)
    temporary = args.history.with_suffix(args.history.suffix + ".migrating")
    rows = changed = invalid = 0

    with args.history.open("r", encoding="utf-8") as source, temporary.open("w", encoding="utf-8") as target:
        for line in source:
            rows += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                invalid += 1
                target.write(line)
                continue
            nodes = row.get("nodes")
            if isinstance(nodes, dict):
                renamed = {}
                for node_id, sample in nodes.items():
                    new_id = mapping.get(node_id, node_id)
                    if new_id != node_id:
                        changed += 1
                    # Prefer an already-canonical sample if a row contains both IDs.
                    if new_id not in renamed or node_id == new_id:
                        renamed[new_id] = sample
                row["nodes"] = renamed
            target.write(json.dumps(row, separators=(",", ":")) + "\n")
        target.flush()
        os.fsync(target.fileno())

    os.replace(temporary, args.history)
    print(json.dumps({"rows": rows, "renamed_node_entries": changed, "invalid_rows_preserved": invalid}))


if __name__ == "__main__":
    main()
