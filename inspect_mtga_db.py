"""
Utility script to inspect MTGA SQLite database files (.mtga) and summarize their structure.
"""
import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fetch_table_columns(connection: sqlite3.Connection, table: str) -> List[Dict[str, object]]:
    cursor = connection.execute(f"PRAGMA table_info({quote_identifier(table)})")
    columns = []
    for row in cursor.fetchall():
        columns.append(
            {
                "cid": row[0],
                "name": row[1],
                "type": row[2],
                "not_null": bool(row[3]),
                "default_value": row[4],
                "primary_key_position": row[5],
            }
        )
    return columns


def fetch_indexes(connection: sqlite3.Connection, table: str) -> List[Dict[str, object]]:
    indexes = []
    cursor = connection.execute(f"PRAGMA index_list({quote_identifier(table)})")
    for index_row in cursor.fetchall():
        index_name = index_row[1]
        index_info = connection.execute(f"PRAGMA index_info({quote_identifier(index_name)})").fetchall()
        indexes.append(
            {
                "name": index_name,
                "unique": bool(index_row[2]),
                "origin": index_row[3],
                "partial": bool(index_row[4]),
                "columns": [info[2] for info in index_info],
            }
        )
    return indexes


def fetch_foreign_keys(connection: sqlite3.Connection, table: str) -> List[Dict[str, object]]:
    cursor = connection.execute(f"PRAGMA foreign_key_list({quote_identifier(table)})")
    return [
        {
            "id": row[0],
            "seq": row[1],
            "table": row[2],
            "from_column": row[3],
            "to_column": row[4],
            "on_update": row[5],
            "on_delete": row[6],
            "match": row[7],
        }
        for row in cursor.fetchall()
    ]


def fetch_row_count(connection: sqlite3.Connection, table: str) -> Optional[int]:
    try:
        cursor = connection.execute(f"SELECT COUNT(*) FROM {quote_identifier(table)}")
        return cursor.fetchone()[0]
    except sqlite3.DatabaseError:
        return None


def inspect_table(connection: sqlite3.Connection, table: str, include_row_count: bool) -> Dict[str, object]:
    row_count = fetch_row_count(connection, table) if include_row_count else None
    return {
        "name": table,
        "sql": connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchone()[0],
        "columns": fetch_table_columns(connection, table),
        "indexes": fetch_indexes(connection, table),
        "foreign_keys": fetch_foreign_keys(connection, table),
        "row_count": row_count,
    }


def inspect_view(connection: sqlite3.Connection, view: str) -> Dict[str, object]:
    definition = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = ?", (view,)
    ).fetchone()[0]
    return {"name": view, "sql": definition}


def inspect_schema(connection: sqlite3.Connection) -> Dict[str, object]:
    pragmas = {
        "page_size": "PRAGMA page_size",
        "user_version": "PRAGMA user_version",
        "application_id": "PRAGMA application_id",
        "auto_vacuum": "PRAGMA auto_vacuum",
        "encoding": "PRAGMA encoding",
    }
    schema_info: Dict[str, object] = {}
    for key, pragma in pragmas.items():
        schema_info[key] = connection.execute(pragma).fetchone()[0]
    return schema_info


def inspect_database(db_path: Path, include_row_count: bool) -> Dict[str, object]:
    file_stats = db_path.stat()

    with sqlite3.connect(db_path) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        views = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name"
        ).fetchall()

        table_details = [inspect_table(connection, row[0], include_row_count) for row in tables]
        view_details = [inspect_view(connection, row[0]) for row in views]

        return {
            "file": str(db_path),
            "size_bytes": file_stats.st_size,
            "modified": file_stats.st_mtime,
            "modified_iso": datetime.fromtimestamp(file_stats.st_mtime, tz=timezone.utc).isoformat(),
            "schema": inspect_schema(connection),
            "tables": table_details,
            "views": view_details,
        }


def discover_databases(targets: Iterable[str], recursive: bool) -> List[Path]:
    discovered: List[Path] = []
    for target in targets:
        path = Path(target).expanduser()
        if path.is_file() and path.suffix.lower() == ".mtga":
            discovered.append(path)
        elif path.is_dir():
            pattern = "**/*.mtga" if recursive else "*.mtga"
            discovered.extend(sorted(path.glob(pattern)))
        else:
            discovered.extend(sorted(Path().glob(target)))

    unique_paths = []
    seen = set()
    for path in discovered:
        if path.suffix.lower() != ".mtga":
            continue
        if path.resolve() in seen:
            continue
        seen.add(path.resolve())
        unique_paths.append(path)
    return sorted(unique_paths)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect MTGA SQLite databases (.mtga) and output a structured summary of their tables, indexes, and schema metadata."
        )
    )
    parser.add_argument(
        "targets",
        nargs="+",
        help="Paths or glob patterns pointing to .mtga files or directories containing them.",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Search directories recursively for .mtga files.",
    )
    parser.add_argument(
        "--include-row-count",
        action="store_true",
        help="Include a row count for each table (can be slow on large databases).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Optional path to write the JSON summary. Prints to stdout when omitted.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="Indent level for JSON output (default: 2).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_paths = discover_databases(args.targets, args.recursive)

    if not db_paths:
        raise SystemExit("No .mtga database files found for the given targets.")

    summary = [inspect_database(path, args.include_row_count) for path in db_paths]
    output_text = json.dumps(summary, indent=args.indent)

    if args.output:
        args.output.write_text(output_text, encoding="utf-8")
    else:
        print(output_text)


if __name__ == "__main__":
    main()
