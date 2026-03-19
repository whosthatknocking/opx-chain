import json
import os
import re
from functools import lru_cache
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
STATIC_ROOT = Path(__file__).resolve().parent / "viewer_static"
README_PATH = REPO_ROOT / "README.md"
CSV_PATTERN = "options_engine_output_*.csv"
HIDDEN_COLUMNS = {
    "currency",
    "underlying_currency",
    "roll_from_days_to_expiration",
    "roll_from_expiration_date",
    "roll_days_added",
    "roll_from_premium_reference_price",
    "roll_net_credit",
    "roll_yield",
    "fetch_status",
    "fetch_error",
    "script_version",
    "fetched_at",
}


def discover_csv_files():
    return sorted(REPO_ROOT.glob(CSV_PATTERN), key=lambda path: path.stat().st_mtime, reverse=True)


def resolve_csv_path(csv_name=None):
    files = discover_csv_files()
    if not files:
        raise FileNotFoundError("No CSV files were found in the project root.")

    if not csv_name:
        return files[0]

    candidate = REPO_ROOT / csv_name
    if candidate.exists() and candidate.is_file() and candidate.name.startswith("options_engine_output_"):
        return candidate

    raise FileNotFoundError(f"CSV file not found: {csv_name}")


@lru_cache(maxsize=1)
def load_readme_text():
    return README_PATH.read_text(encoding="utf-8")


@lru_cache(maxsize=1)
def extract_field_descriptions():
    descriptions = {}
    pattern = re.compile(r"^- `([^`]+)`: (.+)$")
    for line in load_readme_text().splitlines():
        match = pattern.match(line.strip())
        if match:
            descriptions[match.group(1)] = match.group(2)
    return descriptions


def normalize_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value.item() if hasattr(value, "item") else value


def load_csv_payload(csv_name=None):
    csv_path = resolve_csv_path(csv_name)
    frame = pd.read_csv(csv_path)
    visible_columns = [column for column in frame.columns if column not in HIDDEN_COLUMNS]
    frame = frame[visible_columns]
    rows = [
        {column: normalize_value(value) for column, value in record.items()}
        for record in frame.to_dict(orient="records")
    ]
    descriptions = extract_field_descriptions()
    columns = [
        {
            "name": column,
            "description": descriptions.get(column, "No README description available for this field."),
        }
        for column in frame.columns
    ]
    return {
        "selected_file": csv_path.name,
        "row_count": len(rows),
        "columns": columns,
        "rows": rows,
    }


def make_file_listing():
    files = discover_csv_files()
    return [
        {
            "name": path.name,
            "size_bytes": path.stat().st_size,
            "modified_at": path.stat().st_mtime,
        }
        for path in files
    ]


class ViewerRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_ROOT), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/files":
            return self.respond_json({"files": make_file_listing()})
        if parsed.path == "/api/data":
            query = parse_qs(parsed.query)
            csv_name = query.get("file", [None])[0]
            try:
                payload = load_csv_payload(csv_name)
            except FileNotFoundError as exc:
                return self.respond_json({"error": str(exc)}, status=HTTPStatus.NOT_FOUND)
            return self.respond_json(payload)
        if parsed.path == "/api/readme":
            return self.respond_json({"markdown": load_readme_text()})
        if parsed.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def respond_json(self, payload, status=HTTPStatus.OK):
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):
        if os.environ.get("OPTIONS_FETCHER_VIEWER_QUIET") == "1":
            return
        super().log_message(format, *args)


def serve(host="127.0.0.1", port=8000):
    server = ThreadingHTTPServer((host, port), ViewerRequestHandler)
    print(f"CSV viewer running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main():
    host = os.environ.get("OPTIONS_FETCHER_VIEWER_HOST", "127.0.0.1")
    port = int(os.environ.get("OPTIONS_FETCHER_VIEWER_PORT", "8000"))
    serve(host=host, port=port)


if __name__ == "__main__":
    main()
