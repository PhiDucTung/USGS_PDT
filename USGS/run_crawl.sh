#!/usr/bin/env bash
# Full USGS workflow; API key stays only in the process environment.
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"
PYTHON="$PROJECT_DIR/.venv/bin/python"

usage() {
    cat <<'HELP'
Cách dùng:
  ./run_crawl.sh           Chạy tiếp crawl → kiểm định → export → preprocess
  ./run_crawl.sh status    Xem tiến độ, không khởi chạy crawler
  ./run_crawl.sh --help    Xem hướng dẫn

API key: dùng USGS_API_KEY có sẵn trong môi trường, hoặc nhập ẩn khi được hỏi.
Log: bronze/data/logs/crawl.log
Không chạy đồng thời với crawler trong Codex; script sẽ kiểm tra khóa trước.
HELP
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi
if [[ $# -gt 1 || ( "${1:-run}" != "run" && "${1:-run}" != "status" ) ]]; then
    usage >&2
    exit 2
fi
if [[ ! -x "$PYTHON" ]]; then
    echo "Chưa có môi trường Python. Cài trước bằng:" >&2
    echo "  cd \"$PROJECT_DIR\"" >&2
    echo "  python3 -m venv .venv" >&2
    echo "  .venv/bin/pip install -e '.[test]'" >&2
    exit 1
fi

if [[ "${1:-run}" == "status" ]]; then
    exec "$PYTHON" - <<'PY'
import sqlite3
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
p = Path('bronze/data/checkpoint.sqlite').resolve()
if not p.exists():
    raise SystemExit('Chưa có checkpoint.')
with sqlite3.connect(p.as_uri() + '?mode=ro', uri=True) as db:
    counts = dict(db.execute('SELECT status, COUNT(*) FROM jobs GROUP BY status'))
    total = sum(counts.values())
    done = counts.get('complete', 0) + counts.get('empty', 0)
    sites = db.execute("SELECT COUNT(*) FROM (SELECT site FROM jobs GROUP BY site HAVING SUM(status NOT IN ('complete','empty'))=0)").fetchone()[0]
    rows = db.execute('SELECT COALESCE(SUM(rows),0) FROM jobs').fetchone()[0]
    updated = db.execute('SELECT MAX(updated) FROM jobs').fetchone()[0]
print(f'Job đã lập lịch: {done:,}/{total:,} hoàn tất ({done / total:.2%})' if total else 'Chưa lập lịch job.')
print(f'Trạm đã hoàn tất các job hiện có: {sites:,}; quan sát: {rows:,}')
for status in ('pending', 'running', 'complete', 'empty', 'error'):
    print(f'  {status}: {counts.get(status,0):,}')
if updated:
    print('Cập nhật gần nhất:', datetime.fromisoformat(updated).astimezone(ZoneInfo('Asia/Ho_Chi_Minh')).isoformat())
print('Trạng thái running trong checkpoint có thể còn sau khi bị ngắt; không phải xác nhận tiến trình đang sống.')
PY
fi

# Probe the same OS lock used by the CLI before asking for credentials.
# The CLI acquires and retains the real lock for the entire workflow.
"$PYTHON" - <<'PY'
import fcntl
from pathlib import Path
root = Path('bronze/data')
if not (root / 'catalog.json').is_file():
    raise SystemExit('Thiếu bronze/data/catalog.json. Cần danh sách trạm trước khi chạy toàn bộ.')
with (root / '.pipeline.lock').open('a') as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit('Đã có pipeline đang chạy. Không mở thêm bản thứ hai. Dùng ./run_crawl.sh status để xem tiến độ.')
PY

if [[ -z "${USGS_API_KEY:-}" ]]; then
    if [[ ! -t 0 ]]; then
        echo 'Không có USGS_API_KEY. Chạy tương tác trong Terminal để nhập key ẩn.' >&2
        exit 1
    fi
    read -r -s -p 'Nhập USGS API key (ẩn, không lưu ra file): ' USGS_API_KEY
    printf '\n' >&2
    if [[ -z "${USGS_API_KEY//[[:space:]]/}" ]]; then
        echo 'API key không được để trống.' >&2
        exit 1
    fi
fi
export USGS_API_KEY

mkdir -p bronze/data/logs
# Bash process substitution keeps Python's exit code, unlike a plain tee pipe.
exec > >(tee -a "$PROJECT_DIR/bronze/data/logs/crawl.log") 2>&1
printf '\n=== Bắt đầu workflow: %s ===\n' "$(date '+%Y-%m-%d %H:%M:%S %z')"
echo 'Tiếp tục từ checkpoint; giữ thời gian chờ USGS nếu còn hiệu lực.'
exec "$PYTHON" -u -m usgs_pipeline run --config "$PROJECT_DIR/config.example.toml"
