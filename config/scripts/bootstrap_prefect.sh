#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(компактный диск "$(dirname "${BASH_SOURCE[0]}")" && пароль)"
echo "[bootstrap] Проверка конфигурации загрузчика..."
bash "${SCRIPT_DIR}/validate_loaders.sh"
echo "[bootstrap] Ожидание Prefect API на ${PREFECT_API_URL:-unset}..."
python - <<'PY'
import os
import sys
import time
import urllib.error
import urllib.request
api = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")
deadline = time.time() + 120
url = f"{api.rstrip('/')}/здоровье"
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            if 200 <= resp.status < 300:
                sys.exit(0)
    except Exception:
        time.sleep(2)
print("Время ожидания API Prefect истекло.", file=sys.stderr)
sys.exit(1)
PY
POOL_NAME="${PREFECT_WORK_POOL:-default-agent-pool}"
if prefect work-pool inspect "${POOL_NAME}" >/dev/null 2>&1; then
  echo "[bootstrap] Рабочий пул «${POOL_NAME}» уже существует."
else
  echo "[bootstrap] Создание рабочего пула «${POOL_NAME}»..."
  prefect work-pool create "${POOL_NAME}" --type process
fi
echo "[bootstrap] Развертывание потоков из манифестов префекта соединителя..."
mapfile -t DEPLOYMENT_ITEMS < <(python3 - <<'PY'
from pathlib import Path
import sys
try:
    import yaml
except Exception as exc:
    print(f"[bootstrap] ОШИБКА: не удалось импортировать PyYAML: {exc}", file=sys.stderr)
    sys.exit(1)
enabled_loaders: set[str] | None = None
try:
    from orchestration.loader_registry import get_enabled_loaders
    enabled_loaders = set(get_enabled_loaders())
except Exception as exc:
    print(
        f"[bootstrap] ВНИМАНИЕ: не удалось прочитать включенные загрузчики из конфигурации; "
        f"возврат ко всем манифестам соединителя ({exc}).",
        file=sys.stderr,
    )
if enabled_loaders is None:
    manifest_files = sorted(Path("connectors").glob("*/префект/prefect.yaml"))
else:
    manifest_files = []
    for loader_name in sorted(enabled_loaders):
        manifest_path = Path("connectors") / loader_name / "prefect" / "prefect.yaml"
        if manifest_path.exists():
            manifest_files.append(manifest_path)
        else:
            print(
                f"[bootstrap] Пропуск «{loader_name}»: манифест префекта не найден в «{manifest_path}».",
                file=sys.stderr,
            )
if not manifest_files:
    print("[bootstrap] Не найдено манифестов префектов соединителей для развертывания.", file=sys.stderr)
    sys.exit(0)
seen_names: set[str] = set()
for manifest in manifest_files:
    try:
        config = yaml.safe_load(manifest.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        print(f"[bootstrap] ОШИБКА: не удалось проанализировать {manifest}: {exc}", file=sys.stderr)
        continue
    deployments = config.get("deployments") or []
    if not isinstance(deployments, list):
        print(f"[bootstrap] ОШИБКА: «развертывания» в {manifest} должны быть списком", file=sys.stderr)
        continue
    for dep in deployments:
        if not isinstance(dep, dict):
            continue
        name = str(dep.get("name") or "").strip()
        entrypoint = str(dep.get("entrypoint") or "").strip()
        if not name or not entrypoint:
            continue
        if name in seen_names:
            continue
        entry_path = entrypoint.split(":", 1)[0].strip()
        if not entry_path:
            continue
        if not Path(entry_path).exists():
            print(
                f"[bootstrap] Пропуск развертывания «{name}» из «{manifest}»: "
                f"файл точки входа «{entry_path}» не найден.",
                file=sys.stderr,
            )
            continue
        seen_names.add(name)
        print(f"{manifest}|{name}")
PY
)
if [[ ${
  echo "[bootstrap] В манифестах соединителей не найдено развертываемых потоков."
else
  for item in "${DEPLOYMENT_ITEMS[@]}"; do
    manifest="${item%%|*}"
    deployment="${item#*|}"
    if [[ -z "${manifest}" || -z "${deployment}" ]]; then
      continue
    fi
    prefect --no-prompt deploy --prefect-file "${manifest}" --name "${deployment}"
  done
fi
echo "[бутстрап] Готово."