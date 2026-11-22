#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(компактный диск "$(dirname "${BASH_SOURCE[0]}")" && пароль)"
PROJECT_ROOT="$(компактный диск "${SCRIPT_DIR}/../.." && пароль)"
VALIDATE_LOADERS_QUIET="${VALIDATE_LOADERS_QUIET:-0}"
if [[ "${VALIDATE_LOADERS_QUIET}" != "1" ]]; then
  echo "════════════════════════════════════════════════════════════"
  echo "🔍 Проверка конфигурации загрузчика"
  echo "════════════════════════════════════════════════════════════"
fi
VALIDATE_PROJECT_ROOT="${PROJECT_ROOT}" VALIDATE_LOADERS_QUIET="${VALIDATE_LOADERS_QUIET}" python3 - <<'PYVALIDATE'
import sys
import os
from pathlib import Path
project_root = os.environ.get("VALIDATE_PROJECT_ROOT", os.getcwd())
quiet_mode = os.environ.get("VALIDATE_LOADERS_QUIET", "0") == "1"
sys.path.insert(0, project_root)
try:
    from orchestration.loader_registry import (
        is_loader_enabled,
        get_loader_config,
        _load_config,
    )
    config = _load_config()
    all_loaders = sorted(config.get("loaders", {}).keys())
    enabled_loaders = [n for n in all_loaders if is_loader_enabled(n)]
    if quiet_mode:
        sys.exit(0)
    print("\n✓ Конфигурация загрузчика успешно загружена")
    print(f"\n📦 Включенные загрузчики ({len(enabled_loaders)}):")
    for loader_name in all_loaders:
        enabled = is_loader_enabled(loader_name)
        cfg = get_loader_config(loader_name)
        display_name = cfg.get("display_name", loader_name)
        if enabled:
            print(f"   ✓ {loader_name:20s} – {display_name}")
        else:
            print(f"   ✗ {loader_name:20s} — ОТКЛЮЧЕНО")
    print(f"\n💡 Файл конфигурации: {os.getenv('LOADER_CONFIG_PATH', 'config/loaders.yaml')}")
    sys.exit(0)
except Exception as e:
    print(f"\n⚠️ Внимание: не удалось проверить конфигурацию загрузчика: {e}.")
    print("   В любом случае продолжаем запуск...")
    sys.exit(0)
PYVALIDATE
if [[ "${VALIDATE_LOADERS_QUIET}" != "1" ]]; then
  echo "════════════════════════════════════════════════════════════"
  echo ""
fi