#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(компактный диск "$(dirname "${BASH_SOURCE[0]}")" && пароль)"
echo "[бот] Проверка конфигурации загрузчика..."
VALIDATE_LOADERS_QUIET=1 bash "${SCRIPT_DIR}/validate_loaders.sh"
echo "[bot] Запуск бота-администратора..."
exec python admin_bot/app.py