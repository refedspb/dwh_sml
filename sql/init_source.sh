#!/usr/bin/env bash
# Инициализация источника (схема public): schema.sql + synthetic.sql в ОДНОЙ транзакции.
# Идемпотентно: если таблица prices уже есть, ничего не делаем; при сбое откатывается всё.
set -euo pipefail

exists=$(psql -tA -c "SELECT to_regclass('public.prices') IS NOT NULL")
if [ "$exists" = "t" ]; then
    echo "[source-init] источник уже инициализирован, пропускаю"
    exit 0
fi

echo "[source-init] генерирую источник: n_prices=${N_PRICES:-300000}"
psql -v ON_ERROR_STOP=1 --single-transaction -v n_prices="${N_PRICES:-300000}" \
     -f /sql/schema.sql -f /sql/synthetic.sql
echo "[source-init] готово"
