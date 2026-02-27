#!/bin/sh
set -e

echo "=== Startup diagnostics ==="
echo "DATABASE_PATH=$DATABASE_PATH"
echo "PORT=${PORT:-8000}"

# Check if /data exists (volume mount point)
if [ -d /data ]; then
    echo "/data exists (volume mounted)"
    ls -la /data/ 2>/dev/null || true
    df -h /data 2>/dev/null || true
else
    echo "WARNING: /data does not exist — creating directory"
    mkdir -p /data
fi

# Check if DB file already exists (persisted from previous deploy)
if [ -f "$DATABASE_PATH" ]; then
    DB_SIZE=$(stat -c%s "$DATABASE_PATH" 2>/dev/null || stat -f%z "$DATABASE_PATH" 2>/dev/null || echo "unknown")
    echo "Existing database found: $DATABASE_PATH ($DB_SIZE bytes)"
else
    echo "No existing database — will create fresh"
fi

# Idempotent init: creates tables IF NOT EXISTS, loads CPT codes, seeds payers
python -m db.init_db
python -m etl.load_cpt
python -m etl.seed_payers

# Post-init stats
if [ -f "$DATABASE_PATH" ]; then
    DB_SIZE=$(stat -c%s "$DATABASE_PATH" 2>/dev/null || stat -f%z "$DATABASE_PATH" 2>/dev/null || echo "unknown")
    echo "Database after init: $DB_SIZE bytes"
    python -c "
import sqlite3, os
db = sqlite3.connect(os.environ['DATABASE_PATH'])
providers = db.execute('SELECT COUNT(*) FROM providers').fetchone()[0]
rates = db.execute('SELECT COUNT(*) FROM normalized_rates').fetchone()[0]
payers = db.execute('SELECT COUNT(*) FROM payers').fetchone()[0]
print(f'  providers={providers}, rates={rates}, payers={payers}')
db.close()
"
fi

echo "=== Starting server ==="
exec uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}
