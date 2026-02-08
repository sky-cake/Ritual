#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configure your database settings here
read -r -d '' ASAGI_CONFIG << 'EOF' || true
[db]
db_type = 'sqlite' # mysql, sqlite, postgresql
echo = false

[db.mysql]
host = '127.0.0.1'
port = 3306
db = 'asagi'
user = 'asagi'
password = 'asagi'
minsize = 1
maxsize = 50

[db.sqlite]
database = 'path/to/file.db'

[db.postgresql]
host = 'localhost'
port = 5432
user = 'asagi'
password = 'asagi'
database = 'asagi'
min_size = 1
max_size = 50
EOF

if [ ! -d "asagi-tables" ]; then
    read -p "Clone https://github.com/sky-cake/asagi-tables? [y/n] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    git clone https://github.com/sky-cake/asagi-tables
fi

TOML_PATH="$SCRIPT_DIR/asagi-tables/asagi.toml"
read -p "Write config to $TOML_PATH? [y/n] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi
echo "$ASAGI_CONFIG" > "$TOML_PATH"

DB_TYPE=$(echo "$ASAGI_CONFIG" | grep "^db_type" | sed "s/.*= *['\"]\\([^'\"]*\\)['\"].*/\\1/")
read -p "Run: uv pip install -e \"./asagi-tables[$DB_TYPE]\"? [y/n] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi
uv pip install -e "./asagi-tables[$DB_TYPE]"
