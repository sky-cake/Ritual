#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configure your database settings here
read -r -d '' ASAGI_CONFIG << 'EOF' || true
[db]
db_type = 'sqlite' # 'sqlite', 'mysql', 'postgresql'
echo = false

[db.sqlite]
database = 'path/to/file.db'

[db.mysql]
host = '127.0.0.1'
port = 3306
db = 'asagi'
user = 'asagi'
password = 'asagi'
minsize = 1
maxsize = 50

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
    read -p "Fetch https://github.com/sky-cake/asagi-tables? [y/n] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi

    curl -L --fail --silent --show-error https://github.com/sky-cake/asagi-tables/archive/refs/heads/master.zip  -o /tmp/asagi-tables.zip

    mkdir -p asagi-tables
    unzip -q /tmp/asagi-tables.zip -d /tmp/asagi-tables
    cp -a /tmp/asagi-tables/asagi-tables-master/. asagi-tables/
    rm -rf /tmp/asagi-tables /tmp/asagi-tables.zip
fi

TOML_PATH="$SCRIPT_DIR/asagi-tables/asagi.toml"
read -p "Write config to $TOML_PATH? [y/n] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi
echo "$ASAGI_CONFIG" > "$TOML_PATH"

DB_TYPE=$(echo "$ASAGI_CONFIG" | grep "^db_type" | sed "s/.*= *['\"]\([^'\"]*\)['\"].*/\1/")

read -p "Install with uv (1) or pip (2)? " -r INSTALLER
case "$INSTALLER" in
    uv|UV|1)
        INSTALL_CMD="uv pip install"
        ;;
    pip|PIP|2)
        INSTALL_CMD="python3 -m pip install --break-system-packages"
        ;;
    *)
        echo "Invalid choice. Please enter 'uv', '1', 'pip', or '2'."
        exit 1
        ;;
esac

read -p "Run: $INSTALL_CMD -e \"./asagi-tables[$DB_TYPE]\"? [y/n] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

$INSTALL_CMD -e "./asagi-tables[$DB_TYPE]"
