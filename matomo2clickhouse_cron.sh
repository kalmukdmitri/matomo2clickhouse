#!/bin/bash
cd /opt/dix/matomo2clickhouse/
PATH="/opt/dix/matomo2clickhouse/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
VIRTUAL_ENV="/opt/dix/matomo2clickhouse/.venv"

pipenv run python3 matomo2clickhouse.py
