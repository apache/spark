#!/usr/bin/env bash

set -e

# {{{ PostgreSQL

postgres-remote-access-allow() {
  log-operation "$FUNCNAME" "$@"
  $SUDO sed -e "s/[#]\?listen_addresses = .*/listen_addresses = '*'/g" -i '/etc/postgresql/'*'/main/postgresql.conf'
  $SUDO grep '192.168.1.1' '/etc/postgresql/'*'/main/pg_hba.conf' | \
    ( echo 'host all all 192.168.1.1/22 md5' | $SUDO tee -a '/etc/postgresql/'*'/main/pg_hba.conf' >/dev/null )
}

postgres-password-reset() {
  log-operation "$FUNCNAME"
  $SUDO -u 'postgres' psql -c "ALTER ROLE postgres WITH ENCRYPTED PASSWORD '$1'" 1>/dev/null
}

postgres-autovacuum-on() {
  log-operation "$FUNCNAME"
  $SUDO sed -e "s/[#]\?autovacuum = .*/autovacuum = on/g" -i '/etc/postgresql/'*'/main/postgresql.conf'
  $SUDO sed -e "s/[#]\?track_counts = .*/track_counts = on/g" -i '/etc/postgresql/'*'/main/postgresql.conf'
}

postgres-template-encoding() {
  local encoding
  local ctype
  local collate
  encoding="${1:-UTF8}"
  ctype="${2:-en_GB.$( echo "$encoding" | tr 'A-Z' 'a-z' )}"
  collate="${3:-$ctype}"
  log-operation "$FUNCNAME" "$@"
  SQL_TMP_FILE="$( mktemp -t 'sql-XXXXXXXX' )"
  cat > "$SQL_TMP_FILE" <<-EOD
UPDATE pg_database SET datallowconn = TRUE WHERE datname = 'template0';
\\c template0;
UPDATE pg_database SET datistemplate = FALSE WHERE datname = 'template1';
DROP DATABASE template1;
CREATE DATABASE template1 WITH template = template0 ENCODING = '${encoding}' LC_CTYPE = '${ctype}' LC_COLLATE = '${collate}';
UPDATE pg_database SET datistemplate = TRUE WHERE datname = 'template1';
\\c template1;
UPDATE pg_database SET datallowconn = FALSE WHERE datname = 'template0';
EOD
  $SUDO chmod 0777 "$SQL_TMP_FILE"
  $SUDO -u 'postgres' psql -f "$SQL_TMP_FILE" 1>/dev/null || {
    EXIT_CODE=$?
    rm "$SQL_TMP_FILE"
    exit $EXIT_CODE
  }
  rm "$SQL_TMP_FILE"
}

# }}}
