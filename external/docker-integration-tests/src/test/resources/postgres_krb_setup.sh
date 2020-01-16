#!/usr/bin/env bash

sed -i 's/host all all all md5/host all all all gss/g' /var/lib/postgresql/data/pg_hba.conf
echo "krb_server_keyfile='/docker-entrypoint-initdb.d/postgres.keytab'" >> /var/lib/postgresql/data/postgresql.conf
psql -U postgres -c "CREATE ROLE \"postgres/__IP_ADDRESS_REPLACE_ME__@EXAMPLE.COM\" LOGIN SUPERUSER"
