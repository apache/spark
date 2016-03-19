#!/usr/bin/env bash
#!/usr/bin/env bash
set -o verbose

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DATA_DIR="${DIR}/data"
DATA_FILE="${DATA_DIR}/baby_names.csv"
DATABASE=airflow_ci

mysqladmin -u root create ${DATABASE}
mysql -u root < ${DATA_DIR}/mysql_schema.sql
mysqlimport -u root --fields-optionally-enclosed-by="\"" --fields-terminated-by=, --ignore-lines=1 ${DATABASE} ${DATA_FILE}

