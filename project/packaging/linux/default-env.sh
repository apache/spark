
# The contents of this file are appended to the config file template.

# Paths from the cloudera hadoop packages
LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/lib/hadoop/lib/native
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}

# Read by spark-slave.service
SPARK_MASTER="spark://localhost:7077"
