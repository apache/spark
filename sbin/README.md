# Spark Admin Scripts

This directory contains administrative scripts for managing Spark standalone clusters.

## Overview

The `sbin/` scripts are used by cluster administrators to:
- Start and stop Spark standalone clusters
- Start and stop individual daemons (master, workers, history server)
- Manage cluster lifecycle
- Configure cluster nodes

**Note**: These scripts are for **Spark Standalone** cluster mode only. For YARN, Kubernetes, or Mesos, use their respective cluster management tools.

## Cluster Management Scripts

### start-all.sh / stop-all.sh

Start or stop all Spark daemons on the cluster.

**Usage:**
```bash
# Start master and all workers
./sbin/start-all.sh

# Stop all daemons
./sbin/stop-all.sh
```

**What they do:**
- `start-all.sh`: Starts master on the current machine and workers on machines listed in `conf/workers`
- `stop-all.sh`: Stops all master and worker daemons

**Prerequisites:**
- SSH key-based authentication configured
- `conf/workers` file with worker hostnames
- Spark installed at same location on all machines

**Configuration files:**
- `conf/workers`: List of worker hostnames (one per line)
- `conf/spark-env.sh`: Environment variables

### start-master.sh / stop-master.sh

Start or stop the Spark master daemon on the current machine.

**Usage:**
```bash
# Start master
./sbin/start-master.sh

# Stop master
./sbin/stop-master.sh
```

**Master Web UI**: Access at `http://<master-host>:8080/`

**Configuration:**
```bash
# In conf/spark-env.sh
export SPARK_MASTER_HOST=master-hostname
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
```

### start-worker.sh / stop-worker.sh

Start or stop a Spark worker daemon on the current machine.

**Usage:**
```bash
# Start worker connecting to master
./sbin/start-worker.sh spark://master:7077

# Stop worker
./sbin/stop-worker.sh
```

**Worker Web UI**: Access at `http://<worker-host>:8081/`

**Configuration:**
```bash
# In conf/spark-env.sh
export SPARK_WORKER_CORES=8      # Number of cores to use
export SPARK_WORKER_MEMORY=16g   # Memory to allocate
export SPARK_WORKER_PORT=7078    # Worker port
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_DIR=/var/spark/work  # Work directory
```

### start-workers.sh / stop-workers.sh

Start or stop workers on all machines listed in `conf/workers`.

**Usage:**
```bash
# Start all workers
./sbin/start-workers.sh spark://master:7077

# Stop all workers
./sbin/stop-workers.sh
```

**Requirements:**
- `conf/workers` file configured
- SSH access to all worker machines
- Master URL (for starting)

## History Server Scripts

### start-history-server.sh / stop-history-server.sh

Start or stop the Spark History Server for viewing completed application logs.

**Usage:**
```bash
# Start history server
./sbin/start-history-server.sh

# Stop history server
./sbin/stop-history-server.sh
```

**History Server UI**: Access at `http://<host>:18080/`

**Configuration:**
```properties
# In conf/spark-defaults.conf
spark.history.fs.logDirectory=hdfs://namenode/spark-logs
spark.history.ui.port=18080
spark.eventLog.enabled=true
spark.eventLog.dir=hdfs://namenode/spark-logs
```

**Requirements:**
- Applications must have event logging enabled
- Log directory must be accessible

## Shuffle Service Scripts

### start-shuffle-service.sh / stop-shuffle-service.sh

Start or stop the external shuffle service (for YARN).

**Usage:**
```bash
# Start shuffle service
./sbin/start-shuffle-service.sh

# Stop shuffle service
./sbin/stop-shuffle-service.sh
```

**Note**: Typically used only when running on YARN without the YARN auxiliary service.

## Configuration Files

### conf/workers

Lists worker hostnames, one per line.

**Example:**
```
worker1.example.com
worker2.example.com
worker3.example.com
```

**Usage:**
- Used by `start-all.sh` and `start-workers.sh`
- Each line should contain a hostname or IP address
- Blank lines and lines starting with `#` are ignored

### conf/spark-env.sh

Environment variables for Spark daemons.

**Example:**
```bash
#!/usr/bin/env bash

# Java
export JAVA_HOME=/usr/lib/jvm/java-17

# Master settings
export SPARK_MASTER_HOST=master.example.com
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Worker settings
export SPARK_WORKER_CORES=8
export SPARK_WORKER_MEMORY=16g
export SPARK_WORKER_PORT=7078
export SPARK_WORKER_WEBUI_PORT=8081
export SPARK_WORKER_DIR=/var/spark/work

# Directories
export SPARK_LOG_DIR=/var/log/spark
export SPARK_PID_DIR=/var/run/spark

# History Server
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://namenode/spark-logs"

# Additional Java options
export SPARK_DAEMON_JAVA_OPTS="-Dspark.deploy.recoveryMode=ZOOKEEPER -Dspark.deploy.zookeeper.url=zk1:2181,zk2:2181"
```

**Key Variables:**

**Master:**
- `SPARK_MASTER_HOST`: Master hostname
- `SPARK_MASTER_PORT`: Master port (default: 7077)
- `SPARK_MASTER_WEBUI_PORT`: Web UI port (default: 8080)

**Worker:**
- `SPARK_WORKER_CORES`: Number of cores per worker
- `SPARK_WORKER_MEMORY`: Memory per worker (e.g., 16g)
- `SPARK_WORKER_PORT`: Worker communication port
- `SPARK_WORKER_WEBUI_PORT`: Worker web UI port (default: 8081)
- `SPARK_WORKER_DIR`: Directory for scratch space and logs
- `SPARK_WORKER_INSTANCES`: Number of worker instances per machine

**General:**
- `SPARK_LOG_DIR`: Directory for daemon logs
- `SPARK_PID_DIR`: Directory for PID files
- `SPARK_IDENT_STRING`: Identifier for daemons (default: username)
- `SPARK_NICENESS`: Nice value for daemons
- `SPARK_DAEMON_MEMORY`: Memory for daemon processes

## Setting Up a Standalone Cluster

### Step 1: Install Spark on All Nodes

```bash
# Download and extract Spark on each machine
tar xzf spark-X.Y.Z-bin-hadoopX.tgz
cd spark-X.Y.Z-bin-hadoopX
```

### Step 2: Configure spark-env.sh

Create `conf/spark-env.sh` from template:
```bash
cp conf/spark-env.sh.template conf/spark-env.sh
# Edit conf/spark-env.sh with appropriate settings
```

### Step 3: Configure Workers File

Create `conf/workers`:
```bash
cp conf/workers.template conf/workers
# Add worker hostnames, one per line
```

### Step 4: Configure SSH Access

Set up password-less SSH from master to all workers:
```bash
ssh-keygen -t rsa
ssh-copy-id user@worker1
ssh-copy-id user@worker2
# ... for each worker
```

### Step 5: Synchronize Configuration

Copy configuration to all workers:
```bash
for host in $(cat conf/workers); do
  rsync -av conf/ user@$host:spark/conf/
done
```

### Step 6: Start the Cluster

```bash
./sbin/start-all.sh
```

### Step 7: Verify

- Check master UI: `http://master:8080`
- Check worker UIs: `http://worker1:8081`, etc.
- Look for workers registered with master

## High Availability

For production deployments, configure high availability with ZooKeeper.

### ZooKeeper-based HA Configuration

**In conf/spark-env.sh:**
```bash
export SPARK_DAEMON_JAVA_OPTS="
  -Dspark.deploy.recoveryMode=ZOOKEEPER
  -Dspark.deploy.zookeeper.url=zk1:2181,zk2:2181,zk3:2181
  -Dspark.deploy.zookeeper.dir=/spark
"
```

### Start Multiple Masters

```bash
# On master1
./sbin/start-master.sh

# On master2
./sbin/start-master.sh

# On master3
./sbin/start-master.sh
```

### Connect Workers to All Masters

```bash
./sbin/start-worker.sh spark://master1:7077,master2:7077,master3:7077
```

**Automatic failover:** If active master fails, standby masters detect the failure and one becomes active.

## Monitoring and Logs

### Log Files

Daemon logs are written to `$SPARK_LOG_DIR` (default: `logs/`):

```bash
# Master log
$SPARK_LOG_DIR/spark-$USER-org.apache.spark.deploy.master.Master-*.out

# Worker log
$SPARK_LOG_DIR/spark-$USER-org.apache.spark.deploy.worker.Worker-*.out

# History Server log
$SPARK_LOG_DIR/spark-$USER-org.apache.spark.deploy.history.HistoryServer-*.out
```

### View Logs

```bash
# Tail master log
tail -f logs/spark-*-master-*.out

# Tail worker log
tail -f logs/spark-*-worker-*.out

# Search for errors
grep ERROR logs/spark-*-master-*.out
```

### Web UIs

- **Master UI**: `http://<master>:8080` - Cluster status, workers, applications
- **Worker UI**: `http://<worker>:8081` - Worker status, running executors
- **Application UI**: `http://<driver>:4040` - Running application metrics
- **History Server**: `http://<history-server>:18080` - Completed applications

## Advanced Configuration

### Memory Overhead

Reserve memory for system processes:
```bash
export SPARK_DAEMON_MEMORY=2g
```

### Multiple Workers per Machine

Run multiple worker instances on a single machine:
```bash
export SPARK_WORKER_INSTANCES=2
export SPARK_WORKER_CORES=4      # Cores per instance
export SPARK_WORKER_MEMORY=8g    # Memory per instance
```

### Work Directory

Change worker scratch space:
```bash
export SPARK_WORKER_DIR=/mnt/fast-disk/spark-work
```

### Port Configuration

Use non-default ports:
```bash
export SPARK_MASTER_PORT=9077
export SPARK_MASTER_WEBUI_PORT=9080
export SPARK_WORKER_PORT=9078
export SPARK_WORKER_WEBUI_PORT=9081
```

## Security

### Enable Authentication

```bash
export SPARK_DAEMON_JAVA_OPTS="
  -Dspark.authenticate=true
  -Dspark.authenticate.secret=your-secret-key
"
```

### Enable SSL

```bash
export SPARK_DAEMON_JAVA_OPTS="
  -Dspark.ssl.enabled=true
  -Dspark.ssl.keyStore=/path/to/keystore
  -Dspark.ssl.keyStorePassword=password
  -Dspark.ssl.trustStore=/path/to/truststore
  -Dspark.ssl.trustStorePassword=password
"
```

## Troubleshooting

### Master Won't Start

**Check:**
1. Port 7077 is available: `netstat -an | grep 7077`
2. Hostname is resolvable: `ping $SPARK_MASTER_HOST`
3. Logs for errors: `cat logs/spark-*-master-*.out`

### Workers Not Connecting

**Check:**
1. Master URL is correct
2. Network connectivity: `telnet master 7077`
3. Firewall allows connections
4. Worker logs: `cat logs/spark-*-worker-*.out`

### SSH Connection Issues

**Solutions:**
1. Verify SSH key: `ssh worker1 echo test`
2. Check SSH config: `~/.ssh/config`
3. Use SSH agent: `eval $(ssh-agent); ssh-add`

### Insufficient Resources

**Check:**
- Worker has enough memory: `free -h`
- Enough cores available: `nproc`
- Disk space: `df -h`

## Cluster Shutdown

### Graceful Shutdown

```bash
# Stop all workers first
./sbin/stop-workers.sh

# Stop master
./sbin/stop-master.sh

# Or stop everything
./sbin/stop-all.sh
```

### Check All Stopped

```bash
# Check for running Java processes
jps | grep -E "(Master|Worker)"
```

### Force Kill if Needed

```bash
# Kill any remaining Spark processes
pkill -f org.apache.spark.deploy
```

## Best Practices

1. **Use HA in production**: Configure ZooKeeper-based HA
2. **Monitor resources**: Watch CPU, memory, disk usage
3. **Separate log directories**: Use dedicated disk for logs
4. **Regular maintenance**: Clean old logs and application data
5. **Automate startup**: Use systemd or init scripts
6. **Configure limits**: Set file descriptor and process limits
7. **Use external shuffle service**: For better fault tolerance
8. **Back up metadata**: Regularly back up ZooKeeper data

## Scripts Reference

| Script | Purpose |
|--------|---------|
| `start-all.sh` | Start master and all workers |
| `stop-all.sh` | Stop master and all workers |
| `start-master.sh` | Start master on current machine |
| `stop-master.sh` | Stop master |
| `start-worker.sh` | Start worker on current machine |
| `stop-worker.sh` | Stop worker |
| `start-workers.sh` | Start workers on all machines in `conf/workers` |
| `stop-workers.sh` | Stop all workers |
| `start-history-server.sh` | Start history server |
| `stop-history-server.sh` | Stop history server |

## Further Reading

- [Spark Standalone Mode](../docs/spark-standalone.md)
- [Cluster Mode Overview](../docs/cluster-overview.md)
- [Configuration Guide](../docs/configuration.md)
- [Security Guide](../docs/security.md)
- [Monitoring Guide](../docs/monitoring.md)

## User-Facing Scripts

For user-facing scripts (spark-submit, spark-shell, etc.), see [../bin/README.md](../bin/README.md).
