# Spark Resource Managers

This directory contains integrations with various cluster resource managers.

## Overview

Spark can run on different cluster managers:
- **YARN** (Hadoop YARN)
- **Kubernetes** (Container orchestration)
- **Mesos** (General-purpose cluster manager)
- **Standalone** (Spark's built-in cluster manager)

Each integration provides Spark-specific implementation for:
- Resource allocation
- Task scheduling
- Application lifecycle management
- Security integration

## Modules

### kubernetes/

Integration with Kubernetes for container-based deployments.

**Location**: `kubernetes/`

**Key Features:**
- Native Kubernetes resource management
- Dynamic executor allocation
- Volume mounting support
- Kerberos integration
- Custom resource definitions

**Running on Kubernetes:**
```bash
./bin/spark-submit \
  --master k8s://https://<k8s-apiserver-host>:<k8s-apiserver-port> \
  --deploy-mode cluster \
  --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.container.image=spark:3.5.0 \
  local:///opt/spark/examples/jars/spark-examples.jar
```

**Documentation**: See [running-on-kubernetes.md](../docs/running-on-kubernetes.md)

### mesos/

Integration with Apache Mesos cluster manager.

**Location**: `mesos/`

**Key Features:**
- Fine-grained mode (one task per Mesos task)
- Coarse-grained mode (dedicated executors)
- Dynamic allocation
- Mesos frameworks integration

**Running on Mesos:**
```bash
./bin/spark-submit \
  --master mesos://mesos-master:5050 \
  --deploy-mode cluster \
  --class org.apache.spark.examples.SparkPi \
  spark-examples.jar
```

**Documentation**: Check Apache Mesos documentation

### yarn/

Integration with Hadoop YARN (Yet Another Resource Negotiator).

**Location**: `yarn/`

**Key Features:**
- Client and cluster deploy modes
- Dynamic resource allocation
- YARN container management
- Security integration (Kerberos)
- External shuffle service
- Application timeline service integration

**Running on YARN:**
```bash
# Client mode (driver runs locally)
./bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --class org.apache.spark.examples.SparkPi \
  spark-examples.jar

# Cluster mode (driver runs on YARN)
./bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class org.apache.spark.examples.SparkPi \
  spark-examples.jar
```

**Documentation**: See [running-on-yarn.md](../docs/running-on-yarn.md)

## Comparison

### YARN

**Best for:**
- Existing Hadoop deployments
- Enterprise environments with Hadoop ecosystem
- Multi-tenancy with resource queues
- Organizations standardized on YARN

**Pros:**
- Mature and stable
- Rich security features
- Queue-based resource management
- Good tooling and monitoring

**Cons:**
- Requires Hadoop installation
- More complex setup
- Higher overhead

### Kubernetes

**Best for:**
- Cloud-native deployments
- Containerized applications
- Modern microservices architectures
- Multi-cloud environments

**Pros:**
- Container isolation
- Modern orchestration features
- Cloud provider integration
- Active development community

**Cons:**
- Newer integration (less mature)
- Requires Kubernetes cluster
- Learning curve for K8s

### Mesos

**Best for:**
- General-purpose cluster management
- Mixed workload environments (not just Spark)
- Large-scale deployments

**Pros:**
- Fine-grained resource allocation
- Flexible framework support
- Good for mixed workloads

**Cons:**
- Less common than YARN/K8s
- Setup complexity
- Smaller community

### Standalone

**Best for:**
- Quick start and development
- Small clusters
- Dedicated Spark clusters

**Pros:**
- Simple setup
- No dependencies
- Fast deployment

**Cons:**
- Limited resource management
- No multi-tenancy
- Basic scheduling

## Architecture

### Resource Manager Integration

```
Spark Application
       ↓
SparkContext
       ↓
Cluster Manager Client
       ↓
Resource Manager (YARN/K8s/Mesos)
       ↓
Container/Pod/Task Launch
       ↓
Executor Processes
```

### Common Components

Each integration implements:

1. **SchedulerBackend**: Launches executors and schedules tasks
2. **ApplicationMaster/Driver**: Manages application lifecycle
3. **ExecutorBackend**: Runs tasks on executors
4. **Resource Allocation**: Requests and manages resources
5. **Security Integration**: Authentication and authorization

## Building

### Build All Resource Manager Modules

```bash
# Build all resource manager integrations
./build/mvn -pl 'resource-managers/*' -am package
```

### Build Specific Modules

```bash
# YARN only
./build/mvn -pl resource-managers/yarn -am package

# Kubernetes only
./build/mvn -pl resource-managers/kubernetes/core -am package

# Mesos only
./build/mvn -pl resource-managers/mesos -am package
```

### Build with Specific Profiles

```bash
# Build with Kubernetes support
./build/mvn -Pkubernetes package

# Build with YARN support
./build/mvn -Pyarn package

# Build with Mesos support (requires Mesos libraries)
./build/mvn -Pmesos package
```

## Configuration

### YARN Configuration

**Key settings:**
```properties
# Resource allocation
spark.executor.instances=10
spark.executor.memory=4g
spark.executor.cores=2

# YARN specific
spark.yarn.am.memory=1g
spark.yarn.am.cores=1
spark.yarn.queue=default
spark.yarn.jars=hdfs:///spark-jars/*

# Dynamic allocation
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=1
spark.dynamicAllocation.maxExecutors=100
spark.shuffle.service.enabled=true
```

### Kubernetes Configuration

**Key settings:**
```properties
# Container image
spark.kubernetes.container.image=my-spark:latest
spark.kubernetes.container.image.pullPolicy=Always

# Resource allocation
spark.executor.instances=5
spark.kubernetes.executor.request.cores=1
spark.kubernetes.executor.limit.cores=2
spark.kubernetes.executor.request.memory=4g

# Namespace and service account
spark.kubernetes.namespace=spark
spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa

# Volumes
spark.kubernetes.driver.volumes.persistentVolumeClaim.data.options.claimName=spark-pvc
spark.kubernetes.driver.volumes.persistentVolumeClaim.data.mount.path=/data
```

### Mesos Configuration

**Key settings:**
```properties
# Mesos master
spark.mesos.coarse=true
spark.executor.uri=hdfs://path/to/spark.tgz

# Resource allocation
spark.executor.memory=4g
spark.cores.max=20

# Mesos specific
spark.mesos.role=spark
spark.mesos.constraints=rack:us-east
```

## Source Code Organization

```
resource-managers/
├── kubernetes/
│   ├── core/                    # Core K8s integration
│   │   └── src/main/scala/org/apache/spark/
│   │       ├── deploy/k8s/      # Deployment logic
│   │       ├── scheduler/       # K8s scheduler backend
│   │       └── executor/        # K8s executor backend
│   └── integration-tests/       # K8s integration tests
├── mesos/
│   └── src/main/scala/org/apache/spark/
│       ├── scheduler/           # Mesos scheduler
│       └── executor/            # Mesos executor
└── yarn/
    └── src/main/scala/org/apache/spark/
        ├── deploy/yarn/         # YARN deployment
        ├── scheduler/           # YARN scheduler
        └── executor/            # YARN executor
```

## Development

### Testing Resource Manager Integrations

```bash
# Run YARN tests
./build/mvn test -pl resource-managers/yarn

# Run Kubernetes tests
./build/mvn test -pl resource-managers/kubernetes/core

# Run Mesos tests
./build/mvn test -pl resource-managers/mesos
```

### Integration Tests

**Kubernetes:**
```bash
cd resource-managers/kubernetes/integration-tests
./dev/dev-run-integration-tests.sh
```

See `kubernetes/integration-tests/README.md` for details.

## Security

### YARN Security

**Kerberos authentication:**
```bash
./bin/spark-submit \
  --master yarn \
  --principal user@REALM \
  --keytab /path/to/user.keytab \
  --class org.apache.spark.examples.SparkPi \
  spark-examples.jar
```

**Token renewal:**
```properties
spark.yarn.principal=user@REALM
spark.yarn.keytab=/path/to/keytab
spark.yarn.token.renewal.interval=86400
```

### Kubernetes Security

**Service account:**
```properties
spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa
spark.kubernetes.authenticate.executor.serviceAccountName=spark-sa
```

**Secrets:**
```bash
kubectl create secret generic spark-secret --from-literal=password=mypassword
```

```properties
spark.kubernetes.driver.secrets.spark-secret=/etc/secrets
```

### Mesos Security

**Authentication:**
```properties
spark.mesos.principal=spark-user
spark.mesos.secret=spark-secret
```

## Migration Guide

### Moving from Standalone to YARN

1. Set up Hadoop cluster
2. Configure YARN resource manager
3. Enable external shuffle service
4. Update spark-submit commands to use `--master yarn`
5. Test dynamic allocation

### Moving from YARN to Kubernetes

1. Build Docker image with Spark
2. Push image to container registry
3. Create Kubernetes namespace and service account
4. Update spark-submit to use `--master k8s://`
5. Configure volume mounts for data access

## Troubleshooting

### YARN Issues

**Application stuck in ACCEPTED state:**
- Check YARN capacity
- Verify queue settings
- Check resource availability

**Container allocation failures:**
- Increase memory overhead
- Check node resources
- Verify memory/core requests

### Kubernetes Issues

**Image pull failures:**
- Verify image name and tag
- Check image pull secrets
- Ensure registry is accessible

**Pod failures:**
- Check pod logs: `kubectl logs <pod-name>`
- Verify service account permissions
- Check resource limits

### Mesos Issues

**Framework registration failures:**
- Verify Mesos master URL
- Check authentication settings
- Ensure proper role configuration

## Best Practices

1. **Choose the right manager**: Based on infrastructure and requirements
2. **Enable dynamic allocation**: For better resource utilization
3. **Use external shuffle service**: For executor failure tolerance
4. **Configure memory overhead**: Account for non-heap memory
5. **Monitor resource usage**: Track executor and driver metrics
6. **Use appropriate deploy mode**: Client for interactive, cluster for production
7. **Implement security**: Enable authentication and encryption
8. **Test failure scenarios**: Verify fault tolerance

## Performance Tuning

### YARN Performance

```properties
# Memory overhead
spark.yarn.executor.memoryOverhead=512m

# Locality wait
spark.locality.wait=3s

# Container reuse
spark.yarn.executor.launch.parallelism=10
```

### Kubernetes Performance

```properties
# Resource limits
spark.kubernetes.executor.limit.cores=2

# Volume performance
spark.kubernetes.driver.volumes.emptyDir.cache.medium=Memory

# Network optimization
spark.kubernetes.executor.podNamePrefix=spark-exec
```

### Mesos Performance

```properties
# Fine-grained mode for better sharing
spark.mesos.coarse=false

# Container timeout
spark.mesos.executor.docker.pullTimeout=600
```

## Further Reading

- [Running on YARN](../docs/running-on-yarn.md)
- [Running on Kubernetes](../docs/running-on-kubernetes.md)
- [Cluster Mode Overview](../docs/cluster-overview.md)
- [Configuration Guide](../docs/configuration.md)
- [Security Guide](../docs/security.md)

## Contributing

For contributing to resource manager integrations, see [CONTRIBUTING.md](../CONTRIBUTING.md).

When adding features:
- Ensure cross-compatibility
- Add comprehensive tests
- Update documentation
- Consider security implications
