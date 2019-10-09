package io.radanalytics.operator.cluster;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.radanalytics.operator.historyServer.HistoryServerHelper;
import io.radanalytics.operator.resource.LabelsHelper;
import io.radanalytics.types.*;

import java.util.*;

import static io.radanalytics.operator.Constants.*;
import static io.radanalytics.operator.resource.LabelsHelper.OPERATOR_KIND_LABEL;

public class KubernetesSparkClusterDeployer {
    private KubernetesClient client;
    private String entityName;
    private String prefix;
    private String namespace;

    KubernetesSparkClusterDeployer(KubernetesClient client, String entityName, String prefix, String namespace) {
        this.client = client;
        this.entityName = entityName;
        this.prefix = prefix;
        this.namespace = namespace;
    }

    public KubernetesResourceList getResourceList(SparkCluster cluster) {
        synchronized (this.client) {
            checkForInjectionVulnerabilities(cluster, namespace);
            String name = cluster.getName();

            Map<String, String> allMasterLabels = new HashMap<>();
            if (cluster.getLabels() != null) allMasterLabels.putAll(cluster.getLabels());
            if (cluster.getMaster() != null && cluster.getMaster().getLabels() != null)
                allMasterLabels.putAll(cluster.getMaster().getLabels());

            ReplicationController masterRc = getRCforMaster(cluster);
            ReplicationController workerRc = getRCforWorker(cluster);
            Service masterService = getService(false, name, 7077, allMasterLabels);
            List<HasMetadata> list = new ArrayList<>(Arrays.asList(masterRc, workerRc, masterService));
            if (cluster.getSparkWebUI()) {
                Service masterUiService = getService(true, name, 8080, allMasterLabels);
                list.add(masterUiService);
            }

            // pvc for history server (in case of sharedVolume strategy)
            if (HistoryServerHelper.needsVolume(cluster)) {
                PersistentVolumeClaim pvc = getPersistentVolumeClaim(cluster, getDefaultLabels(name));
                list.add(pvc);
            }
            KubernetesList resources = new KubernetesListBuilder().withItems(list).build();
            return resources;
        }
    }

    private ReplicationController getRCforMaster(SparkCluster cluster) {
        return getRCforMasterOrWorker(true, cluster);
    }

    private ReplicationController getRCforWorker(SparkCluster cluster) {
        return getRCforMasterOrWorker(false, cluster);
    }

    private Service getService(boolean isUi, String name, int port, Map<String, String> allMasterLabels) {
        Map<String, String> labels = getDefaultLabels(name);
        labels.put(prefix + LabelsHelper.OPERATOR_SEVICE_TYPE_LABEL, isUi ? OPERATOR_TYPE_UI_LABEL : OPERATOR_TYPE_MASTER_LABEL);
        labels.putAll(allMasterLabels);
        Service masterService = new ServiceBuilder().withNewMetadata().withName(isUi ? name + "-ui" : name)
                .withLabels(labels).endMetadata()
                .withNewSpec().withSelector(getSelector(name, name + "-m"))
                .withPorts(new ServicePortBuilder().withPort(port).withNewTargetPort()
                        .withIntVal(port).endTargetPort().withProtocol("TCP").build())
                .endSpec().build();
        return masterService;
    }

    public static EnvVar env(String key, String value) {
        return new EnvVarBuilder().withName(key).withValue(value).build();
    }

    private ReplicationController getRCforMasterOrWorker(boolean isMaster, SparkCluster cluster) {
        String name = cluster.getName();
        String podName = name + (isMaster ? "-m" : "-w");
        Map<String, String> selector = getSelector(name, podName);

        List<ContainerPort> ports = new ArrayList<>(2);
        List<EnvVar> envVars = new ArrayList<>();
        envVars.add(env("OSHINKO_SPARK_CLUSTER", name));
        cluster.getEnv().forEach(kv -> {
            envVars.add(env(kv.getName(), kv.getValue()));
        });
        if (isMaster) {
            ContainerPort apiPort = new ContainerPortBuilder().withName("spark-master").withContainerPort(7077).withProtocol("TCP").build();
            ports.add(apiPort);
            if (cluster.getSparkWebUI()) {
                ContainerPort uiPort = new ContainerPortBuilder().withName("spark-webui").withContainerPort(8080).withProtocol("TCP").build();
                ports.add(uiPort);
            }
        } else {
            envVars.add(env("SPARK_MASTER_ADDRESS", "spark://" + name + ":7077"));
            if (cluster.getSparkWebUI()) {
                ContainerPort uiPort = new ContainerPortBuilder().withName("spark-webui").withContainerPort(8081).withProtocol("TCP").build();
                ports.add(uiPort);
                envVars.add(env("SPARK_MASTER_UI_ADDRESS", "http://" + name + "-ui:8080"));
            }
        }
        if (cluster.getMetrics()) {
            envVars.add(env("SPARK_METRICS_ON", "prometheus"));
            ContainerPort metricsPort = new ContainerPortBuilder().withName("metrics").withContainerPort(7777).withProtocol("TCP").build();
            ports.add(metricsPort);
        }

        final String cmName = InitContainersHelper.getExpectedCMName(cluster);
        final boolean cmExists = cmExists(cmName);
        final int expectedMasterDelay = InitContainersHelper.getExpectedDelay(cluster, cmExists, true);
        final int expectedWorkerDelay = InitContainersHelper.getExpectedDelay(cluster, cmExists, false);
        Probe masterReadiness = new ProbeBuilder().withNewExec().withCommand(Arrays.asList("/bin/bash", "-c", "curl -s localhost:8080 | grep -e Status.*ALIVE")).endExec()
                .withFailureThreshold(3)
                .withInitialDelaySeconds(expectedMasterDelay - 4)
                .withPeriodSeconds(7)
                .withSuccessThreshold(1)
                .withTimeoutSeconds(1).build();

        Probe workerReadiness = new ProbeBuilder().withNewExec().withCommand(Arrays.asList("/bin/bash", "-c", "curl -s localhost:8081 | grep -e 'Master URL:.*spark://'" +
                " || echo Unable to connect to the Spark master at $SPARK_MASTER_ADDRESS")).endExec()
                .withFailureThreshold(3)
                .withInitialDelaySeconds(expectedWorkerDelay - 4)
                .withPeriodSeconds(7)
                .withSuccessThreshold(1)
                .withTimeoutSeconds(1).build();

        Probe generalLivenessProbe = new ProbeBuilder().withFailureThreshold(3).withNewHttpGet()
                .withPath("/")
                .withNewPort().withIntVal(isMaster ? 8080 : 8081).endPort()
                .withScheme("HTTP")
                .endHttpGet()
                .withPeriodSeconds(10)
                .withSuccessThreshold(1)
                .withFailureThreshold(6)
                .withInitialDelaySeconds(isMaster ? expectedMasterDelay : expectedWorkerDelay)
                .withTimeoutSeconds(1).build();

        String imageRef = getDefaultSparkImage(); // from Constants
        if (cluster.getCustomImage() != null) {
            imageRef = cluster.getCustomImage();
        }

        ContainerBuilder containerBuilder = new ContainerBuilder().withEnv(envVars).withImage(imageRef)
                .withImagePullPolicy("IfNotPresent")
                .withName(name + (isMaster ? "-m" : "-w"))
                .withTerminationMessagePath("/dev/termination-log")
                .withTerminationMessagePolicy("File")
                .withPorts(ports)
                .withLivenessProbe(generalLivenessProbe)
                .withReadinessProbe(isMaster ? masterReadiness : workerReadiness);

        // limits & cmd
        containerBuilder = augmentContainerBuilder(cluster, containerBuilder, isMaster);

        // labels
        Map<String, String> labels = getDefaultLabels(name);
        labels.put(prefix + LabelsHelper.OPERATOR_RC_TYPE_LABEL, isMaster ? OPERATOR_TYPE_MASTER_LABEL : OPERATOR_TYPE_WORKER_LABEL);
        addLabels(labels, cluster, isMaster);

        Map<String, String> podLabels = getSelector(name, podName);
        podLabels.put(prefix + LabelsHelper.OPERATOR_POD_TYPE_LABEL, isMaster ? OPERATOR_TYPE_MASTER_LABEL : OPERATOR_TYPE_WORKER_LABEL);
        addLabels(podLabels, cluster, isMaster);

        PodTemplateSpecFluent.SpecNested<ReplicationControllerSpecFluent.TemplateNested<ReplicationControllerFluent.SpecNested<ReplicationControllerBuilder>>> rcBuilder = new ReplicationControllerBuilder().withNewMetadata()
                .withName(podName).withLabels(labels)
                .endMetadata()
                .withNewSpec().withReplicas(
                        isMaster
                                ?
                                Optional.ofNullable(cluster.getMaster()).orElse(new Master()).getInstances()
                                :
                                Optional.ofNullable(cluster.getWorker()).orElse(new Worker()).getInstances()
                )
                .withSelector(selector)
                .withNewTemplate().withNewMetadata().withLabels(podLabels).endMetadata()
                .withNewSpec().withContainers(containerBuilder.build());

        ReplicationController rc = rcBuilder.endSpec().endTemplate().endSpec().build();

        // history server
        if (isMaster && null != cluster.getHistoryServer()) {
            augmentSparkConfWithHistoryServer(cluster);
        }

        // add init containers that will prepare the data on the nodes or override the configuration
        if (!cluster.getDownloadData().isEmpty() || !cluster.getSparkConfiguration().isEmpty() || cmExists) {
            InitContainersHelper.addInitContainers(rc, cluster, cmExists, isMaster);
        }
        return rc;
    }

    private PersistentVolumeClaim getPersistentVolumeClaim(SparkCluster cluster, Map<String, String> labels) {
        SharedVolume sharedVolume = Optional.ofNullable(cluster.getHistoryServer().getSharedVolume()).orElse(new SharedVolume());
        Map<String,Quantity> requests = new HashMap<>();
        requests.put("storage", new QuantityBuilder().withAmount(sharedVolume.getSize()).build());
        Map<String, String> matchLabels = sharedVolume.getMatchLabels();
        if (null == matchLabels || matchLabels.isEmpty()) {
            // if no match labels are specified, we assume the default one: radanalytics.io/SparkCluster: spark-cluster-name
            matchLabels = new HashMap<>(1);
            matchLabels.put(prefix + entityName, cluster.getName());
        }
        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder().withNewMetadata().withName(cluster.getName() + "-claim").withLabels(labels).endMetadata()
                .withNewSpec().withAccessModes("ReadWriteMany")
                .withNewSelector().withMatchLabels(matchLabels).endSelector()
                .withNewResources().withRequests(requests).endResources().endSpec().build();
        return pvc;
    }

    private void augmentSparkConfWithHistoryServer(SparkCluster cluster) {

        String eventLog;
        if (HistoryServerHelper.needsVolume(cluster)) {
            SharedVolume sharedVolume = Optional.ofNullable(cluster.getHistoryServer().getSharedVolume()).orElse(new SharedVolume());
            eventLog = sharedVolume.getMountPath();
        } else {
            eventLog = cluster.getHistoryServer().getRemoteURI();
        }

        SparkConfiguration nv1 = new SparkConfiguration();
        nv1.setName("spark.eventLog.dir");
        nv1.setValue(eventLog);
        SparkConfiguration nv2 = new SparkConfiguration();
        nv2.setName("spark.eventLog.enabled");
        nv2.setValue("true");
        cluster.getSparkConfiguration().add(0, nv1);
        cluster.getSparkConfiguration().add(0, nv2);
    }

    private void augmentSparkConfWithJarsPath(SparkCluster cluster) {
        SparkConfiguration nv = new SparkConfiguration();
        nv.setName("spark.jars.ivy");
        nv.setValue("/tmp");
        cluster.getSparkConfiguration().add(0, nv);
    }

    private ContainerBuilder augmentContainerBuilder(SparkCluster cluster, ContainerBuilder builder, boolean isMaster) {
        Master m = null;
        Worker w = null;
        if (isMaster) {
            m = Optional.ofNullable(cluster.getMaster()).orElse(new Master());
        } else {
            w = Optional.ofNullable(cluster.getWorker()).orElse(new Worker());
        }

        Map<String, Quantity> limits = new HashMap<>(2);
        Optional.ofNullable(isMaster ? m.getMemory() : w.getMemory()).ifPresent(memory -> limits.put("memory", new Quantity(memory)));
        Optional.ofNullable(isMaster ? m.getCpu() : w.getCpu()).ifPresent(cpu -> limits.put("cpu", new Quantity(cpu)));

        if (!limits.isEmpty()) {
            builder = builder.withResources(new ResourceRequirements(limits, limits));
        }

        // if maven deps are not empty let spark-submit to download them
        if (!cluster.getMavenDependencies().isEmpty()) {
            augmentSparkConfWithJarsPath(cluster);
            List<String> command = isMaster ? m.getCommand() : w.getCommand();
            List<String> commandArgs = isMaster ? m.getCommandArgs() : w.getCommandArgs();
            if (!command.isEmpty() || !commandArgs.isEmpty()) {
                throw new IllegalArgumentException("Use either custom mavenDependencies or custom starting command for the image. Unfortunately, you can't have both.");
            }
            command.add("/bin/sh");
            command.add("-c");
            String dependencies = String.join(",", cluster.getMavenDependencies());
            String repositories = cluster.getMavenRepositories().isEmpty() ? "" : " --repositories " + String.join(",", cluster.getMavenRepositories());
            commandArgs.add("/entrypoint pwd ; " + logConfig() + " ; spark-submit --packages " +
                    dependencies +
                    repositories +
                    " --conf spark.jars.ivy=/tmp --driver-java-options '-Dlog4j.configuration=file:///tmp/log4j.properties' " +
                    "--class no-op-ignore-this 0 || true; /entrypoint /launch.sh");
        }

        List<String> command = isMaster ? m.getCommand() : w.getCommand();
        if (!command.isEmpty()) {
            builder = builder.withCommand(command);
        }
        List<String> commandArgs = isMaster ? m.getCommandArgs() : w.getCommandArgs();
        if (!commandArgs.isEmpty()) {
            builder = builder.withArgs(commandArgs);
        }
        return builder;
    }

    private String logConfig(){
        String log4jconfig ="'log4j.rootCategory=ERROR, console\\n" +
                "log4j.appender.console=org.apache.log4j.ConsoleAppender\\n" +
                "log4j.appender.console.layout=org.apache.log4j.PatternLayout'";
        return "echo -e " + log4jconfig + " > /tmp/log4j.properties";

    }

    private void addLabels( Map<String, String> labels, SparkCluster cluster, boolean isMaster) {
        if (cluster.getLabels() != null) labels.putAll(cluster.getLabels());
        if (isMaster) {
            if (cluster.getMaster() != null && cluster.getMaster().getLabels() != null)
                labels.putAll(cluster.getMaster().getLabels());
        } else {
            if (cluster.getWorker() != null && cluster.getWorker().getLabels() != null)
                labels.putAll(cluster.getWorker().getLabels());
        }
    }


    private boolean cmExists(String name) {
        ConfigMap configMap;
        if ("*".equals(namespace)) {
            List<ConfigMap> items = client.configMaps().inAnyNamespace().withField("metadata.name", name).list().getItems();
            configMap = items != null && !items.isEmpty() ? items.get(0) : null;
        } else {
            configMap = client.configMaps().inNamespace(namespace).withName(name).get();
        }
        return configMap != null && configMap.getData() != null && !configMap.getData().isEmpty();
    }

    private Map<String, String> getSelector(String clusterName, String podName) {
        Map<String, String> map = getDefaultLabels(clusterName);
        map.put(prefix + LabelsHelper.OPERATOR_DEPLOYMENT_LABEL, podName);
        return map;
    }

    public Map<String, String> getDefaultLabels(String name) {
        Map<String, String> map = new HashMap<>(3);
        map.put(prefix + OPERATOR_KIND_LABEL, entityName);
        map.put(prefix + entityName, name);
        return map;
    }

    private void checkForInjectionVulnerabilities(SparkCluster app, String namespace) {
        //todo: this
    }
}
