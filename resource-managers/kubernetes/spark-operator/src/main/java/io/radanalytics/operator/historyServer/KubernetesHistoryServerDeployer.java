package io.radanalytics.operator.historyServer;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentFluent;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpecFluent;
import io.fabric8.kubernetes.api.model.extensions.HTTPIngressPathBuilder;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.extensions.IngressBuilder;
import io.fabric8.kubernetes.api.model.extensions.IngressRuleBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.radanalytics.types.SparkConfiguration;
import io.radanalytics.types.SparkHistoryServer;

import java.util.*;

import static io.radanalytics.operator.Constants.getDefaultSparkImage;
import static io.radanalytics.operator.resource.LabelsHelper.OPERATOR_KIND_LABEL;

public class KubernetesHistoryServerDeployer {


    private String entityName;
    private String prefix;

    KubernetesHistoryServerDeployer(String entityName, String prefix) {
        this.entityName = entityName;
        this.prefix = prefix;
    }

    public KubernetesResourceList getResourceList(SparkHistoryServer hs, String namespace, boolean isOpenshift) {
        checkForInjectionVulnerabilities(hs, namespace);
        List<HasMetadata> resources = new ArrayList<>();

        Map<String, String> defaultLabels = getDefaultLabels(hs.getName());
        int uiPort = hs.getInternalPort();

        Deployment deployment = getDeployment(hs, defaultLabels);
        resources.add(deployment);

        if (HistoryServerHelper.needsVolume(hs) && null != hs.getSharedVolume()) {
            PersistentVolumeClaim pvc = getPersistentVolumeClaim(hs, defaultLabels);
            resources.add(pvc);
        }

        // expose the service using Ingress or Route
        if (hs.getExpose()) {
            Service service = new ServiceBuilder().withNewMetadata().withLabels(defaultLabels).withName(hs.getName())
                    .endMetadata().withNewSpec().withSelector(defaultLabels)
                    .withPorts(new ServicePortBuilder().withName("web-ui").withPort(uiPort).build()).endSpec().build();
            resources.add(service);
            if (isOpenshift) {
                Route route = new RouteBuilder().withNewMetadata().withName(hs.getName())
                        .withLabels(defaultLabels).endMetadata()
                        .withNewSpec().withHost(hs.getHost())
                        .withNewTo("Service", hs.getName(), 100)
                        .endSpec().build();
                resources.add(route);
            } else {
                Ingress ingress = new IngressBuilder().withNewMetadata().withName(hs.getName())
                        .withLabels(defaultLabels).endMetadata()
                        .withNewSpec().withRules(new IngressRuleBuilder().withHost(hs.getHost()).withNewHttp()
                                .withPaths(new HTTPIngressPathBuilder().withNewBackend().withServiceName(hs.getName()).withNewServicePort(uiPort).endBackend().build()).endHttp().build())
                        .endSpec().build();
                resources.add(ingress);
            }
        }

        KubernetesList k8sResources = new KubernetesListBuilder().withItems(resources).build();
        return k8sResources;
    }

    private Deployment getDeployment(SparkHistoryServer hs, Map<String, String> labels) {
        String volumeName = "history-server-volume";

        ContainerBuilder containerBuilder = new ContainerBuilder().withName("history-server")
                .withImage(Optional.ofNullable(hs.getCustomImage()).orElse(getDefaultSparkImage()))
                .withCommand(Arrays.asList("/bin/sh", "-xc"))
                .withArgs("mkdir /tmp/spark-events || true ; /entrypoint ls ; /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer")
                .withEnv(env("SPARK_HISTORY_OPTS", getHistoryOpts(hs)))
                .withPorts(new ContainerPortBuilder().withName("web-ui").withContainerPort(hs.getInternalPort()).build());
        if (HistoryServerHelper.needsVolume(hs) && null != hs.getSharedVolume()) {
            containerBuilder = containerBuilder.withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath(hs.getSharedVolume().getMountPath()).build());
        }
        Container historyServerContainer = containerBuilder.build();

        PodTemplateSpecFluent.SpecNested<DeploymentSpecFluent.TemplateNested<DeploymentFluent.SpecNested<DeploymentBuilder>>> deploymentBuilder = new DeploymentBuilder()
                .withNewMetadata().withName(hs.getName()).withLabels(labels).endMetadata()
                .withNewSpec().withReplicas(1).withNewSelector().withMatchLabels(labels).endSelector()
                .withNewStrategy().withType("Recreate").endStrategy()
                .withNewTemplate().withNewMetadata().withLabels(labels).endMetadata()
                .withNewSpec().withServiceAccountName("spark-operator")
                .withContainers(historyServerContainer);
        if (HistoryServerHelper.needsVolume(hs) && null != hs.getSharedVolume()) {
            deploymentBuilder = deploymentBuilder.withVolumes(new VolumeBuilder().withName(volumeName).withNewPersistentVolumeClaim()
                    .withReadOnly(false).withClaimName(hs.getName() + "-claim").endPersistentVolumeClaim().build());
        }
        Deployment deployment = deploymentBuilder.endSpec().endTemplate().endSpec().build();

        return deployment;
    }

    private PersistentVolumeClaim getPersistentVolumeClaim(SparkHistoryServer hs, Map<String, String> labels) {
        Map<String,Quantity> requests = new HashMap<>();
        requests.put("storage", new QuantityBuilder().withAmount(hs.getSharedVolume().getSize()).build());
        Map<String, String> matchLabels = hs.getSharedVolume().getMatchLabels();
        if (null == matchLabels || matchLabels.isEmpty()) {
            // if no match labels are specified, we assume the default one: radanalytics.io/SparkHistoryServer: history-server-name
            matchLabels = new HashMap<>(1);
            matchLabels.put(prefix + entityName, hs.getName());
        }
        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder().withNewMetadata().withName(hs.getName() + "-claim").withLabels(labels).endMetadata()
                .withNewSpec().withAccessModes("ReadWriteMany")
                .withNewSelector().withMatchLabels(matchLabels).endSelector()
                .withNewResources().withRequests(requests).endResources().endSpec().build();
        return pvc;
    }

    private String getHistoryOpts(SparkHistoryServer hs) {
        // https://spark.apache.org/docs/latest/monitoring.html#spark-history-server-configuration-options

        StringBuilder sb = new StringBuilder();
        sb.append("-Dspark.history.provider=").append(hs.getProvider());
        sb.append(" -Dspark.history.fs.logDirectory=").append(hs.getLogDirectory());
        sb.append(" -Dspark.history.fs.update.interval=").append(hs.getUpdateInterval());
        sb.append(" -Dspark.history.retainedApplications=").append(hs.getRetainedApplications());
        sb.append(" -Dspark.history.maxApplications=").append(hs.getMaxApplications());
        sb.append(" -Dspark.history.ui.port=").append(hs.getInternalPort());

        // kerberos
        if (null != hs.getKerberos()) {
            sb.append(" -Dspark.history.kerberos.enabled=").append(hs.getKerberos().getEnabled());
            sb.append(" -Dspark.history.kerberos.principal=").append(hs.getKerberos().getPrincipal());
            sb.append(" -Dspark.history.kerberos.keytab=").append(hs.getKerberos().getKeytab());
        }
        // cleaner
        if (null != hs.getCleaner()) {
            sb.append(" -Dspark.history.fs.cleaner.enabled=").append(hs.getCleaner().getEnabled());
            sb.append(" -Dspark.history.fs.cleaner.interval=").append(hs.getCleaner().getInterval());
            sb.append(" -Dspark.history.fs.cleaner.maxAge=").append(hs.getCleaner().getMaxAge());
        }

        sb.append(" -Dspark.history.fs.endEventReparseChunkSize=").append(hs.getEndEventReparseChunkSize());
        sb.append(" -Dspark.history.fs.inProgressOptimization.enabled=").append(hs.getInProgressOptimization());
        if (null != hs.getNumReplayThreads()) {
            sb.append(" -Dspark.history.fs.numReplayThreads=").append(hs.getNumReplayThreads());
        }
        sb.append(" -Dspark.history.store.maxDiskUsage=").append(hs.getMaxDiskUsage());
        if (null != hs.getPersistentPath()) {
            sb.append(" -Dspark.history.store.path=").append(hs.getPersistentPath());
        }

        if (null != hs.getSparkConfiguration() && !hs.getSparkConfiguration().isEmpty()) {
            for (SparkConfiguration nv : hs.getSparkConfiguration()) {
                sb.append(" -D").append(nv.getName()).append("=").append(nv.getValue());
            }
        }
        return sb.toString();
    }

    public Map<String, String> getDefaultLabels(String name) {
        Map<String, String> map = new HashMap<>(3);
        map.put(prefix + OPERATOR_KIND_LABEL, entityName);
        map.put(prefix + entityName, name);
        return map;
    }

    public static EnvVar env(String key, String value) {
        return new EnvVarBuilder().withName(key).withValue(value).build();
    }

    private void checkForInjectionVulnerabilities(SparkHistoryServer hs, String namespace) {
        //todo: this
    }
}
