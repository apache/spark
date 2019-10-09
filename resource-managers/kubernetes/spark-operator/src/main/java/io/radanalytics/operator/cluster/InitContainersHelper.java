package io.radanalytics.operator.cluster;

import io.fabric8.kubernetes.api.model.*;
import io.radanalytics.operator.historyServer.HistoryServerHelper;
import io.radanalytics.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static io.radanalytics.operator.Constants.getDefaultSparkImage;

public class InitContainersHelper {

    private static final String NEW_CONF_DIR = "conf-new-dir";
    private static final String NEW_CONF_DIR_PATH = "/tmp/config/new";
    private static final String DEFAULT_SPARK_HOME_PATH = "/opt/spark/";

    /**
     * Based on the SparkCluster configuration, it can add init-containers called 'downloader', 'backup-config' and/or
     * 'override-config'. The firs one will be added if the <code>cluster.getDownloadData()</code> is not empty, while the
     * latter two are always being added together if config map that overrides the default spark configuration exist in the
     * K8s or if the <code>cluster.getSparkConfiguration()</code> is not empty.
     *
     * Optionally adds the init containers that do:
     * <ol>
     *     <li>makes the path for the history server writable by group</li>
     *     <li>downloads the data with curl if it's specified</li>
     *     <li>backups the original (default) Spark configuration into <code>NEW_CONF_DIR_PATH</code></li>
     *     <li>copies/replaces the config files in the <code>NEW_CONF_DIR_PATH</code> with the files coming from the config map</li>
     *     <li>overrides (/appends) the key-value entries in the <code>NEW_CONF_DIR_PATH/spark-defaults.conf</code></li>
     *     <li>use the configuration <code>NEW_CONF_DIR_PATH</code> as the configuration for the spark</li>
     * </ol>
     *
     *
     * @param rc ReplicationController instance
     * @param cluster SparkCluster instance
     * @param cmExists whether config map with overrides exists
     * @return modified ReplicationController instance
     */
    public static final ReplicationController addInitContainers(ReplicationController rc,
                                                                 SparkCluster cluster,
                                                                 boolean cmExists,
                                                                 boolean isMaster) {
        PodSpec podSpec = rc.getSpec().getTemplate().getSpec();

        if (isMaster && HistoryServerHelper.needsVolume(cluster)) {
            createChmodHistoryServerContainer(cluster, podSpec);
        }

        if (!cluster.getDownloadData().isEmpty()) {
            createDownloader(cluster, podSpec);
        }
        if (cmExists || !cluster.getSparkConfiguration().isEmpty()) {
            createBackupContainer(cluster, podSpec);
            createConfigOverrideContainer(cluster, podSpec, cmExists);
        }

        rc.getSpec().getTemplate().setSpec(podSpec);
        return rc;
    }

    private static Container createDownloader(SparkCluster cluster, PodSpec podSpec) {
        final String mountName = "data-dir";
        final String mountPath = "/tmp/";
        final VolumeMount downloadMount = new VolumeMountBuilder().withName(mountName).withMountPath(mountPath).build();
        final Volume downloadVolume = new VolumeBuilder().withName(mountName).withNewEmptyDir().endEmptyDir().build();
        final List<DownloadDatum> downloadData = cluster.getDownloadData();
        final StringBuilder downloaderCmd = new StringBuilder();
        downloadData.forEach(dl -> {
            String url = dl.getUrl();
            String to = dl.getTo();
            // if 'to' ends with slash, we know it's a directory and we use the -P switch to change the prefix,
            // otherwise using -O for renaming the downloaded file
            String param = to.endsWith("/") ? " -P " : " -O ";
            downloaderCmd.append("wget ");
            downloaderCmd.append(url);
            downloaderCmd.append(param);
            downloaderCmd.append(to);
            downloaderCmd.append(" ; ");
        });

        Container downloader = new ContainerBuilder()
                .withName("downloader")
                .withImage("busybox")
                .withImagePullPolicy("IfNotPresent")
                .withCommand("/bin/sh", "-xc")
                .withArgs(downloaderCmd.toString())
                .withVolumeMounts(downloadMount)
                .build();


        podSpec.getContainers().get(0).getVolumeMounts().add(downloadMount);
        podSpec.getVolumes().add(downloadVolume);
        podSpec.getInitContainers().add(downloader);

        return downloader;
    }

    private static Container createBackupContainer(SparkCluster cluster, PodSpec podSpec) {
        final VolumeMount backupMount = new VolumeMountBuilder().withName(NEW_CONF_DIR).withMountPath(NEW_CONF_DIR_PATH).build();
        final Volume backupVolume = new VolumeBuilder().withName(NEW_CONF_DIR).withNewEmptyDir().endEmptyDir().build();

        Container backup = new ContainerBuilder()
                .withName("backup-config")
                .withImage(Optional.ofNullable(cluster.getCustomImage()).orElse(getDefaultSparkImage()))
                .withCommand("/bin/sh", "-xc")
                .withArgs("cp -r " + getSparkHome(cluster) + "/conf/* " + NEW_CONF_DIR_PATH)
                .withVolumeMounts(backupMount)
                .build();


        podSpec.getContainers().get(0).getVolumeMounts().add(backupMount);
        podSpec.getVolumes().add(backupVolume);
        podSpec.getInitContainers().add(backup);

        return backup;
    }

    private static Container createChmodHistoryServerContainer(SparkCluster cluster, PodSpec podSpec) {
        SharedVolume sharedVolume = Optional.ofNullable(cluster.getHistoryServer().getSharedVolume()).orElse(new SharedVolume());
        final VolumeMount mount = new VolumeMountBuilder().withName("history-server-volume").withMountPath(sharedVolume.getMountPath()).build();

        Container chmod = new ContainerBuilder()
                .withName("chmod-history-server")
//                .withNewSecurityContext().withPrivileged(true).endSecurityContext()
                .withImage("busybox")
                .withCommand("/bin/sh", "-xc")
                .withArgs("mkdir -p " + sharedVolume.getMountPath() + " || true ; chmod -R go+ws " + sharedVolume.getMountPath() + " || true")
                .withVolumeMounts(mount)
                .build();

        Volume historyVolume = new VolumeBuilder()
                .withName("history-server-volume")
                .withNewPersistentVolumeClaim()
                .withReadOnly(false)
                .withClaimName(cluster.getName() + "-claim")
                .endPersistentVolumeClaim()
                .build();

        podSpec.getContainers().get(0).getVolumeMounts().add(mount);
        podSpec.getVolumes().add(historyVolume);
        podSpec.getInitContainers().add(chmod);

        return chmod;
    }

    private static Container createConfigOverrideContainer(SparkCluster cluster, PodSpec podSpec, boolean cmExists) {
        final List<io.radanalytics.types.SparkConfiguration> config = cluster.getSparkConfiguration();
        String cmMountName = "configmap-dir";
        String cmMountPath = "/tmp/config/fromCM";
        List<VolumeMount> mounts = new ArrayList<>(2);
        if (cmExists) {
            String cmName = getExpectedCMName(cluster);

            final VolumeMount cmMount = new VolumeMountBuilder().withName(cmMountName).withMountPath(cmMountPath).build();
            final Volume cmVolume = new VolumeBuilder().withName(cmMountName).withNewConfigMap().withName(cmName).endConfigMap().build();
            podSpec.getVolumes().add(cmVolume);
            mounts.add(cmMount);
        }

        final VolumeMount backupMount = new VolumeMountBuilder().withName(NEW_CONF_DIR).withMountPath(NEW_CONF_DIR_PATH).build();
        final String origConfMountName = "conf-orig-dir";
        final VolumeMount origConfMount = new VolumeMountBuilder().withName(origConfMountName).withMountPath(getSparkHome(cluster) + "/conf/").build();
        final Volume origConfVolume = new VolumeBuilder().withName(origConfMountName).withNewEmptyDir().endEmptyDir().build();
        mounts.add(backupMount);
        mounts.add(origConfMount);

        final StringBuilder overrideConfigCmd = new StringBuilder();
        if (cmExists) {
            // add/replace the content of NEW_CONF_DIR_PATH with all the files that comes from the config map
            overrideConfigCmd.append("cp -r ");
            overrideConfigCmd.append(cmMountPath);
            overrideConfigCmd.append("/..data/* ");
            overrideConfigCmd.append(NEW_CONF_DIR_PATH);
            if (!config.isEmpty()) {
                overrideConfigCmd.append(" ; ");
            }
        }

        if (!config.isEmpty()) {
            // override the key-value entries in the spark-defaults.conf
            overrideConfigCmd.append("echo -e \"");
            config.forEach(kv -> {
                overrideConfigCmd.append(kv.getName());
                overrideConfigCmd.append(" ");
                overrideConfigCmd.append(kv.getValue());
                overrideConfigCmd.append("\\n");
            });
            overrideConfigCmd.append("\" >> ");
            overrideConfigCmd.append(NEW_CONF_DIR_PATH);
            overrideConfigCmd.append("/spark-defaults.conf");
        }

        // replace the content of $SPARK_HOME/conf with the newly created config files
        overrideConfigCmd.append(" && cp -r ");
        overrideConfigCmd.append(NEW_CONF_DIR_PATH);
        overrideConfigCmd.append("/* ");
        overrideConfigCmd.append(getSparkHome(cluster) + "/conf/");

        Container overrideConfig = new ContainerBuilder()
                .withName("override-config")
                .withImage("busybox")
                .withImagePullPolicy("IfNotPresent")
                .withCommand("/bin/sh", "-xc")
                .withArgs(overrideConfigCmd.toString())
                .withVolumeMounts(mounts)
                .build();

        podSpec.getInitContainers().add(overrideConfig);
        podSpec.getContainers().get(0).getVolumeMounts().add(origConfMount);
        podSpec.getVolumes().add(origConfVolume);

        return overrideConfig;
    }

    private static String getSparkHome(SparkCluster cluster) {
        Predicate<Env> p = nv -> "SPARK_HOME".equals(nv.getName());
        if (!cluster.getEnv().isEmpty() && cluster.getEnv().stream().anyMatch(p)) {
            Optional<Env> sparkHome = cluster.getEnv().stream().filter(p).findFirst();
            if (sparkHome.isPresent() && null != sparkHome.get().getValue() && !sparkHome.get().getValue().trim().isEmpty()) {
                return sparkHome.get().getValue();
            }
        }
        return DEFAULT_SPARK_HOME_PATH;
    }

    /**
     * Returns the <code>cluster.getSparkConfigurationMap()</code> if it's there. If not, it defaults to the
     * name of the spark cluster concatenated with <code>'-config'</code> suffix.
     *
     * @param cluster SparkCluster instance
     * @return expected name of the config map
     */
    public static String getExpectedCMName(SparkCluster cluster) {
        return cluster.getSparkConfigurationMap() == null ? cluster.getName() + "-config" : cluster.getSparkConfigurationMap();
    }

    /**
     * Calculates the expected initial delay for the liveness or readiness probe for master or worker
     * it considers these attributes:
     * <ul>
     *  <li>if there is anything to download</li>
     *  <li>if config map with overrides exists</li>
     *  <li>if key-value config entries were passed in the custom resource/CM</li>
     *  <li>if there are additional maven dependencies that should be downloaded</li>
     *  <li>cpu limit</li>
     * </ul>
     *
     * Also worker node gets some minor penalisation, because its probe depends on the master's readiness.
     *
     * @param cluster SparkCluster instance
     * @param cmExists if config map with overrides exists
     * @param isMaster whether it is master or worker
     * @return expected time for initial delay for the probes
     */
    public static int getExpectedDelay(SparkCluster cluster, boolean cmExists, boolean isMaster) {
        // todo: honor the chmod init cont.
        int delay = 6;
        delay += cmExists ? 3 : 0;
        delay += !cluster.getSparkConfiguration().isEmpty() ? 3 : 0;
        delay += cluster.getDownloadData().size() * 4;
        delay += cluster.getMavenDependencies().isEmpty() ? 0 : 42;
        delay += cluster.getMavenDependencies().size() * 5;
        if (isMaster) {
            if (null != cluster.getMaster() && null != cluster.getMaster().getCpu()) {
                try {
                    double cpu = Double.parseDouble(cluster.getMaster().getCpu());
                    delay *= Math.max(.75 / cpu, .95);
                } catch (NumberFormatException nfe) {
                    // ignore
                }
            }
            delay += 0;
        } else {
            if (null != cluster.getWorker() && null != cluster.getWorker().getCpu()) {
                try {
                    double cpu = Double.parseDouble(cluster.getWorker().getCpu());
                    delay *= Math.max(.75 / cpu, .95);
                } catch (NumberFormatException nfe) {
                    // ignore
                }
            }
            delay += 5;
        }
        return delay;
    }

}
