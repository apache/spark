package io.radanalytics.operator.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Functions;
import com.google.common.collect.Sets;
import io.fabric8.kubernetes.api.model.DoneableReplicationController;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ReplicationController;
import io.fabric8.kubernetes.api.model.ReplicationControllerList;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListMultiDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.radanalytics.operator.Constants;
import io.radanalytics.operator.common.AbstractOperator;
import io.radanalytics.operator.common.Operator;
import io.radanalytics.types.Master;
import io.radanalytics.types.SparkCluster;
import io.radanalytics.types.Worker;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.radanalytics.operator.common.AnsiColors.*;
import static io.radanalytics.operator.resource.LabelsHelper.OPERATOR_KIND_LABEL;
import static io.radanalytics.operator.resource.LabelsHelper.OPERATOR_RC_TYPE_LABEL;

@Singleton
@Operator(forKind = SparkCluster.class, prefix = "radanalytics.io",
        additionalPrinterColumnNames = {"Workers", "Age"},
        additionalPrinterColumnPaths = {".spec.worker.instances", ".metadata.creationTimestamp"},
        additionalPrinterColumnTypes = {"string", "date"})
public class SparkClusterOperator extends AbstractOperator<SparkCluster> {

    @Inject
    private Logger log;

    private MetricsHelper metrics;
    private RunningClusters clusters;
    private KubernetesSparkClusterDeployer deployer;

    public SparkClusterOperator() {

    }

    @Override
    protected void onInit() {
        log.info("{} operator default spark image = {}", this.entityName, Constants.getDefaultSparkImage());
    }

    @Override
    protected void onAdd(SparkCluster cluster) {
        KubernetesResourceList list = getDeployer().getResourceList(cluster);
        client.resourceList(list).inNamespace(namespace).createOrReplace();
        getClusters().put(cluster);
    }

    @Override
    protected void onDelete(SparkCluster cluster) {
        String name = cluster.getName();
        client.services().inNamespace(namespace).withLabels(getDeployer().getDefaultLabels(name)).delete();
        client.replicationControllers().inNamespace(namespace).withLabels(getDeployer().getDefaultLabels(name)).delete();
        client.pods().inNamespace(namespace).withLabels(getDeployer().getDefaultLabels(name)).delete();
        client.persistentVolumeClaims().inNamespace(namespace).withLabels(getDeployer().getDefaultLabels(name)).delete();
        getClusters().delete(name);
    }

    @Override
    protected void onModify(SparkCluster newCluster) {
        String name = newCluster.getName();

        // if an empty master/worker was passed
        if (null == newCluster.getMaster()) {
            newCluster.setMaster(new Master());
        }
        if (null == newCluster.getWorker()) {
            newCluster.setWorker(new Worker());
        }
        int newWorkers = Optional.ofNullable(newCluster.getWorker()).orElse(new Worker()).getInstances();

        SparkCluster existingCluster = getClusters().getCluster(name);
        if (null == existingCluster) {
            log.error("something went wrong, unable to scale existing cluster. Perhaps it wasn't deployed properly.");
            return;
        }

        if (isOnlyScale(existingCluster, newCluster)) {
            log.info("{}scaling{} from  {}{}{} worker replicas to  {}{}{}", re(), xx(), ye(),
                    existingCluster.getWorker().getInstances(), xx(), ye(), newWorkers, xx());
            client.replicationControllers().inNamespace(namespace).withName(name + "-w").scale(newWorkers);

            // update metrics
            MetricsHelper.workers.labels(newCluster.getName(), namespace).set(newCluster.getWorker().getInstances());
        } else {
            log.info("{}recreating{} cluster  {}{}{}", re(), xx(), ye(), existingCluster.getName(), xx());
            KubernetesResourceList list = getDeployer().getResourceList(newCluster);
            try {
                client.resourceList(list).inNamespace(namespace).createOrReplace();
            } catch (Exception e) {
                log.warn("{}deleting and creating{} cluster  {}{}{}", re(), xx(), ye(), existingCluster.getName(), xx());
                client.resourceList(list).inNamespace(namespace).delete();
                client.resourceList(list).inNamespace(namespace).createOrReplace();
            }
            getClusters().put(newCluster);
        }
    }

    @Override
    public void fullReconciliation() {
//        1. get all the cm/cr and call it desiredSet
//        2. get all the clusters and call it actualSet (and update the this.clusters)
//        3. desiredSet - actualSet = toBeCreated
//        4. actualSet - desiredSet = toBeDeleted
//        5. modify / scale

        if ("*".equals(namespace)) {
            log.info("Skipping full reconciliation for namespace '*' (not supported)");
            return;
        }
        log.info("Running full reconciliation for namespace {} and kind {}..", namespace, entityName);
        final AtomicBoolean change = new AtomicBoolean(false);
        Set<SparkCluster> desiredSet = super.getDesiredSet();
        Map<String, SparkCluster> desiredMap = desiredSet.stream().collect(Collectors.toMap(SparkCluster::getName, Functions.identity()));
        Map<String, Integer> actual = getActual();

        log.debug("desired set: {}", desiredSet);
        log.debug("actual: {}", actual);

        Sets.SetView<String> toBeCreated = Sets.difference(desiredMap.keySet(), actual.keySet());
        Sets.SetView<String> toBeDeleted = Sets.difference(actual.keySet(), desiredMap.keySet());

        if (!toBeCreated.isEmpty()) {
            log.info("toBeCreated: {}", toBeCreated);
            change.set(true);
        }
        if (!toBeDeleted.isEmpty()) {
            log.info("toBeDeleted: {}", toBeDeleted);
            change.set(true);
        }

        // add new
        toBeCreated.forEach(cluster -> {
            log.info("creating cluster {}", cluster);
            onAdd(desiredMap.get(cluster));
        });

        // delete old
        toBeDeleted.forEach(cluster -> {
            SparkCluster c = new SparkCluster();
            c.setName(cluster);
            log.info("deleting cluster {}", cluster);
            onDelete(c);
        });

        // scale
        desiredSet.forEach(dCluster -> {
            int desiredWorkers = Optional.ofNullable(dCluster.getWorker()).orElse(new Worker()).getInstances();
            Integer actualWorkers = actual.get(dCluster.getName());
            if (actualWorkers != null && desiredWorkers != actualWorkers) {
                change.set(true);
                // update the internal representation with the actual # of workers and call onModify
                if (getClusters().getCluster(dCluster.getName()) == null) {
                    // deep copy via json -> room for optimization
                    ObjectMapper om = new ObjectMapper();
                    try {
                        SparkCluster actualCluster = om.readValue(om.writeValueAsString(dCluster), SparkCluster.class);
                        Optional.ofNullable(actualCluster.getWorker()).ifPresent(w -> w.setInstances(actualWorkers));
                        getClusters().put(actualCluster);
                    } catch (IOException e) {
                        log.warn(e.getMessage());
                        e.printStackTrace();
                        return;
                    }
                } else {
                    Optional.ofNullable(getClusters().getCluster(dCluster.getName())).map(SparkCluster::getWorker)
                            .ifPresent(worker -> worker.setInstances(actualWorkers));
                }
                log.info("scaling cluster {}", dCluster.getName());
                onModify(dCluster);
            }
        });

        // first reconciliation after (re)start -> update the clusters instance
        if (!fullReconciliationRun) {
            getClusters().resetMetrics();
            desiredMap.entrySet().forEach(e -> getClusters().put(e.getValue()));
        }

        if (!change.get()) {
            log.info("no change was detected during the reconciliation");
        }
        metrics.reconciliationsTotal.labels(namespace).inc();
    }

    private Map<String, Integer> getActual() {
        MixedOperation<ReplicationController, ReplicationControllerList, DoneableReplicationController, RollableScalableResource<ReplicationController, DoneableReplicationController>> aux1 =
                client.replicationControllers();
        FilterWatchListMultiDeletable<ReplicationController, ReplicationControllerList, Boolean, Watch, Watcher<ReplicationController>> aux2 =
                "*".equals(namespace) ? aux1.inAnyNamespace() : aux1.inNamespace(namespace);
        Map<String, String> labels =new HashMap<>(2);
        labels.put(prefix + OPERATOR_KIND_LABEL, entityName);
        labels.put(prefix + OPERATOR_RC_TYPE_LABEL, "worker");
        List<ReplicationController> workerRcs = aux2.withLabels(labels).list().getItems();
        Map<String, Integer> retMap = workerRcs
                .stream()
                .collect(Collectors.toMap(rc -> rc.getMetadata().getLabels().get(prefix + entityName),
                        rc -> rc.getSpec().getReplicas()));
        return retMap;
    }

    public KubernetesSparkClusterDeployer getDeployer() {
        if (this.deployer == null) {
            this.deployer = new KubernetesSparkClusterDeployer(client, entityName, prefix, namespace);
        }
        return deployer;
    }

    private RunningClusters getClusters() {
        if (null == clusters) {
            clusters = new RunningClusters(namespace);
        }
        return clusters;
    }


    /**
     * This method verifies if any two instances of SparkCluster are the same ones up to the number of
     * workers. This way we can call the scale instead of recreating the whole cluster.
     *
     * @param oldC the first instance of SparkCluster we are comparing
     * @param newC the second instance of SparkCluster we are comparing
     * @return true if both instances represent the same spark cluster but differs only in number of workers (it is safe
     * to call scale method)
     */
    private boolean isOnlyScale(SparkCluster oldC, SparkCluster newC) {
        boolean retVal = oldC.getWorker().getInstances() != newC.getWorker().getInstances();
        int backup = Optional.ofNullable(newC.getWorker()).orElse(new Worker()).getInstances();
        newC.getWorker().setInstances(oldC.getWorker().getInstances());
        retVal &= oldC.equals(newC);
        newC.getWorker().setInstances(backup);
        return retVal;
    }
}
