package io.radanalytics.operator.cluster;

import io.radanalytics.types.SparkCluster;
import io.radanalytics.types.Worker;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RunningClusters {

    private final Map<String, SparkCluster> clusters;
    private final String namespace;
    public RunningClusters(String namespace) {
        clusters = new HashMap<>();
        this.namespace = namespace;
        MetricsHelper.runningClusters.labels(namespace).set(0);
    }

    public void put(SparkCluster ci) {
        MetricsHelper.runningClusters.labels(namespace).inc();
        MetricsHelper.startedTotal.labels(namespace).inc();
        MetricsHelper.workers.labels(ci.getName(), namespace).set(Optional.ofNullable(ci.getWorker()).orElse(new Worker()).getInstances());
        clusters.put(ci.getName(), ci);
    }

    public void delete(String name) {
        if (clusters.containsKey(name)) {
            MetricsHelper.runningClusters.labels(namespace).dec();
            MetricsHelper.workers.labels(name, namespace).set(0);
            clusters.remove(name);
        }
    }

    public SparkCluster getCluster(String name) {
        return this.clusters.get(name);
    }

    public void resetMetrics() {
        MetricsHelper.startedTotal.labels(namespace).set(0);
        clusters.forEach((c, foo) -> MetricsHelper.workers.labels(c, namespace).set(0));
        MetricsHelper.startedTotal.labels(namespace).set(0);
    }

}
