package io.radanalytics.operator.historyServer;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.radanalytics.operator.common.AbstractOperator;
import io.radanalytics.operator.common.Operator;
import io.radanalytics.types.SparkHistoryServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;

@Singleton
@Operator(forKind = SparkHistoryServer.class, prefix = "radanalytics.io")
public class HistoryServerOperator extends AbstractOperator<SparkHistoryServer> {

    @Inject
    private Logger log;
    private KubernetesHistoryServerDeployer deployer;
    private boolean osClient = false;
    private Map<String, KubernetesResourceList> cache = new WeakHashMap<>();

    public HistoryServerOperator() {

    }

    @Override
    protected void onInit() {
        this.deployer = new KubernetesHistoryServerDeployer(entityName, prefix);
    }

    @Override
    protected void onAdd(SparkHistoryServer hs) {
        log.info("Spark history server added");

        KubernetesResourceList list = deployer.getResourceList(hs, namespace, isOpenshift);
        if (isOpenshift && hs.getExpose() && !osClient) {

            // we will create openshift specific resource (Route)
            this.client = new DefaultOpenShiftClient();
            osClient = true;
        }
        client.resourceList(list).inNamespace(namespace).createOrReplace();
        cache.put(hs.getName(), list);
    }

    @Override
    protected void onDelete(SparkHistoryServer hs) {
        log.info("Spark history server removed");
        String name = hs.getName();
        KubernetesResourceList list = Optional.ofNullable(cache.get(name)).orElse(deployer.getResourceList(hs, namespace, isOpenshift));
        client.resourceList(list).inNamespace(namespace).delete();
        cache.remove(name);
    }
}