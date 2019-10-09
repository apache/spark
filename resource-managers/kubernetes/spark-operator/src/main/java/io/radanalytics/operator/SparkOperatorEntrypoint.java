package io.radanalytics.operator;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.api.model.extensions.HTTPIngressPathBuilder;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.extensions.IngressBuilder;
import io.fabric8.kubernetes.api.model.extensions.IngressRuleBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.radanalytics.operator.common.AnsiColors;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

@ApplicationScoped
public class SparkOperatorEntrypoint{
    @Inject
    private Logger log;

    @Inject
    private SDKEntrypoint entrypoint;

    public void onStart(@Observes StartupEvent event) {
        log.info("onStart..");
        try {
            exposeMetrics();
        } catch (Exception e) {
            // ignore, not critical (service may have been already exposed)
            log.warn("Unable to expose the metrics, cause: {}", e.getMessage());
        }
    }

    void onStop(@Observes ShutdownEvent event) {
        // nothing special
    }

    private void exposeMetrics() {
        if (entrypoint.getConfig() != null && entrypoint.getConfig().isMetrics()) {
            List<HasMetadata> resources = new ArrayList<>();
            KubernetesClient client = entrypoint.getClient();
            Service svc = createServiceForMetrics();
            resources.add(svc);
            if (entrypoint.isOpenShift()) {
                client = new DefaultOpenShiftClient();
                Route route = createRouteForMetrics();
                resources.add(route);
            } else {
                Ingress ingress = createIngressForMetrics();
                resources.add(ingress);
            }
            KubernetesList k8sResources = new KubernetesListBuilder().withItems(resources).build();
            client.resourceList(k8sResources).inNamespace(client.getNamespace()).createOrReplace();

            if (entrypoint.isOpenShift()) {
                ScheduledExecutorService s = Executors.newScheduledThreadPool(1);
                int delay = 6;
                ScheduledFuture<?> future =
                        s.schedule(() -> {
                            try {
                                List<Route> routes = new DefaultOpenShiftClient().routes().withLabels(Collections.singletonMap("type", "operator-metrics")).list().getItems();
                                if (!routes.isEmpty()) {
                                    Route metrics = routes.iterator().next();
                                    String host = metrics.getSpec().getHost();
                                    log.info("Metrics for the Spark Operator are available at {} http://{} {}", AnsiColors.ye(), host, AnsiColors.xx());
                                }
                            } catch (Throwable t) {
                                log.warn("error during route retrieval: {}", t.getMessage());
                                t.printStackTrace();
                            }
                        }, delay, SECONDS);
            }
        }

    }

    private Ingress createIngressForMetrics() {
            Ingress ingress = new IngressBuilder().withNewMetadata().withName("spark-operator-metrics")
                    .withLabels(Collections.singletonMap("type", "operator-metrics")).endMetadata()
                    .withNewSpec().withRules(new IngressRuleBuilder().withNewHttp()
                            .withPaths(new HTTPIngressPathBuilder().withNewBackend().withServiceName("spark-operator-metrics")
                                    .withNewServicePort(entrypoint.getConfig().getMetricsPort()).endBackend().build()).endHttp().build())
                    .endSpec().build();
            return ingress;
    }


    private Route createRouteForMetrics() {
            Route route = new RouteBuilder().withNewMetadata().withName("spark-operator-metrics")
                    .withLabels(Collections.singletonMap("type", "operator-metrics")).endMetadata()
                    .withNewSpec()
                    .withNewTo("Service", "spark-operator-metrics", 100)
                    .endSpec().build();
            return route;
    }

    private Service createServiceForMetrics() {
        Service svc = entrypoint.getClient().services().createNew().withNewMetadata().withName("spark-operator-metrics")
                .withLabels(Collections.singletonMap("type", "operator-metrics"))
                .endMetadata().withNewSpec()
                .withSelector(Collections.singletonMap("app.kubernetes.io/name", "spark-operator"))
                .withPorts(new ServicePortBuilder().withPort(entrypoint.getConfig().getMetricsPort())
                        .withNewTargetPort().withIntVal(entrypoint.getConfig().getMetricsPort()).endTargetPort()
                        .withProtocol("TCP").build())
                .endSpec().done();
        return svc;
    }

}
