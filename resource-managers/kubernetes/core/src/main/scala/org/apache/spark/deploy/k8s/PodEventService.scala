import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{DefaultKubernetesClient, Watch, Watcher, KubernetesClientException}
import io.fabric8.kubernetes.client.Watcher.Action
import io.grpc.{Server, ServerBuilder, Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver

import scala.collection.mutable

class PodEventService extends PodEventServiceGrpc.PodEventServiceImplBase {
  private val client = new DefaultKubernetesClient()
  private val podEvents = mutable.Queue[Pod]()

  private val watcher: Watcher[Pod] = new Watcher[Pod] {
    override def eventReceived(action: Action, pod: Pod): Unit = {
      podEvents.enqueue(pod)
    }

    override def onClose(cause: KubernetesClientException): Unit = {
      if (cause != null) {
        // The watcher was closed due to an exception
        println(s"Watcher closed due to exception: ${cause.getMessage}")
        // TODO: Handle the exception, e.g. restart the watcher
      } else {
        // The watcher was closed normally
        println("Watcher closed")
      }
    }
  }

  private val watch: Watch = client.pods().watch(watcher)

  override def getPodEvents(request: GetPodEventsRequest, responseObserver: StreamObserver[GetPodEventsResponse]): Unit = {
    try {
      val events = podEvents.dequeueAll(_ => true)
      val response = GetPodEventsResponse.newBuilder().addAllEvents(events).build()
      responseObserver.onNext(response)
      responseObserver.onCompleted()
    } catch {
      case e: Exception =>
        responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage)))
    }
  }
}

object PodEventService {
  def main(args: Array[String]): Unit = {
    val server: Server = ServerBuilder.forPort(8080)
      .addService(new PodEventService)
      .build
      .start

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = server.shutdown()
    })

    server.awaitTermination()
  }
}