import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClient, Watch, Watcher}
import io.grpc.stub.StreamObserver
import io.grpc.StatusRuntimeException
import io.grpc.Status

class PodEventServiceSpec extends AnyFlatSpec with MockitoSugar {
  "PodEventService" should "enqueue pod events" in {
    val client = mock[KubernetesClient]
    val watch = mock[Watch]
    val pod = new Pod()
    when(client.pods().watch(any[Watcher[Pod]])).thenReturn(watch)

    val service = new PodEventService(client)
    service.watcher.eventReceived(Watcher.Action.ADDED, pod)

    assert(service.podEvents.size == 1)
    assert(service.podEvents.dequeue() == pod)
  }

  it should "handle getPodEvents requests" in {
    val client = mock[KubernetesClient]
    val watch = mock[Watch]
    val pod = new Pod()
    when(client.pods().watch(any[Watcher[Pod]])).thenReturn(watch)

    val service = new PodEventService(client)
    service.podEvents.enqueue(pod)

    val request = GetPodEventsRequest.newBuilder().build()
    val responseObserver = mock[StreamObserver[GetPodEventsResponse]]
    service.getPodEvents(request, responseObserver)

    verify(responseObserver).onNext(any[GetPodEventsResponse])
    verify(responseObserver).onCompleted()
  }

  it should "handle exceptions in getPodEvents" in {
    val client = mock[KubernetesClient]
    val watch = mock[Watch]
    when(client.pods().watch(any[Watcher[Pod]])).thenReturn(watch)

    val service = new PodEventService(client)

    val request = GetPodEventsRequest.newBuilder().build()
    val responseObserver = mock[StreamObserver[GetPodEventsResponse]]
    service.getPodEvents(request, responseObserver)

    verify(responseObserver).onError(any[StatusRuntimeException])
  }
}