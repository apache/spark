---
layout: global
title: Spark Streaming Custom Receivers
---

A "Spark Streaming" receiver can be a simple network stream, streams of messages from a message queue, files etc. A receiver can also assume roles more than just receiving data like filtering, preprocessing, to name a few of the possibilities. The api to plug-in any user defined custom receiver is thus provided to encourage development of receivers which may be well suited to ones specific need.

This guide shows the programming model and features by walking through a simple sample receiver and corresponding Spark Streaming application.

### Writing a Simple Receiver

This starts with implementing [NetworkReceiver](api/streaming/index.html#org.apache.spark.streaming.dstream.NetworkReceiver).

The following is a simple socket text-stream receiver.

{% highlight scala %}
       class SocketTextStreamReceiver(host: String, port: Int(
         extends NetworkReceiver[String]
       {
         protected lazy val blocksGenerator: BlockGenerator =
           new BlockGenerator(StorageLevel.MEMORY_ONLY_SER_2)

         protected def onStart() = {
           blocksGenerator.start()
           val socket = new Socket(host, port)
           val dataInputStream = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
           var data: String = dataInputStream.readLine()
           while (data != null) {
             blocksGenerator += data
             data = dataInputStream.readLine()
           }
         }

         protected def onStop() {
           blocksGenerator.stop()
         }
       }
{% endhighlight %}


All we did here is extended NetworkReceiver and called blockGenerator's API method (i.e. +=) to push our blocks of data. Please refer to scala-docs of NetworkReceiver for more details.


### An Actor as Receiver

This starts with implementing [Actor](#References)

Following is a simple socket text-stream receiver, which is appearently overly simplified using Akka's socket.io api.

{% highlight scala %}
       class SocketTextStreamReceiver (host:String,
         port:Int,
         bytesToString: ByteString => String) extends Actor with Receiver {

          override def preStart = IOManager(context.system).connect(host, port)

          def receive = {
           case IO.Read(socket, bytes) => pushBlock(bytesToString(bytes))
         }

       }
{% endhighlight %}

All we did here is mixed in trait Receiver and called pushBlock api method to push our blocks of data. Please refer to scala-docs of Receiver for more details.

### A Sample Spark Application

* First create a Spark streaming context with master url and batchduration.

{% highlight scala %}
    val ssc = new StreamingContext(master, "WordCountCustomStreamSource",
      Seconds(batchDuration))
{% endhighlight %}

* Plug-in the custom receiver into the spark streaming context and create a DStream.

{% highlight scala %}
    val lines = ssc.networkStream[String](new SocketTextStreamReceiver(
      "localhost", 8445))
{% endhighlight %}

* OR Plug-in the actor as receiver into the spark streaming context and create a DStream.

{% highlight scala %}
    val lines = ssc.actorStream[String](Props(new SocketTextStreamReceiver(
      "localhost",8445, z => z.utf8String)),"SocketReceiver")
{% endhighlight %}

* Process it.

{% highlight scala %}
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
{% endhighlight %}

* After processing it, stream can be tested using the netcat utility.

     $ nc -l localhost 8445
     hello world
     hello hello


## Multiple Homogeneous/Heterogeneous Receivers.

A DStream union operation is provided for taking union on multiple input streams.

{% highlight scala %}
    val lines = ssc.actorStream[String](Props(new SocketTextStreamReceiver(
      "localhost",8445, z => z.utf8String)),"SocketReceiver")

    // Another socket stream receiver
    val lines2 = ssc.actorStream[String](Props(new SocketTextStreamReceiver(
      "localhost",8446, z => z.utf8String)),"SocketReceiver")

    val union = lines.union(lines2)
{% endhighlight %}

Above stream can be easily process as described earlier.

_A more comprehensive example is provided in the spark streaming examples_

## References

1.[Akka Actor documentation](http://doc.akka.io/docs/akka/2.0.5/scala/actors.html)
2.[NetworkReceiver](api/streaming/index.html#org.apache.spark.streaming.dstream.NetworkReceiver)
