---
layout: global
title: ZeroMQ Stream setup guide
---

## Install ZeroMQ (using JNA)

To work with zeroMQ, some native libraries have to be installed.

* Install zeroMQ (release 2.1) core libraries. [ZeroMQ Install guide](http://www.zeromq.org/intro:get-the-software)

   Typically if you are using ubuntu 12.04, you can do:

    `$ sudo apt-get install libzmq1`

 __To work with akka-zeromq, zmq 2.1 version is supported via [JNA](https://github.com/twall/jna). Incase you want to switch to zeromq 3.0, please install [JZMQ](http://www.zeromq.org/bindings:java) which uses [JNI](http://docs.oracle.com/javase/6/docs/technotes/guides/jni/) and drop in jzmq jar__

## Sample scala code

A publisher is an entity assumed to be outside the spark ecosystem. A sample zeroMQ publisher is provided to try out the sample spark ZeroMQ application.

1. Start the sample publisher.

{% highlight scala %}


      val acs: ActorSystem = ActorSystem()

      val pubSocket = ZeroMQExtension(acs).newSocket(SocketType.Pub, Bind(url))

      pubSocket ! ZMQMessage(Seq(Frame("topic"), Frame("My message".getBytes)))



{% endhighlight %}

A typical zeromq url looks like `tcp://<ip>:<port>`

It does nothing more than publishing the message on the specified topic and url.

2. Start the spark application by plugging the zeroMQ stream receiver.

{% highlight scala %}

    val lines = ssc.zeroMQStream(url, Subscribe(topic), bytesToObjectsIterator)

{% endhighlight %}

bytesToObjectsIterator is going to be a function for decoding the Frame data.

_For example: For decoding into strings using default charset:_


{% highlight scala %}


    def bytesToStringIterator(x: Seq[Seq[Byte]]) = (x.map(x => new String(x.toArray))).iterator

{% endhighlight %}
