---
layout: global
title: Spark Streaming + Kafka Integration Guide
---

[Apache Kafka](https://kafka.apache.org/) is publish-subscribe messaging rethought as a distributed, partitioned, replicated commit log service.  Please read the [Kafka documentation](https://kafka.apache.org/documentation.html) thoroughly before starting an integration using Spark.

The Kafka project introduced a new consumer API between versions 0.8 and 0.10, so there are 2 separate corresponding Spark Streaming packages available.  Please choose the correct package for your brokers and desired features; note that the 0.8 integration is compatible with later 0.9 and 0.10 brokers, but the 0.10 integration is not compatible with earlier brokers.

**Note: Kafka 0.8 support is deprecated as of Spark 2.3.0.**

<table class="table">
<tr><th></th><th><a href="streaming-kafka-0-8-integration.html">spark-streaming-kafka-0-8</a></th><th><a href="streaming-kafka-0-10-integration.html">spark-streaming-kafka-0-10</a></th></tr>
<tr>
  <td>Broker Version</td>
  <td>0.8.2.1 or higher</td>
  <td>0.10.0 or higher</td>
</tr>
<tr>
  <td>API Maturity</td>
  <td>Deprecated</td>
  <td>Stable</td>
</tr>
<tr>
  <td>Language Support</td>
  <td>Scala, Java, Python</td>
  <td>Scala, Java</td>
</tr>
<tr>
  <td>Receiver DStream</td>
  <td>Yes</td>
  <td>No</td>
</tr>
<tr>
  <td>Direct DStream</td>
  <td>Yes</td>
  <td>Yes</td>
</tr>
<tr>
  <td>SSL / TLS Support</td>
  <td>No</td>
  <td>Yes</td>
</tr>
<tr>
  <td>Offset Commit API</td>
  <td>No</td>
  <td>Yes</td>
</tr>
<tr>
  <td>Dynamic Topic Subscription</td>
  <td>No</td>
  <td>Yes</td>
</tr>
</table>
