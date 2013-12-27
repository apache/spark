package org.apache.spark.streaming

package object twitter {
  implicit def enrichMyStreamingContext(ssc: StreamingContext): StreamingContextWithTwitter = {
    new StreamingContextWithTwitter(ssc)
  }
}
