package org.apache.spark.sql.streaming;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.plans.logical.ProcessingTimeTimeout$;


@Experimental
@InterfaceStability.Evolving
public interface KeyedStateTimeout {

  static KeyedStateTimeout withProcessingTime() {
    return ProcessingTimeTimeout$.MODULE$;
  }
}
