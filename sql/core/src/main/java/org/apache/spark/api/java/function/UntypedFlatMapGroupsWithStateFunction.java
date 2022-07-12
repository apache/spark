package org.apache.spark.api.java.function;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;

@Experimental
@Evolving
public interface UntypedFlatMapGroupsWithStateFunction extends Serializable {
  Iterator<Row> call(Row key, Iterator<Row> values, GroupState<Row> state) throws Exception;
}
