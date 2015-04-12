package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.udf.generic.UDFCurrentDB
import org.apache.hadoop.hive.ql.exec.Description
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.ql.session.SessionState

// deterministic in the query range
@Description(name = "current_database",
    value = "_FUNC_() - returns currently using database name")
class sqlUDFCurrentDB extends UDFCurrentDB {

  // This function just throws an exception in hive0.13
  override def evaluate(arguments: Array[DeferredObject]): Object = {
    SessionState.get().getCurrentDatabase()
  }
}

