package spark

import scala.collection.mutable.Map

// Task result. Also contains updates to accumulator variables.
// TODO: Use of distributed cache to return result is a hack to get around
// what seems to be a bug with messages over 60KB in libprocess; fix it
private class TaskResult[T](val value: T, val accumUpdates: Map[Long, Any]) extends Serializable
