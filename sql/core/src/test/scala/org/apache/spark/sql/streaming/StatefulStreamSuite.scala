package org.apache.spark.sql.streaming

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.util.StreamManualClock

class StatefulStreamSuite extends StreamTest {

  import testImplicits._
  implicit val keyEncoder: ExpressionEncoder[String] = ExpressionEncoder[String]

  test("SPARK-39632: generate the expected state during a single batch execution") {
    val stream = MemoryStream[Transaction]
    val ds =  strategy(stream.toDS())

    testStream(ds)(
      StartStream(Trigger.ProcessingTime(10L), new StreamManualClock),
      AddData(stream, Seq(Transaction("user1", 1000, "car"), Transaction("user1", 50, "shoes"), Transaction("user1", 100, "book")): _*),
      AdvanceManualClock(10L),
      CheckNonEmptyStateAnswer("user1", UserStats(1150, Seq("car", "shoes", "book"))),
      CheckAnswer(UserReport("user1", 1150, Seq("car", "shoes", "book")))
    )
  }

  test("SPARK-39632: generate the expected states on the first batch execution and update them on the second batch") {
    val stream = MemoryStream[Transaction]
    val ds = strategy(stream.toDS())

    testStream(ds)(
      StartStream(Trigger.ProcessingTime(10L), new StreamManualClock),
      AddData(stream, Seq(Transaction("user1", 1000, "car"), Transaction("user1", 50, "shoes"), Transaction("user2", 500, "pool")): _*),
      AdvanceManualClock(10L),
      CheckNonEmptyStateAnswer("user1", UserStats(1050, Seq("car", "shoes"))),
      CheckNonEmptyStateAnswer("user2", UserStats(500, Seq("pool"))),
      AddData(stream, Seq(Transaction("user1", 200, "ball"), Transaction("user2", 5000, "house")): _*),
      AdvanceManualClock(10L),
      CheckNonEmptyStateAnswer("user1", UserStats(1250, Seq("car", "shoes", "ball"))),
      CheckNonEmptyStateAnswer("user2", UserStats(5500, Seq("pool", "house"))),
      CheckAnswer(Seq(
        UserReport("user1", 1050, Seq("car", "shoes")),
        UserReport("user2", 500, Seq("pool")),
        UserReport("user1", 1250, Seq("car", "shoes", "ball")),
        UserReport("user2", 5500, Seq("pool", "house"))
      ): _*),
    )
  }

  test("SPARK-39632: generate the expected state on the first batch execution. On the second batch execution, the state will expire due to state timeout") {
    val stream = MemoryStream[Transaction]
    val ds =  strategy(stream.toDS())

    testStream(ds)(
      StartStream(Trigger.ProcessingTime(10L), new StreamManualClock),
      AddData(stream, Seq(Transaction("user1", 1000, "car"), Transaction("user1", 50, "shoes")): _*),
      AdvanceManualClock(10L),
      CheckNonEmptyStateAnswer("user1", UserStats(1050, Seq("car","shoes"))),
      AddData(stream, Seq(Transaction("user2", 5000, "house")): _*),
      AdvanceManualClock(1010L), // wait for the state timeout
      CheckEmptyStateAnswer("user1"), // expired
      CheckNonEmptyStateAnswer("user2", UserStats(5000, Seq("house"))),
      CheckAnswer(Seq(
        UserReport("user1", 1050, Seq("car","shoes")),
        UserReport("user2", 5000, Seq("house"))
      ): _*)
    )
  }

  def strategy(df: Dataset[Transaction]): Dataset[UserReport] =
    df
      .groupByKey(_.userId)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout())((key: String,
                                                                                               rows: Iterator[Transaction],
                                                                                               state: GroupState[UserStats]) =>
        if (state.hasTimedOut) {
          state.remove()
          Iterator.empty
        } else {
          state.setTimeoutDuration(1000L)
          val newState =
            if (state.exists) {
              val oldStats = state.get
              rows.foldLeft(oldStats)((stats, transaction) => UserStats(stats.totalSpent + transaction.amount, stats.products :+ transaction.product))
            } else {
              rows.foldLeft(UserStats(0, Seq.empty))((stats, transaction) => UserStats(stats.totalSpent + transaction.amount, stats.products :+ transaction.product))
            }
          state.update(newState)
          Iterator(UserReport(key, newState.totalSpent, newState.products))
        }
      )

}

case class Transaction(userId: String, amount: Int, product: String) extends Product
case class UserStats(totalSpent: Int, products: Seq[String]) extends Product
case class UserReport(userId: String, totalSpent: Int, products: Seq[String])