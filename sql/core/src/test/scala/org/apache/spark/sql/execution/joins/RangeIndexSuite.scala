package org.apache.spark.sql.execution.joins

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, InterpretedMutableProjection}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.IntegerType

class RangeIndexSuite extends SparkFunSuite {
  private[this] val ordering = TypeUtils.getOrdering(IntegerType)

  private[this] val projection = new InterpretedMutableProjection(Seq(
    BoundReference(0, IntegerType, nullable = false),
    BoundReference(1, IntegerType, nullable = false)))

  private[this] val eventifier = RangeIndex.toRangeEvent(projection, ordering)

  test("RangeIndex Point Query") {
    val r1 = InternalRow(1, 1)
    val r2 = InternalRow(2, 2)
    val r3 = InternalRow(3, 3)
    val r4 = InternalRow(4, 4)
    val r5 = InternalRow(4, 4)
    val rs = Array(r1, r2, r3, r4, r5).flatMap(eventifier)

    // Low Bound Included - High Bound Excluded:
    val index1 = RangeIndex.build(ordering, rs, allowLowEqual = true, allowHighEqual = false)

    // All
    assertResult(Seq(r1, r2, r3, r4, r5))(index1.intersect(null, null).toSeq)

    // Bounds - l <= p && p < h
    assertResult(Nil)(index1.intersect(5, null).toSeq)
    assertResult(r3 :: r4 :: r5 :: Nil)(index1.intersect(3, null).toSeq)
    assertResult(Nil)(index1.intersect(null, 1).toSeq)
    assertResult(r1 :: Nil)(index1.intersect(null, 2).toSeq)

    // Ranges
    assertResult(r1 :: Nil)(index1.intersect(1, 2).toSeq)
    assertResult(r3 :: r4 :: r5 :: Nil)(index1.intersect(3, 5).toSeq)

    // Low Bound Excluded - High Bound Included:
    val index2 = RangeIndex.build(ordering, rs, allowLowEqual = false, allowHighEqual = true)

    // Bounds
    assertResult(r4 :: r5 :: Nil)(index2.intersect(3, null).toSeq)
    assertResult(r1 :: Nil)(index2.intersect(null, 1).toSeq)
    assertResult(r1 :: r2 :: Nil)(index2.intersect(null, 2).toSeq)

    // Ranges
    assertResult(r2 :: Nil)(index2.intersect(1, 2).toSeq)
    assertResult(r4 :: r5 :: Nil)(index2.intersect(3, 5).toSeq)
  }

  test("RangeIndex Interval Query") {
    val r1 = InternalRow(1, 2)
    val r3 = InternalRow(3, 4)
    val r4 = InternalRow(4, 5)
    val r5 = InternalRow(3, 6)
    val rs = Array(r1, r3, r4, r5).flatMap(eventifier)

    // Low Bound Excluded - High Bound Excluded (Normal when intersecting intervals):
    val index1 = RangeIndex.build(ordering, rs, allowLowEqual = false, allowHighEqual = false)

    // All
    assertResult(Seq(r1, r3, r5, r4))(index1.intersect(null, null).toSeq)

    // Bounds
    assertResult(r5 :: Nil)(index1.intersect(5, null).toSeq)
    assertResult(r3 :: r5 :: r4 :: Nil)(index1.intersect(3, null).toSeq)
    assertResult(Nil)(index1.intersect(null, 1).toSeq)
    assertResult(r1 :: Nil)(index1.intersect(null, 2).toSeq)

    // Ranges
    assertResult(r1 :: Nil)(index1.intersect(1, 2).toSeq)
    assertResult(r3 :: r5 :: r4 :: Nil)(index1.intersect(3, 5).toSeq)
    assertResult(r5 :: r4 :: Nil)(index1.intersect(4, 5).toSeq)

    // Points
    assertResult(Nil)(index1.intersect(2, 2).toSeq)
    assertResult(r3 :: r5 :: Nil)(index1.intersect(3, 3).toSeq)

    // Low Bound Included - High Bound Included:
    val index2 = RangeIndex.build(ordering, rs, allowLowEqual = true, allowHighEqual = true)

    // Bounds
    assertResult(r5 :: r4 :: Nil)(index2.intersect(5, null).toSeq)
    assertResult(r1 :: Nil)(index2.intersect(null, 1).toSeq)
    assertResult(r1 :: Nil)(index2.intersect(null, 2).toSeq)

    // Ranges
    assertResult(r1 :: r3 :: r5 :: Nil)(index2.intersect(1, 3).toSeq)
    assertResult(r3 :: r5 :: r4 :: Nil)(index2.intersect(3, 5).toSeq)
    assertResult(r3 :: r5 :: r4 :: Nil)(index2.intersect(4, 5).toSeq)

    // Points
    assertResult(r1 :: Nil)(index2.intersect(2, 2).toSeq)
    assertResult(r3 :: r5 :: Nil)(index2.intersect(3, 3).toSeq)
  }
}
