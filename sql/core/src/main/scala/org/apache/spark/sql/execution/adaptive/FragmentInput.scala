package org.apache.spark.sql.execution.adaptive

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, ShuffledRowRDD, SparkPlan}

/**
 * FragmentInput is the leaf node of parent fragment that connect with an child fragment.
 */
case class FragmentInput(@transient childFragment: QueryFragment) extends LeafExecNode {

  private[this] var optimized: Boolean = false

  private[this] var inputPlan: SparkPlan = null

  private[this] var shuffledRdd: ShuffledRowRDD = null

  override def output: Seq[Attribute] = inputPlan.output

  private[sql] def setOptimized() = {
    this.optimized = true
  }

  private[sql] def isOptimized(): Boolean = this.optimized

  private[sql] def setShuffleRdd(shuffledRdd: ShuffledRowRDD) = {
    this.shuffledRdd = shuffledRdd
  }

  private[sql] def setInputPlan(inputPlan: SparkPlan) = {
    this.inputPlan = inputPlan
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (shuffledRdd != null) {
      shuffledRdd
    } else {
      inputPlan.execute()
    }
  }

  override def simpleString: String = "FragmentInput"

  override def innerChildren: Seq[SparkPlan] = inputPlan :: Nil
}
