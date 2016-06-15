package org.apache.spark.ml.fpm

import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasPredictionCol}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth => MLlibFPGrowth, FPGrowthModel => MLlibFPGrowthModel}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, StructType}

trait FPGrowthParams extends Params with HasFeaturesCol with HasPredictionCol {

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {

    val inputType = schema($(featuresCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    SchemaUtils.appendColumn(schema, $(predictionCol), new VectorUDT)
  }
}

class FPGrowth @Since("2.1.0") (
    @Since("2.1.0") override val uid: String)
  extends Estimator[FPGrowthModel] with FPGrowthParams with DefaultParamsWritable {

  @Since("2.1.0")
  def this() = this(Identifiable.randomUID("FPGrowth"))

  def fit(dataset: Dataset[_]): FPGrowthModel = {
    val data = dataset.select($(featuresCol)).rdd.map(r => r.getAs[Seq[Any]](0).toArray)
    val parentModel = new MLlibFPGrowth().run(data)
    copyValues(new FPGrowthModel(uid, parentModel))
  }

  @Since("2.1.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): FPGrowth = defaultCopy(extra)
}

class FPGrowthModel private[ml] (
    @Since("2.1.0") override val uid: String,
    private val parentModel: MLlibFPGrowthModel[_])
  extends Model[FPGrowthModel] with FPGrowthParams with MLWritable {

  @Since("2.1.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF()
  }

  @Since("2.1.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

    @Since("1.5.0")
  override def copy(extra: ParamMap): FPGrowthModel = {
    val copied = new FPGrowthModel(uid, parentModel)
    copyValues(copied, extra)
  }

  def getFreqItemsets: DataFrame = {
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._
    parentModel.freqItemsets.map(f => (f.items.map(_.toString), f.freq)).toDF("items", "freq")
  }

  def generateAssociationRules(confidence: Double): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(parentModel.freqItemsets.sparkContext)
    import sqlContext.implicits._
    val associationRules = new AssociationRules().setMinConfidence(confidence)
    associationRules.run(parentModel.freqItemsets)
      .map(r => (r.antecedent.map(_.toString), r.consequent.map(_.toString), r.confidence))
      .toDF("antecedent",  "consequent", "confidence")
  }

  @Since("2.1.0")
  override def write: MLWriter = new FPGrowthModel.FPGrowthModelWriter(this)

}


object FPGrowthModel extends MLReadable[FPGrowthModel] {
  @Since("2.0.0")
  override def read: MLReader[FPGrowthModel] = new FPGrowthModelReader

  @Since("2.0.0")
  override def load(path: String): FPGrowthModel = super.load(path)

  /** [[MLWriter]] instance for [[FPGrowthModel]] */
  private[FPGrowthModel]
  class FPGrowthModelWriter(instance: FPGrowthModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.parentModel.save(sc, dataPath)
    }
  }

  private class FPGrowthModelReader extends MLReader[FPGrowthModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[FPGrowthModel].getName

    override def load(path: String): FPGrowthModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val mllibModel = MLlibFPGrowthModel.load(sc, dataPath)
      val model = new FPGrowthModel(metadata.uid, mllibModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

