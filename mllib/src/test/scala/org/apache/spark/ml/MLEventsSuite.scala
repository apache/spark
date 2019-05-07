/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml

import scala.collection.mutable
import scala.concurrent.duration._

import org.apache.hadoop.fs.Path
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.mockito.MockitoSugar.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter, MLWriter}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql._
import org.apache.spark.util.JsonProtocol


class MLEventsSuite
  extends SparkFunSuite with BeforeAndAfterEach with MLlibTestSparkContext with Eventually {

  private val events = mutable.ArrayBuffer.empty[MLEvent]
  private val listener: SparkListener = new SparkListener {
    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: MLEvent => events.append(e)
      case _ =>
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.addSparkListener(listener)
  }

  override def afterEach(): Unit = {
    try {
      events.clear()
    } finally {
      super.afterEach()
    }
  }

  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.sparkContext.removeSparkListener(listener)
      }
    } finally {
      super.afterAll()
    }
  }

  abstract class MyModel extends Model[MyModel]

  test("pipeline fit events") {
    val estimator1 = mock[Estimator[MyModel]]
    val model1 = mock[MyModel]
    val transformer1 = mock[Transformer]
    val estimator2 = mock[Estimator[MyModel]]
    val model2 = mock[MyModel]

    when(estimator1.copy(any[ParamMap])).thenReturn(estimator1)
    when(model1.copy(any[ParamMap])).thenReturn(model1)
    when(transformer1.copy(any[ParamMap])).thenReturn(transformer1)
    when(estimator2.copy(any[ParamMap])).thenReturn(estimator2)
    when(model2.copy(any[ParamMap])).thenReturn(model2)

    val dataset1 = mock[DataFrame]
    val dataset2 = mock[DataFrame]
    val dataset3 = mock[DataFrame]
    val dataset4 = mock[DataFrame]
    val dataset5 = mock[DataFrame]

    when(dataset1.toDF).thenReturn(dataset1)
    when(dataset2.toDF).thenReturn(dataset2)
    when(dataset3.toDF).thenReturn(dataset3)
    when(dataset4.toDF).thenReturn(dataset4)
    when(dataset5.toDF).thenReturn(dataset5)

    when(estimator1.fit(meq(dataset1))).thenReturn(model1)
    when(model1.transform(meq(dataset1))).thenReturn(dataset2)
    when(model1.parent).thenReturn(estimator1)
    when(transformer1.transform(meq(dataset2))).thenReturn(dataset3)
    when(estimator2.fit(meq(dataset3))).thenReturn(model2)

    val pipeline = new Pipeline()
      .setStages(Array(estimator1, transformer1, estimator2))
    assert(events.isEmpty)
    val pipelineModel = pipeline.fit(dataset1)

    val event0 = FitStart[PipelineModel]()
    event0.estimator = pipeline
    event0.dataset = dataset1
    val event1 = FitStart[MyModel]()
    event1.estimator = estimator1
    event1.dataset = dataset1
    val event2 = FitEnd[MyModel]()
    event2.estimator = estimator1
    event2.model = model1
    val event3 = TransformStart()
    event3.transformer = model1
    event3.input = dataset1
    val event4 = TransformEnd()
    event4.transformer = model1
    event4.output = dataset2
    val event5 = TransformStart()
    event5.transformer = transformer1
    event5.input = dataset2
    val event6 = TransformEnd()
    event6.transformer = transformer1
    event6.output = dataset3
    val event7 = FitStart[MyModel]()
    event7.estimator = estimator2
    event7.dataset = dataset3
    val event8 = FitEnd[MyModel]()
    event8.estimator = estimator2
    event8.model = model2
    val event9 = FitEnd[PipelineModel]()
    event9.estimator = pipeline
    event9.model = pipelineModel

    val expected = Seq(
      event0, event1, event2, event3, event4, event5, event6, event7, event8, event9)
    eventually(timeout(10.seconds), interval(1.second)) {
      assert(events === expected)
    }
    // Test if they can be ser/de via JSON protocol.
    assert(events.nonEmpty)
    events.map(JsonProtocol.sparkEventToJson).foreach { event =>
      assert(JsonProtocol.sparkEventFromJson(event).isInstanceOf[MLEvent])
    }
  }

  test("pipeline model transform events") {
    val dataset1 = mock[DataFrame]
    val dataset2 = mock[DataFrame]
    val dataset3 = mock[DataFrame]
    val dataset4 = mock[DataFrame]
    when(dataset1.toDF).thenReturn(dataset1)
    when(dataset2.toDF).thenReturn(dataset2)
    when(dataset3.toDF).thenReturn(dataset3)
    when(dataset4.toDF).thenReturn(dataset4)

    val transformer1 = mock[Transformer]
    val model = mock[MyModel]
    val transformer2 = mock[Transformer]
    when(transformer1.transform(meq(dataset1))).thenReturn(dataset2)
    when(model.transform(meq(dataset2))).thenReturn(dataset3)
    when(transformer2.transform(meq(dataset3))).thenReturn(dataset4)

    val newPipelineModel = new PipelineModel(
      "pipeline0", Array(transformer1, model, transformer2))
    assert(events.isEmpty)
    val output = newPipelineModel.transform(dataset1)

    val event0 = TransformStart()
    event0.transformer = newPipelineModel
    event0.input = dataset1
    val event1 = TransformStart()
    event1.transformer = transformer1
    event1.input = dataset1
    val event2 = TransformEnd()
    event2.transformer = transformer1
    event2.output = dataset2
    val event3 = TransformStart()
    event3.transformer = model
    event3.input = dataset2
    val event4 = TransformEnd()
    event4.transformer = model
    event4.output = dataset3
    val event5 = TransformStart()
    event5.transformer = transformer2
    event5.input = dataset3
    val event6 = TransformEnd()
    event6.transformer = transformer2
    event6.output = dataset4
    val event7 = TransformEnd()
    event7.transformer = newPipelineModel
    event7.output = output

    val expected = Seq(event0, event1, event2, event3, event4, event5, event6, event7)
    eventually(timeout(10.seconds), interval(1.second)) {
      assert(events === expected)
    }
    // Test if they can be ser/de via JSON protocol.
    assert(events.nonEmpty)
    events.map(JsonProtocol.sparkEventToJson).foreach { event =>
      assert(JsonProtocol.sparkEventFromJson(event).isInstanceOf[MLEvent])
    }
  }

  test("pipeline read/write events") {
    def getInstance(w: MLWriter): AnyRef =
      w.getClass.getDeclaredMethod("instance").invoke(w)

    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath, "pipeline").toUri.toString
      val writableStage = new WritableStage("writableStage")
      val newPipeline = new Pipeline().setStages(Array(writableStage))
      val pipelineWriter = newPipeline.write
      assert(events.isEmpty)
      pipelineWriter.save(path)
      eventually(timeout(10.seconds), interval(1.second)) {
        events.foreach {
          case e: SaveInstanceStart if e.writer.isInstanceOf[DefaultParamsWriter] =>
            assert(e.path.endsWith("writableStage"))
          case e: SaveInstanceEnd if e.writer.isInstanceOf[DefaultParamsWriter] =>
            assert(e.path.endsWith("writableStage"))
          case e: SaveInstanceStart if getInstance(e.writer).isInstanceOf[Pipeline] =>
            assert(getInstance(e.writer).asInstanceOf[Pipeline].uid === newPipeline.uid)
          case e: SaveInstanceEnd if getInstance(e.writer).isInstanceOf[Pipeline] =>
            assert(getInstance(e.writer).asInstanceOf[Pipeline].uid === newPipeline.uid)
          case e => fail(s"Unexpected event thrown: $e")
        }
      }
      // Test if they can be ser/de via JSON protocol.
      assert(events.nonEmpty)
      events.map(JsonProtocol.sparkEventToJson).foreach { event =>
        assert(JsonProtocol.sparkEventFromJson(event).isInstanceOf[MLEvent])
      }
      sc.listenerBus.waitUntilEmpty(timeoutMillis = 10000)

      events.clear()
      val pipelineReader = Pipeline.read
      assert(events.isEmpty)
      pipelineReader.load(path)
      eventually(timeout(10.seconds), interval(1.second)) {
        events.foreach {
          case e: LoadInstanceStart[_]
              if e.reader.isInstanceOf[DefaultParamsReader[_]] =>
            assert(e.path.endsWith("writableStage"))
          case e: LoadInstanceEnd[_]
              if e.reader.isInstanceOf[DefaultParamsReader[_]] =>
            assert(e.instance.isInstanceOf[PipelineStage])
          case e: LoadInstanceStart[_] =>
            assert(e.reader === pipelineReader)
          case e: LoadInstanceEnd[_] =>
            assert(e.instance.asInstanceOf[Pipeline].uid === newPipeline.uid)
          case e => fail(s"Unexpected event thrown: $e")
        }
      }
      // Test if they can be ser/de via JSON protocol.
      assert(events.nonEmpty)
      events.map(JsonProtocol.sparkEventToJson).foreach { event =>
        assert(JsonProtocol.sparkEventFromJson(event).isInstanceOf[MLEvent])
      }
    }
  }

  test("pipeline model read/write events") {
    def getInstance(w: MLWriter): AnyRef =
      w.getClass.getDeclaredMethod("instance").invoke(w)

    withTempDir { dir =>
      val path = new Path(dir.getCanonicalPath, "pipeline").toUri.toString
      val writableStage = new WritableStage("writableStage")
      val pipelineModel =
        new PipelineModel("pipeline_89329329", Array(writableStage.asInstanceOf[Transformer]))
      val pipelineWriter = pipelineModel.write
      assert(events.isEmpty)
      pipelineWriter.save(path)
      eventually(timeout(10.seconds), interval(1.second)) {
        events.foreach {
          case e: SaveInstanceStart if e.writer.isInstanceOf[DefaultParamsWriter] =>
            assert(e.path.endsWith("writableStage"))
          case e: SaveInstanceEnd if e.writer.isInstanceOf[DefaultParamsWriter] =>
            assert(e.path.endsWith("writableStage"))
          case e: SaveInstanceStart if getInstance(e.writer).isInstanceOf[PipelineModel] =>
            assert(getInstance(e.writer).asInstanceOf[PipelineModel].uid === pipelineModel.uid)
          case e: SaveInstanceEnd if getInstance(e.writer).isInstanceOf[PipelineModel] =>
            assert(getInstance(e.writer).asInstanceOf[PipelineModel].uid === pipelineModel.uid)
          case e => fail(s"Unexpected event thrown: $e")
        }
      }
      // Test if they can be ser/de via JSON protocol.
      assert(events.nonEmpty)
      events.map(JsonProtocol.sparkEventToJson).foreach { event =>
        assert(JsonProtocol.sparkEventFromJson(event).isInstanceOf[MLEvent])
      }
      sc.listenerBus.waitUntilEmpty(timeoutMillis = 10000)

      events.clear()
      val pipelineModelReader = PipelineModel.read
      assert(events.isEmpty)
      pipelineModelReader.load(path)
      eventually(timeout(10.seconds), interval(1.second)) {
        events.foreach {
          case e: LoadInstanceStart[_]
            if e.reader.isInstanceOf[DefaultParamsReader[_]] =>
            assert(e.path.endsWith("writableStage"))
          case e: LoadInstanceEnd[_]
            if e.reader.isInstanceOf[DefaultParamsReader[_]] =>
            assert(e.instance.isInstanceOf[PipelineStage])
          case e: LoadInstanceStart[_] =>
            assert(e.reader === pipelineModelReader)
          case e: LoadInstanceEnd[_] =>
            assert(e.instance.asInstanceOf[PipelineModel].uid === pipelineModel.uid)
          case e => fail(s"Unexpected event thrown: $e")
        }
      }
      // Test if they can be ser/de via JSON protocol.
      assert(events.nonEmpty)
      events.map(JsonProtocol.sparkEventToJson).foreach { event =>
        assert(JsonProtocol.sparkEventFromJson(event).isInstanceOf[MLEvent])
      }
    }
  }
}
