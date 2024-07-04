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
package org.apache.spark.ml.feature

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.Row
import org.apache.spark.util.ArrayImplicits._

class CountVectorizerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new CountVectorizer)
    ParamsSuite.checkParams(new CountVectorizerModel(Array("empty")))
  }

  private def split(s: String): Seq[String] = s.split("\\s+").toImmutableArraySeq

  test("CountVectorizerModel common cases") {
    val df = Seq(
      (0, split("a b c d"),
        Vectors.sparse(4, Seq((0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)))),
      (1, split("a b b c d  a"),
        Vectors.sparse(4, Seq((0, 2.0), (1, 2.0), (2, 1.0), (3, 1.0)))),
      (2, split("a"), Vectors.sparse(4, Seq((0, 1.0)))),
      (3, split(""), Vectors.sparse(4, Seq())), // empty string
      (4, split("a notInDict d"),
        Vectors.sparse(4, Seq((0, 1.0), (3, 1.0))))  // with words not in vocabulary
    ).toDF("id", "words", "expected")
    val cv = new CountVectorizerModel(Array("a", "b", "c", "d"))
      .setInputCol("words")
      .setOutputCol("features")
    testTransformer[(Int, Seq[String], Vector)](df, cv, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer common cases") {
    val df = Seq(
      (0, split("a b c d e"),
        Vectors.sparse(5, Seq((0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)))),
      (1, split("a a a a a a"), Vectors.sparse(5, Seq((0, 6.0)))),
      (2, split("c c"), Vectors.sparse(5, Seq((2, 2.0)))),
      (3, split("d"), Vectors.sparse(5, Seq((3, 1.0)))),
      (4, split("b b b b b"), Vectors.sparse(5, Seq((1, 5.0))))
    ).toDF("id", "words", "expected")
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
    val cvm = cv.fit(df)
    MLTestingUtils.checkCopyAndUids(cv, cvm)
    assert(cvm.vocabulary.toSet === Set("a", "b", "c", "d", "e"))

    testTransformer[(Int, Seq[String], Vector)](df, cvm, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer vocabSize and minDF") {
    val df = Seq(
      (0, split("a b c d"), Vectors.sparse(2, Seq((0, 1.0), (1, 1.0)))),
      (1, split("a b c"), Vectors.sparse(2, Seq((0, 1.0), (1, 1.0)))),
      (2, split("a b"), Vectors.sparse(2, Seq((0, 1.0), (1, 1.0)))),
      (3, split("a"), Vectors.sparse(2, Seq((0, 1.0))))
    ).toDF("id", "words", "expected")
    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)  // limit vocab size to 3
      .fit(df)
    assert(cvModel.vocabulary === Array("a", "b", "c"))

    // minDF: ignore terms with count less than 3
    val cvModel2 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(3)
      .fit(df)
    assert(cvModel2.vocabulary === Array("a", "b"))

    testTransformer[(Int, Seq[String], Vector)](df, cvModel2, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }

    // minDF: ignore terms with freq < 0.75
    val cvModel3 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(3.0 / df.count())
      .fit(df)
    assert(cvModel3.vocabulary === Array("a", "b"))

    testTransformer[(Int, Seq[String], Vector)](df, cvModel3, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer maxDF") {
    val df = Seq(
      (0, split("a b c d"), Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0)))),
      (1, split("a b c"), Vectors.sparse(3, Seq((0, 1.0), (1, 1.0)))),
      (2, split("a b"), Vectors.sparse(3, Seq((0, 1.0)))),
      (3, split("a"), Vectors.sparse(3, Seq()))
    ).toDF("id", "words", "expected")

    // maxDF: ignore terms with count more than 3
    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMaxDF(3)
      .fit(df)
    assert(cvModel.vocabulary === Array("b", "c", "d"))

    cvModel.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }

    // maxDF: ignore terms with freq > 0.75
    val cvModel2 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMaxDF(0.75)
      .fit(df)
    assert(cvModel2.vocabulary === Array("b", "c", "d"))

    cvModel2.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer using both minDF and maxDF") {
    // Ignore terms with count more than 3 AND less than 2
    val df = Seq(
      (0, split("a b c d"), Vectors.sparse(2, Seq((0, 1.0), (1, 1.0)))),
      (1, split("a b c"), Vectors.sparse(2, Seq((0, 1.0), (1, 1.0)))),
      (2, split("a b"), Vectors.sparse(2, Seq((0, 1.0)))),
      (3, split("a"), Vectors.sparse(2, Seq()))
    ).toDF("id", "words", "expected")

    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(2)
      .setMaxDF(3)
      .fit(df)
    assert(cvModel.vocabulary === Array("b", "c"))

    cvModel.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }

    // Ignore terms with frequency higher than 0.75 AND less than 0.5
    val cvModel2 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(0.5)
      .setMaxDF(0.75)
      .fit(df)
    assert(cvModel2.vocabulary === Array("b", "c"))

    cvModel2.transform(df).select("features", "expected").collect().foreach {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizerModel with minTF count") {
    val df = Seq(
      (0, split("a a a b b c c c d "), Vectors.sparse(4, Seq((0, 3.0), (2, 3.0)))),
      (1, split("c c c c c c"), Vectors.sparse(4, Seq((2, 6.0)))),
      (2, split("a"), Vectors.sparse(4, Seq())),
      (3, split("e e e e e"), Vectors.sparse(4, Seq()))
    ).toDF("id", "words", "expected")

    // minTF: count
    val cv = new CountVectorizerModel(Array("a", "b", "c", "d"))
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(3)
    testTransformer[(Int, Seq[String], Vector)](df, cv, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizerModel with minTF freq") {
    val df = Seq(
      (0, split("a a a b b c c c d "), Vectors.sparse(4, Seq((0, 3.0), (2, 3.0)))),
      (1, split("c c c c c c"), Vectors.sparse(4, Seq((2, 6.0)))),
      (2, split("a"), Vectors.sparse(4, Seq((0, 1.0)))),
      (3, split("e e e e e"), Vectors.sparse(4, Seq()))
    ).toDF("id", "words", "expected")

    // minTF: set frequency
    val cv = new CountVectorizerModel(Array("a", "b", "c", "d"))
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(0.3)
    testTransformer[(Int, Seq[String], Vector)](df, cv, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizerModel and CountVectorizer with binary") {
    val df = Seq(
      (0, split("a a a a b b b b c d"),
      Vectors.sparse(4, Seq((0, 1.0), (1, 1.0), (2, 1.0), (3, 1.0)))),
      (1, split("c c c"), Vectors.sparse(4, Seq((2, 1.0)))),
      (2, split("a"), Vectors.sparse(4, Seq((0, 1.0))))
    ).toDF("id", "words", "expected")

    // CountVectorizer test
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setBinary(true)
      .fit(df)
    testTransformer[(Int, Seq[String], Vector)](df, cv, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }

    // CountVectorizerModel test
    val cv2 = new CountVectorizerModel(cv.vocabulary)
      .setInputCol("words")
      .setOutputCol("features")
      .setBinary(true)
    testTransformer[(Int, Seq[String], Vector)](df, cv2, "features", "expected") {
      case Row(features: Vector, expected: Vector) =>
        assert(features ~== expected absTol 1e-14)
    }
  }

  test("CountVectorizer read/write") {
    val t = new CountVectorizer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMinDF(0.5)
      .setMinTF(3.0)
      .setVocabSize(10)
    testDefaultReadWrite(t)
  }

  test("CountVectorizerModel read/write") {
    val instance = new CountVectorizerModel("myCountVectorizerModel", Array("a", "b", "c"))
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMinTF(3.0)
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.vocabulary === instance.vocabulary)
  }

  test("SPARK-22974: CountVectorModel should attach proper attribute to output column") {
    val df = spark.createDataFrame(Seq(
      (0, 1.0, Array("a", "b", "c")),
      (1, 2.0, Array("a", "b", "b", "c", "a", "d"))
    )).toDF("id", "features1", "words")

    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
      .setInputCol("words")
      .setOutputCol("features2")

    val df1 = cvm.transform(df)
    val interaction = new Interaction().setInputCols(Array("features1", "features2"))
      .setOutputCol("features")
    interaction.transform(df1)
  }

  test("SPARK-32662: Test on empty dataset") {
    val df = Seq[(Int, Array[String])]().toDF("id", "words")
    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .fit(df)
    assert(cvModel.vocabulary.isEmpty === true)
    val ans = cvModel.transform(df).select("features").collect()
    assert(ans.length === 0)
  }

  test("SPARK-32662: Remove requirement for minimum vocabulary size") {
    val df = Seq(
      (0, Array[String]()),
      (1, Array[String]())
    ).toDF("id", "words")
    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .fit(df)
    assert(cvModel.vocabulary.isEmpty === true)
    testTransformer[(Int, Seq[String])](df, cvModel, "features") {
      case Row(features: Vector) =>
        assert(features === Vectors.sparse(0, Seq()))
    }

    val df2 = Seq(
      (0, Array("a", "b", "c")),
      (1, Array("d", "e")),
      (2, Array[String]())
    ).toDF("id", "words")
    val cvModel2 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinDF(2)
      .fit(df2)
    assert(cvModel2.vocabulary.isEmpty === true)
    testTransformer[(Int, Seq[String])](df2, cvModel2, "features") {
      case Row(features: Vector) =>
        assert(features === Vectors.sparse(0, Seq()))
    }

    val df3 = Seq(
      (0, Array("a")),
      (1, Array("a")),
      (2, Array("a"))
    ).toDF("id", "words")
    val cvModel3 = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMaxDF(2)
      .fit(df3)
    assert(cvModel3.vocabulary.isEmpty === true)
    testTransformer[(Int, Seq[String])](df3, cvModel3, "features") {
      case Row(features: Vector) =>
        assert(features === Vectors.sparse(0, Seq()))
    }
  }
}
