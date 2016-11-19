package edu.uci.eecs.spectralLDA.textprocessing

import breeze.linalg.{DenseVector, SparseVector, min}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable


object TextProcessor {
  /** Process input text files under specified paths into word count vectors, and vocabulary array
    *
    */
  def processDocuments(sc: SparkContext,
                       path: String,
                       stopwordFile: String,
                       vocabSize: Int)
      : (RDD[(Long, breeze.linalg.SparseVector[Double])], Array[String]) = {
    val textRDD: RDD[(String, String)] = sc.wholeTextFiles(path)

    processDocumentsRDD(textRDD, stopwordFile, vocabSize)
  }

  /** Process text files into word count vectors, and vocabulary array
    *
    * The input text files are in RDD[(String, String)], each row being (filename, content)
    */
  def processDocumentsRDD(textRDD: RDD[(String, String)],
                          stopwordFile: String,
                          vocabSize: Int)
      : (RDD[(Long, breeze.linalg.SparseVector[Double])], Array[String]) = {
    val sc = textRDD.sparkContext

    // Split text into words
    val tokenizer: SimpleTokenizer = new SimpleTokenizer(sc, stopwordFile)

    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex()
      .map {
        case ((filename, text), id) => id -> tokenizer.getWords(text)
      }

    // Counts words: RDD[(word, wordCount)]
    val wordCounts: RDD[(String, Long)] = tokenized.flatMap{ case (_, tokens) => tokens.map(_ -> 1L) }.reduceByKey(_ + _)
    wordCounts.cache()
    val fullVocabSize: Long = wordCounts.count()
    println(s"Full vocabulary size $fullVocabSize")

    // Select vocab
    val (vocab: Map[String, Int], selectedTokenCount: Long) = {
      val tmpSortedWC: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
        // Use all terms
        wordCounts.collect().sortBy(-_._2)
      } else {
        // Sort terms to select vocab
        wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
      }
      (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
    }
    println("selectedTokenCount: " + selectedTokenCount.toString)
    println("vocab.size: " + vocab.size.toString)

    val mydocuments: RDD[(Long, breeze.linalg.SparseVector[Double])] = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc: mutable.HashMap[Int, Int] = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex: Int = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices: Array[Int] = wc.keys.toArray.sorted
      val values: Array[Double] = indices.map(i => wc(i).toDouble)
      val sb: breeze.linalg.SparseVector[Double] = {
        new breeze.linalg.SparseVector[Double](indices, values, vocab.size)
      }
      (id, sb)
    }
    println("successfully got the documents.")
    val vocabarray: Array[String] = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabarray(i) = term }

    (mydocuments, vocabarray)
  }

  def processDocuments_libsvm(sc: SparkContext, path: String, vocabSize: Int)
  : (RDD[(Long, breeze.linalg.SparseVector[Double])], Array[String]) ={
    println(path)
    val mydocuments: RDD[(Long, breeze.linalg.SparseVector[Double])] = loadLibSVMFile2sparseVector(sc, path)
    val vocabsize = mydocuments.map(_._2.length).take(1)(0)
    val vocabarray: Array[String] = (0 until vocabsize).toArray.map(x => x.toString)
    (mydocuments, vocabarray)
  }

  private def loadLibSVMFile2sparseVector(
                                           sc: SparkContext,
                                           path: String,
                                           numFeatures: Int,
                                           minPartitions: Int)
        : RDD[(Long, breeze.linalg.SparseVector[Double])] = {
    val parsed = sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map { line =>
        val items = line.split(' ')
        val label = items.head.toDouble.toLong
        val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
          val indexAndValue = item.split(':')
          val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
        val value = indexAndValue(1).toDouble
          (index, value)
        }.unzip


        (label, indices.toArray, values.toArray)
      }

    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      parsed.map { case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
    }

    parsed.map { case (label, indices, values) =>
      // LabeledPoint(label, Vectors.sparse(d, indices, values))
      val myDoubleZero:Double = 0.0
      val mySparseArray:breeze.collection.mutable.SparseArray[Double] =new  breeze.collection.mutable.SparseArray[Double](indices,values,indices.size,d,myDoubleZero)
      (label, new breeze.linalg.SparseVector[Double](indices, values, d))
    }
  }

  private def loadLibSVMFile2sparseVector(
                                           sc: SparkContext,
                                           path: String,
                                           multiclass: Boolean,
                                           numFeatures: Int,
                                           minPartitions: Int): RDD[(Long, breeze.linalg.SparseVector[Double])] = loadLibSVMFile2sparseVector(sc, path, numFeatures, minPartitions)

  private def loadLibSVMFile2sparseVector(
                                           sc: SparkContext,
                                           path: String,
                                           numFeatures: Int): RDD[(Long, breeze.linalg.SparseVector[Double])] = loadLibSVMFile2sparseVector(sc, path, numFeatures, sc.defaultMinPartitions)

  private def loadLibSVMFile2sparseVector(sc: SparkContext, path: String): RDD[(Long, breeze.linalg.SparseVector[Double])] = loadLibSVMFile2sparseVector(sc, path, -1)

  /** Returns the Inverse Document Frequency
    *
    * @param docs the documents RDD
    * @return     the IDF vector
    */
  def inverseDocumentFrequency(docs: RDD[(Long, SparseVector[Double])], approxCount: Boolean = true)
      : DenseVector[Double] = {
    val numDocs: Double = if (approxCount)
      docs.countApprox(30000L).getFinalValue.mean
    else
      docs.count.toDouble

    val vocabSize: Int = docs.map(_._2.length).take(1)(0)
    val documentFrequency: DenseVector[Double] = DenseVector.zeros[Double](vocabSize)

    docs
      .flatMap[(Int, Double)] {
        case (_, w: SparseVector[Double]) =>
          for ((token, count) <- w.activeIterator)
            yield (token, if (count > 0.0) 1.0 else 0.0)
      }
      .reduceByKey(_ + _)
      .collect
      .foreach {
        case (token, df) => documentFrequency(token) = df
      }

    numDocs / documentFrequency
  }

  /** Filter out terms whose IDF is lower than the given bound
    *
    * @param docs           the documents RDD
    * @param idfLowerBound  lower bound for the IDF
    */
  def filterIDF(docs: RDD[(Long, SparseVector[Double])], idfLowerBound: Double)
      : RDD[(Long, SparseVector[Double])] = {
    if (idfLowerBound > 1.0 + 1e-12) {
      val idf = inverseDocumentFrequency(docs)
      val invalidTermIndices = (idf :< idfLowerBound).toVector
      println(s"Ignoring term IDs $invalidTermIndices")

      docs.map {
        case (id: Long, w: SparseVector[Double]) =>
          w(invalidTermIndices) := 0.0
          (id, w)
      }
    }
    else
      docs
  }
}
