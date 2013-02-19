package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.xml.{XML,NodeSeq}

/**
 * Run PageRank on XML Wikipedia dumps from http://wiki.freebase.com/wiki/WEX. Uses the "articles"
 * files from there, which contains one line per wiki article in a tab-separated format
 * (http://wiki.freebase.com/wiki/WEX/Documentation#articles).
 */
object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WikipediaPageRank <inputFile> <threshold> <numPartitions> <host> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", classOf[PRKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numPartitions = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean
    val sc = new SparkContext(host, "WikipediaPageRank")

    // Parse the Wikipedia page data into a graph
    val input = sc.textFile(inputFile)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices.")

    println("Parsing input file...")
    var vertices = input.map(line => {
      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val links =
        if (body == "\\N")
          NodeSeq.Empty
        else
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \""+title+"\" has malformed XML in body:\n"+body)
            NodeSeq.Empty
          }
      val outEdges = links.map(link => new String(link.text)).toArray
      val id = new String(title)
      (id, new PRVertex(1.0 / numVertices, outEdges))
    })
    if (usePartitioner)
      vertices = vertices.partitionBy(new HashPartitioner(sc.defaultParallelism)).cache
    else
      vertices = vertices.cache
    println("Done parsing input file.")

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(Array[(String, PRMessage)]())
    val utils = new PageRankUtils
    val result =
        Bagel.run(
          sc, vertices, messages, combiner = new PRCombiner(),
          numPartitions = numPartitions)(
          utils.computeWithCombiner(numVertices, epsilon))

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top =
      (result
       .filter { case (id, vertex) => vertex.value >= threshold }
       .map { case (id, vertex) => "%s\t%s\n".format(id, vertex.value) }
       .collect.mkString)
    println(top)
  }
}
