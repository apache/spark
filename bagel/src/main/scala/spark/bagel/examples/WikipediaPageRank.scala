package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.collection.mutable.ArrayBuffer
import scala.xml.{XML,NodeSeq}

import java.io.{Externalizable,ObjectInput,ObjectOutput,DataOutputStream,DataInputStream}

import com.esotericsoftware.kryo._

object WikipediaPageRank {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: WikipediaPageRank <inputFile> <threshold> <numSplits> <host> [<noCombiner>]")
      System.exit(-1)
    }

    System.setProperty("spark.serialization", "spark.KryoSerialization")
    System.setProperty("spark.kryo.registrator", classOf[PRKryoRegistrator].getName)

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numSplits = args(2).toInt
    val host = args(3)
    val noCombiner = args.length > 4 && args(4).nonEmpty
    val sc = new SparkContext(host, "WikipediaPageRank")

    // Parse the Wikipedia page data into a graph
    val input = sc.textFile(inputFile)

    println("Counting vertices...")
    val numVertices = input.count()
    println("Done counting vertices.")

    println("Parsing input file...")
    val vertices: RDD[(String, PRVertex)] = input.map(line => {
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
      val outEdges = ArrayBuffer(links.map(link => new PREdge(new String(link.text))): _*)
      val id = new String(title)
      (id, new PRVertex(id, 1.0 / numVertices, outEdges, true))
    }).cache
    println("Done parsing input file.")

    // Do the computation
    val epsilon = 0.01 / numVertices
    val messages = sc.parallelize(List[(String, PRMessage)]())
    val result =
      if (noCombiner) {
        Bagel.run(sc, vertices, messages)(numSplits = numSplits)(PRNoCombiner.compute(numVertices, epsilon))
      } else {
        Bagel.run(sc, vertices, messages)(combiner = PRCombiner, numSplits = numSplits)(PRCombiner.compute(numVertices, epsilon))
      }

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top = result.filter(_.value >= threshold).map(vertex =>
      "%s\t%s\n".format(vertex.id, vertex.value)).collect.mkString
    println(top)
  }
}

@serializable
object PRCombiner extends Combiner[PRMessage, Double] {
  def createCombiner(msg: PRMessage): Double =
    msg.value
  def mergeMsg(combiner: Double, msg: PRMessage): Double =
    combiner + msg.value
  def mergeCombiners(a: Double, b: Double): Double =
    a + b

  def compute(numVertices: Long, epsilon: Double)(self: PRVertex, messageSum: Option[Double], superstep: Int): (PRVertex, Iterable[PRMessage]) = {
    val newValue = messageSum match {
      case Some(msgSum) if msgSum != 0 =>
        0.15 / numVertices + 0.85 * msgSum
      case _ => self.value
    }

    val terminate = (superstep >= 10 && (newValue - self.value).abs < epsilon) || superstep >= 30

    val outbox =
      if (!terminate)
        self.outEdges.map(edge =>
          new PRMessage(edge.targetId, newValue / self.outEdges.size))
      else
        ArrayBuffer[PRMessage]()

    (new PRVertex(self.id, newValue, self.outEdges, !terminate), outbox)
  }
}

@serializable
object PRNoCombiner extends DefaultCombiner[PRMessage] {
  def compute(numVertices: Long, epsilon: Double)(self: PRVertex, messages: Option[ArrayBuffer[PRMessage]], superstep: Int): (PRVertex, Iterable[PRMessage]) =
    PRCombiner.compute(numVertices, epsilon)(self, messages match {
      case Some(msgs) => Some(msgs.map(_.value).sum)
      case None => None
    }, superstep)
}

@serializable class PRVertex() extends Vertex {
  var id: String = _
  var value: Double = _
  var outEdges: ArrayBuffer[PREdge] = _
  var active: Boolean = true

  def this(id: String, value: Double, outEdges: ArrayBuffer[PREdge], active: Boolean) {
    this()
    this.id = id
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }
}

@serializable class PRMessage() extends Message {
  var targetId: String = _
  var value: Double = _

  def this(targetId: String, value: Double) {
    this()
    this.targetId = targetId
    this.value = value
  }
}

@serializable class PREdge() extends Edge {
  var targetId: String = _

  def this(targetId: String) {
    this()
    this.targetId = targetId
   }
}

class PRKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PRVertex])
    kryo.register(classOf[PRMessage])
    kryo.register(classOf[PREdge])
  }
}
