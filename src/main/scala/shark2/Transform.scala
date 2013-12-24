package catalyst
package shark2

import catalyst.expressions._
import shark.SharkContext
import java.io.{InputStreamReader, BufferedReader}

import collection.JavaConversions._

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
case class Transform(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SharkPlan)(@transient sc: SharkContext) extends UnaryNode {
  override def otherCopyArgs = sc :: Nil

  def execute() = {
    child.execute().mapPartitions { partition =>
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd)
      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val reader = new BufferedReader(new InputStreamReader(inputStream))

      // TODO: This should be exposed as an iterator instead of reading in all the data at once for a partition.
      val outputLines = collection.mutable.ArrayBuffer[Row]()
      val readerThread = new Thread("Transform OutoutReader") {
        override def run() {
          var curLine = reader.readLine()
          while(curLine != null) {
            outputLines += buildRow(curLine.split("\t"))
            curLine = reader.readLine()
          }
        }
      }
      readerThread.start()
      partition
        .map(row => input.map(Evaluate(_, Vector(row))))
        .map(_.mkString("", "\t", "\n").getBytes).foreach(outputStream.write)
      outputStream.close()
      readerThread.join()
      outputLines.toIterator
    }
  }
}