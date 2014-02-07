package catalyst
package execution

import java.io.{InputStreamReader, BufferedReader}

import catalyst.expressions._

import scala.collection.JavaConversions._

/**
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SharkPlan)(@transient sc: SharkContext)
  extends UnaryNode {

  override def otherCopyArgs = sc :: Nil

  def execute() = {
    child.execute().mapPartitions { iter =>
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd)
      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val reader = new BufferedReader(new InputStreamReader(inputStream))

      // TODO: This should be exposed as an iterator instead of reading in all the data at once.
      val outputLines = collection.mutable.ArrayBuffer[Row]()
      val readerThread = new Thread("Transform OutputReader") {
        override def run() {
          var curLine = reader.readLine()
          while (curLine != null) {
            // TODO: Use SerDe
            outputLines += new GenericRow(curLine.split("\t"))
            curLine = reader.readLine()
          }
        }
      }
      readerThread.start()
      iter
        .map(row => input.map(Evaluate(_, Vector(row))))
        // TODO: Use SerDe
        .map(_.mkString("", "\t", "\n").getBytes).foreach(outputStream.write)
      outputStream.close()
      readerThread.join()
      outputLines.toIterator
    }
  }
}