/* examples/xml/phonebook/phonebook2.scala */
package examples.phonebook  

object phonebook2 {

  import scala.xml.Node

  /** adds an entry to a phonebook */
  def add( p: Node, newEntry: Node ): Node = p match {

      case <phonebook>{ ch @ _* }</phonebook> => 

        <phonebook>{ ch }{ newEntry }</phonebook>
  }

  val pb2 = 
    add( phonebook1.labPhoneBook, 
         <entry>
           <name>Kim</name> 
           <phone where="work">+41 21 111 11 11</phone>
         </entry> )

  def main( args: Array[String] ) = println( pb2 )
}
