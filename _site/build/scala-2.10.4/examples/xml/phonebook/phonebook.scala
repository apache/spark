package examples.phonebook  

object phonebook {

  val labPhoneBook = 
    <phonebook>
      <descr>
        This is the <b>phonebook</b> of the 
        <a href="http://acme.org">ACME</a> corporation.
      </descr>
      <entry>
        <name>Burak</name> 
        <phone where="work">  +41 21 693 68 67</phone>
        <phone where="mobile">+41 79 602 23 23</phone>
      </entry>
    </phonebook>

  println( labPhoneBook )

  // XML is immutable - adding an element

  import scala.xml.{ Node, Text }

  def add( phonebook:Node, newEntry:Node ):Node = phonebook match {
    case <phonebook>{ ch @ _* }</phonebook> => 
            <phonebook>{ ch }{ newEntry }</phonebook>
  }

  val pb2 = 
    add( labPhoneBook, 
         <entry>
           <name>Kim</name> 
           <phone where="work">  +41 21 111 11 11</phone>
         </entry> )

  def main(args:Array[String]) = println( pb2 )

}
