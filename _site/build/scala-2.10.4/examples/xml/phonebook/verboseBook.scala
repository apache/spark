/* examples/xml/phonebook/verboseBook.scala */
package examples.phonebook  

object verboseBook {

  import scala.xml.{ UnprefixedAttribute, Elem, Node, Null, Text, TopScope } 

  val pbookVerbose = 
    Elem(null, "phonebook", Null, TopScope, false,
       Elem(null, "descr", Null, TopScope, false,
            Text("This is a "), 
            Elem(null, "b", Null, TopScope, false, Text("sample")),
            Text("description")
          ),
       Elem(null, "entry", Null, TopScope, false,
            Elem(null, "name", Null, TopScope, false, Text("Burak Emir")),
            Elem(null, "phone", new UnprefixedAttribute("where","work", Null), TopScope, false,
                 Text("+41 21 693 68 67"))
          )
       )

  def main(args: Array[String]) = println( pbookVerbose )
}
