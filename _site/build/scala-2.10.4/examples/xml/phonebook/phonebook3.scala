package examples.phonebook  

object phonebook3 {

  import scala.xml.{Elem, Node, Text}
  import scala.xml.PrettyPrinter
  import Node.NoAttributes

  /* this method "changes" (returns an updated copy) of the phonebook when the
   *   entry for Name exists. If it has an attribute "where" whose value is equal to the
   *   parameter Where, it is changed, otherwise, it is added.
   */
  def change ( phonebook:Node, Name:String, Where:String, newPhone:String ) = {

    /** this nested function walks through tree, and returns an updated copy of it  */
    def copyOrChange ( ch: Iterator[Node] ) = {

      import xml.Utility.{trim,trimProper} //removes whitespace nodes, which are annoying in matches

      for( c <- ch ) yield trimProper(c) match {

          // if the node is the particular entry we are looking for, return an updated copy

          case x @ <entry><name>{ Text(Name) }</name>{ ch1 @ _* }</entry> => 

            var updated = false
            val ch2 = for(c <- ch1) yield c match { // does it have the phone number?

              case y @ <phone>{ _* }</phone> if y \ "@where" == Where => 
                updated = true
                <phone where={ Where }>{ newPhone }</phone>
              
              case y => y
              
            }
            if( !updated ) { // no, so we add as first entry
            
              <entry>
                <name>{ Name }</name>
                <phone where={ Where }>{ newPhone }</phone>
                { ch1 }
              </entry>
              
            } else {         // yes, and we changed it as we should
              
              <entry>
                { ch2 }
              </entry>
        
            } 
          // end case x @ <entry>...
          
          // other entries are copied without changing them

          case x =>           
            x
          
        }
    } // for ... yield ... returns an Iterator[Node]
    
    // decompose phonebook, apply updates
    phonebook match {
      case <phonebook>{ ch @ _* }</phonebook> =>
        <phonebook>{ copyOrChange( ch.iterator ) }</phonebook>
    }
    
  }

  val pb2 = change( phonebook1.labPhoneBook, "John", "work", "+41 55 555 55 55" )

  val pp = new PrettyPrinter( 80, 5 )

  def main( args:Array[String] ) = {
    println("---before---")
    println( pp.format( phonebook1.labPhoneBook ))
    println("---after---")
    println( pp.format( pb2 ))
  }
}
