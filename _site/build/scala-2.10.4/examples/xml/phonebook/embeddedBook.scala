/* examples/phonebook/embeddedBook.scala */
package examples.phonebook  

object embeddedBook {

  val company  = <a href="http://acme.org">ACME</a>
  val first    = "Burak"
  val last     = "Emir"
  val location = "work"

  val embBook = 
    <phonebook>
      <descr>
        This is the <b>phonebook</b> of the 
        {company} corporation.
      </descr>
      <entry>
        <name>{ first+" "+last }</name> 
        <phone where={ location }>+41 21 693 68 {val x = 60 + 7; x}</phone>
      </entry>
    </phonebook>

  def main(args: Array[String]) = println( embBook )
}
