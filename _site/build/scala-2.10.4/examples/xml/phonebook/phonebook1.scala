/* examples/phonebook/phonebook1.scala */
package examples.phonebook  

object phonebook1 {

  val labPhoneBook = 
    <phonebook>
      <descr>
        This is the <b>phonebook</b> of the 
        <a href="http://acme.org">ACME</a> corporation.
      </descr>
      <entry>
        <name>Burak Emir</name> 
        <phone where="work">+41 21 693 68 67</phone>
      </entry>
    </phonebook>

  def main(args: Array[String]) = println( labPhoneBook )
}
