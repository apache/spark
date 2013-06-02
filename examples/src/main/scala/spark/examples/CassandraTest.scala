package spark.examples

import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.hadoop.{ConfigHelper, ColumnFamilyInputFormat}
import org.apache.cassandra.thrift.{IndexExpression, SliceRange, SlicePredicate}
import spark.{RDD, SparkContext}
import SparkContext._
import java.nio.ByteBuffer
import java.util.SortedMap
import org.apache.cassandra.db.IColumn
import org.apache.cassandra.utils.ByteBufferUtil
import scala.collection.JavaConversions._


/*
 * This example demonstrates using Spark with Cassandra with the New Hadoop API and Cassandra support for Hadoop.
 *
 * To run this example, run this file with the following command params -
 * <spark_master> <cassandra_node> <cassandra_port>
 *
 * So if you want to run this on localhost this will be,
 * local[3] localhost 9160
 *
 * The example makes some assumptions:
 * 1. You have already created a keyspace called casDemo and it has a column family named Words
 * 2. There are column family has a column named "para" which has test content.
 *
 * You can create the content by running the following script at the bottom of this file with cassandra-cli.
 *
 */
object CassandraTest {
  def main(args: Array[String]) {

    //Get a SparkContext
    val sc = new SparkContext(args(0), "casDemo")

    //Build the job configuration with ConfigHelper provided by Cassandra
    val job = new Job()
    job.setInputFormatClass(classOf[ColumnFamilyInputFormat])

    ConfigHelper.setInputInitialAddress(job.getConfiguration(), args(1))

    ConfigHelper.setInputRpcPort(job.getConfiguration(), args(2))

    ConfigHelper.setInputColumnFamily(job.getConfiguration(), "casDemo", "Words")

    val predicate = new SlicePredicate()
    val sliceRange = new SliceRange()
    sliceRange.setStart(Array.empty[Byte])
    sliceRange.setFinish(Array.empty[Byte])
    predicate.setSlice_range(sliceRange)
    ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate)

    ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner")

    //Make a new Hadoop RDD
    val casRdd = sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[ColumnFamilyInputFormat],
      classOf[ByteBuffer],
      classOf[SortedMap[ByteBuffer, IColumn]])

    // Let us first get all the paragraphs from the retrieved rows
    val paraRdd = casRdd.flatMap {
      case (key, value) => {
        value.filter(v => ByteBufferUtil.string(v._1).compareTo("para") == 0).map(v => ByteBufferUtil.string(v._2.value()))
      }
    }

    //Lets get the word count in paras
    val counts = paraRdd.flatMap(p => p.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    counts.collect().foreach {
      case (word, count) => println(word + ":" + count)
    }
  }
}

/*
create keyspace casDemo;
use casDemo;

create column family Words with comparator = UTF8Type;
update column family Words with column_metadata = [{column_name: book, validation_class: UTF8Type}, {column_name: para, validation_class: UTF8Type}];

assume Words keys as utf8;

set Words['3musk001']['book'] = 'The Three Musketeers';
set Words['3musk001']['para'] = 'On the first Monday of the month of April, 1625, the market town of
	Meung, in which the author of ROMANCE OF THE ROSE was born, appeared to
	be in as perfect a state of revolution as if the Huguenots had just made
	a second La Rochelle of it. Many citizens, seeing the women flying
	toward the High Street, leaving their children crying at the open doors,
	hastened to don the cuirass, and supporting their somewhat uncertain
	courage with a musket or a partisan, directed their steps toward the
	hostelry of the Jolly Miller, before which was gathered, increasing
	every minute, a compact group, vociferous and full of curiosity.';

set Words['3musk002']['book'] = 'The Three Musketeers';
set Words['3musk002']['para'] = 'In those times panics were common, and few days passed without some city
	or other registering in its archives an event of this kind. There were
	nobles, who made war against each other; there was the king, who made
	war against the cardinal; there was Spain, which made war against the
	king. Then, in addition to these concealed or public, secret or open
	wars, there were robbers, mendicants, Huguenots, wolves, and scoundrels,
	who made war upon everybody. The citizens always took up arms readily
	against thieves, wolves or scoundrels, often against nobles or
	Huguenots, sometimes against the king, but never against cardinal or
	Spain. It resulted, then, from this habit that on the said first Monday
	of April, 1625, the citizens, on hearing the clamor, and seeing neither
	the red-and-yellow standard nor the livery of the Duc de Richelieu,
	rushed toward the hostel of the Jolly Miller. When arrived there, the
	cause of the hubbub was apparent to all';

set Words['3musk003']['book'] = 'The Three Musketeers';
set Words['3musk003']['para'] = 'You ought, I say, then, to husband the means you have, however large
	the sum may be; but you ought also to endeavor to perfect yourself in
	the exercises becoming a gentleman. I will write a letter today to the
	Director of the Royal Academy, and tomorrow he will admit you without
	any expense to yourself. Do not refuse this little service. Our
	best-born and richest gentlemen sometimes solicit it without being able
	to obtain it. You will learn horsemanship, swordsmanship in all its
	branches, and dancing. You will make some desirable acquaintances; and
	from time to time you can call upon me, just to tell me how you are
	getting on, and to say whether I can be of further service to you.';


set Words['thelostworld001']['book'] = 'The Lost World';
set Words['thelostworld001']['para'] = 'She sat with that proud, delicate profile of hers outlined against the
	red curtain.  How beautiful she was!  And yet how aloof!  We had been
	friends, quite good friends; but never could I get beyond the same
	comradeship which I might have established with one of my
	fellow-reporters upon the Gazette,--perfectly frank, perfectly kindly,
	and perfectly unsexual.  My instincts are all against a woman being too
	frank and at her ease with me.  It is no compliment to a man.  Where
	the real sex feeling begins, timidity and distrust are its companions,
	heritage from old wicked days when love and violence went often hand in
	hand.  The bent head, the averted eye, the faltering voice, the wincing
	figure--these, and not the unshrinking gaze and frank reply, are the
	true signals of passion.  Even in my short life I had learned as much
	as that--or had inherited it in that race memory which we call instinct.';

set Words['thelostworld002']['book'] = 'The Lost World';
set Words['thelostworld002']['para'] = 'I always liked McArdle, the crabbed, old, round-backed, red-headed news
	editor, and I rather hoped that he liked me.  Of course, Beaumont was
	the real boss; but he lived in the rarefied atmosphere of some Olympian
	height from which he could distinguish nothing smaller than an
	international crisis or a split in the Cabinet.  Sometimes we saw him
	passing in lonely majesty to his inner sanctum, with his eyes staring
	vaguely and his mind hovering over the Balkans or the Persian Gulf.  He
	was above and beyond us.  But McArdle was his first lieutenant, and it
	was he that we knew.  The old man nodded as I entered the room, and he
	pushed his spectacles far up on his bald forehead.';

*/
