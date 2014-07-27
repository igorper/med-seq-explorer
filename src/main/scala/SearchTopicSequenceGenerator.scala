package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory

/*
Prints sorted occurences of sequences in the form of
- (search query): Topics sequence
- where Topics sequence is not interupted by a new search query
*/
object SearchTopicSequenceGenerator {

	def main(args: Array[String]) {
		// prefixes
		val SearchPrefix = "s_"
		val TopicPrefix = "t_"

		// formatter for printing results
		val ListPrintFormatter = (m:(Int, (Iterable[String], List[(String, Int)]))) => "*** ["+ m._1 +"]\n" + m._2._1.mkString(" --> ") + ":\n" + m._2._2.mkString("\n") + "\n"
		
		// removes the prefix from each item
		val RemovePrefix = (x:(Int, (Iterable[String], List[(String, Int)]))) => (x._1,(x._2._1.map(t=>t.slice(2,t.size)), x._2._2.map(s=>(s._1.slice(2,s._1.size),s._2))))


		// Splits the sequence into two parts, one containg these search query, the other containg the topics sequence.
		// A prefix of each item is used for disambiguation.
		val SplitToTopicAndSearch = (m:(Iterable[String], Int)) => (m._1.filter(f=>f.startsWith(TopicPrefix)), m._1.filter(f=>f.startsWith(SearchPrefix)).head, m._2)

		// start timing execution
		val t0 = System.nanoTime()

		// read configuration
		val conf = ConfigFactory.load
		val prepFolder = conf.getString("outputFolder")
		val output = conf.getString("search_tseq_nint")
		val minSeq = conf.getInt("minSequence")
		val maxSeq = conf.getInt("maxSequence")
		val maxResults = conf.getInt("maxNonFilteredResults")

		val config = new SparkConf().setAppName("SearchTopicSequenceGenerator")		
		val sc = new SparkContext(config)

		// read data
		val file = sc.textFile(prepFolder + "*")
		//	val file =sc.textFile("/scratch/users/pernek/results/preproc_search/2_to_10monthly201101/*")

		// Split count and sequence (at this point still search + topics). Also lowercase the sequence.
		val reducedSequences = file.map(m => m.split("\t")).map(m => (m.slice(1,m.size).map(i=>i.toLowerCase), m(0).toInt))
		
		// Reduce the sequences (this will take care of joining same sequences from different files).
		val reducedSequencesList = reducedSequences.map(m => (m._1.toList, m._2)).reduceByKey(_+_)

		// split search and topic sequence and cache the results
		val countSeparateList = reducedSequencesList.map(SplitToTopicAndSearch).cache

		// loop different sequence lengths
		for(l <- minSeq to (maxSeq - 1)) {

			// pick only sequences of the particular length
			val combineCountList = countSeparateList.filter(f => f._1.size == l).map(i=> (i._1.toList, (i._2, i._3)))

			// the main part of the logic. count search occurences for each topic sequence.
			val orderedList = combineCountList.combineByKey((v) => (List((v._1, v._2)),v._2),(a: (List[(String,Int)],Int), v) => (a._1 ++ List((v._1,v._2)), a._2 + v._2), (b: (List[(String,Int)],Int), c: (List[(String,Int)], Int)) => ((b._1 ++ c._1).sortWith((x,y)=> x._2 > y._2),b._2 + c._2)).map(m=>(m._2._2,(m._1,m._2._1))).sortByKey(false)

			// save to one file
			sc.makeRDD(orderedList.map(RemovePrefix).map(ListPrintFormatter).take(maxResults)).coalesce(1).saveAsTextFile(output + "list_" + l)
		}

		// stop timing execution
		val t1 = System.nanoTime()

		println("##########")
		printf("Processed '%s' in %d seconds.\n", prepFolder, (t1 - t0)/1000000000)
		println("##########")
	}
}
