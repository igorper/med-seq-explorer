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
		val PrintFormatter = (m:(Int, (Iterable[String], List[(String, Int)]))) => "{" + m._2._2.mkString(", ") + "}:" + m._2._1.mkString(" --> ") + " [" + m._1 + "]"

		// start timing execution
		val t0 = System.nanoTime()

		val conf = ConfigFactory.load
		val prepFolder = conf.getString("outputFolder")
		// search and topic sequence non interupted (only one search in each sequence)
		val output = conf.getString("search_tseq_nint")
		val minSeq = conf.getInt("minSequence")
		val maxSeq = conf.getInt("maxSequence")
		val maxResults = conf.getInt("maxNonFilteredResults")


		val config = new SparkConf()
		.setAppName("SearchTopicSequenceGenerator")
		
		val sc = new SparkContext(config)

		// read data
		val file = sc.textFile(prepFolder + "*")

		val countSeparate = file.map(m => m.split("\t")).map(m => (m.slice(2, m.size), m(1), m(0).toInt))

	 	for(l <- minSeq to (maxSeq - 1)) {

			val combineCountList = countSeparate.filter(f => f._1.size == l).map(i=> (i._1.toList, (i._2, i._3)))

			val orderedList = combineCountList.combineByKey((v) => (List(),0),(a: (List[(String,Int)],Int), v) => (List((v._1,v._2)), v._2), (b: (List[(String,Int)],Int), c: (List[(String,Int)], Int)) => (b._1 ++ c._1,b._2 + c._2)).map(m=>(m._2._2,(m._1,m._2._1))).sortByKey(false)

			val combineCountSet = countSeparate.filter(f => f._1.size == l).map(i=> (i._1.toSet, (i._2, i._3)))

			val orderedSet = combineCountSet.combineByKey((v) => (List(),0),(a: (List[(String,Int)],Int), v) => (List((v._1,v._2)), v._2), (b: (List[(String,Int)],Int), c: (List[(String,Int)], Int)) => (b._1 ++ c._1,b._2 + c._2)).map(m=>(m._2._2,(m._1,m._2._1))).sortByKey(false)

	 		sc.makeRDD(orderedSet.map(PrintFormatter).take(maxResults)).coalesce(1).saveAsTextFile(output + "set_" + l )
	 		sc.makeRDD(orderedList.map(PrintFormatter).take(maxResults)).coalesce(1).saveAsTextFile(output + "list_" + l)
	 	}

		// stop timing execution
		val t1 = System.nanoTime()

		println("##########")
		printf("Processed '%s' in %d seconds.\n", prepFolder, (t1 - t0)/1000000000)
		println("##########")
	}
}
