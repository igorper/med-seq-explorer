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

		val countSeperate = file.map(m => m.split("\t")).map(m => (m.slice(1, m.size), m(0).toInt))

		// select only those starting with S_ (which is the only S_) of length 3 or more
	 	val startWithSearch = countSeperate.filter(x => x._1(0).startsWith("S_") && x._1.count(y=>y.startsWith("S_")) == 1 && x._1.size > 2)

	 	val toReduce = startWithSearch.map(x=> (x._1.mkString("\t"), x._2)).reduceByKey(_+_).map(x => (x._2, x._1))

	 	val ordered = toReduce.sortByKey(false)

	 	val withoutPref = ordered.map(m =>(m._1, m._2.split("\t").map(i=>i.substring(2,i.size)))).map(m => (m._1, m._2(0), m._2.slice(1, m._2.size))).cache


	 	for(l <- minSeq to (maxSeq - 1)) {
	 		// store only those that occure more than once
	 		val toSave = withoutPref.filter(f => f._1 > 1 && f._3.size == l).map(x => x._1 + ", (" + x._2 + "): " + x._3.mkString(" --> "))
	 		sc.makeRDD(toSave.take(maxResults)).coalesce(1).saveAsTextFile(output + l)
	 	}

		// stop timing execution
		val t1 = System.nanoTime()

		println("##########")
		printf("Processed '%s' in %d seconds.\n", prepFolder, (t1 - t0)/1000000000)
		println("##########")
	}
}
