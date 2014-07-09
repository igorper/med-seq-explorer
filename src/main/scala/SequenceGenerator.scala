import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object SequenceGenerator {

	val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

	def main(args: Array[String]) {
			// start timing execution
			val t0 = System.nanoTime()


			val conf = ConfigFactory.load
			val input = conf.getString("input");
			val sessionThreshold = conf.getInt("sessionThreshold")
			val clusterUrl = conf.getString("clusterUrl")

			println("This is input " + input)

			val config = new SparkConf()
			.setAppName("SequenceGenerator")
			// .set("spark.executor.memory", "120g")
			// .set("spark.executor.memory", "1g")
			.set("spark.storage.memoryFraction", "0.1")
			// .set("spark.cores.max", "8")
			.set("spark.shuffle.consolidateFiles", "true")
			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.shuffle.memoryFraction", "0.7")
			.set("spark.shuffle.spill", "false")
			// .set("spark.local.dir", "/scratch/users/pernek")
			val sc = new SparkContext(config)
			val file = sc.textFile("/media/lin-data/Code/med-seq-explorer/data/short.txt")
			println(file.count)

			/*val allMonths = new java.io.File("/ncbodata/non-emr/uptodate/rawlogs/").listFiles.filter(_.getName.endsWith(".txt")).map(_.getName.slice(0,6)).distinct
			//val allMonths = Vector("201206*.txt")
			var month = null
			for(month <- allMonths){
				val loadPath = "/ncbodata/non-emr/uptodate/rawlogs/" + month + "*.txt"
				println(loadPath)

				// read data
				//val file = sc.textFile("/home/pernek/UpToDate/20110101.txt")
				val file = sc.textFile("/ncbodata/non-emr/uptodate/rawlogs/201101*.txt")

				// remove lines that contain the header string (it could be many of them
				// as different files could be loaded each starting with the header)
				// (the string starts with 'Session ID')
				val noHeaderFile = file.filter(!_.startsWith("Session ID"))

				// remove lines that do not contain at least 7 columns
				var removedShort = noHeaderFile.map(line => line.split("\t")).filter(n=>n.length >= 7)

				// keep only TopicView/ful
				// store as (sessionID, topicView, topicTitle)
				val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full")).map(m => (m(0), m(3), m(6)))

				// count occurences for single topics
				//val titleCounts = topicFullSessions.map(n => (n._3, 1)).reduceByKey(_+_, nPartitions)
				//      println(titleCounts.first)

				// order results by decreasing count
				//val sortedTitleCounts = titleCounts.map { case (title, count) => (count, title) }.sortByKey(false,nPartitions)

				//              sortedTitleCounts.take(10).foreach(println)

				// store single counts to files
				//sortedTitleCounts.saveAsTextFile("results/counts-single.txt")

				// group nodes by sessionID
				val groupedBySession = topicFullSessions.groupBy(_._1)

				// for each session create a sequence of topic titles
				val sequences = groupedBySession.map { case (sessionID, nodes) => nodes.map(_._3) }

				// generate all sequence combinations for sessions
				// (for now we just constrain max sequnce length to 5 due to possible large sessions => ~1k nodes)
				val sequenceCombinations = sequences.map(sequence => (2 to math.min(sequence.size,5)).map(winSize => sequence.sliding(winSize)).flatMap(seqIter => seqIter)).flatMap(iter => iter)

				val sequenceCombinationsCounts = sequenceCombinations.filter(s => s.size > 2).map(s => (s, 1)).reduceByKey(_+_)

				val sequenceCombinationsFiltered = sequenceCombinationsCounts.filter{ case (key, count) => count > 1}

				//val filteredSequenceCombinations = sequenceCombinationsCounts.filter((key, count) => count > )

				val reversedSequenceCombinationsCounts = sequenceCombinationsFiltered.map{ case (comb, count) => count + "\t" + comb.mkString("\t") }

				reversedSequenceCombinationsCounts.coallesce(1)saveAsTextFile("/scratch/users/pernek/results/comb_counts_txt1_" + month)


					//	val sortedSequenceCombinationsCounts = sequenceCombinationsCounts.map{ case (comb, count) => (count, comb) }.sortByKey(false)
				}

			// read data
			//val file = sc.textFile("/home/pernek/UpToDate/20110101.txt")
//			val file = sc.textFile("/ncbodata/non-emr/uptodate/rawlogs/201101*.txt")

				// remove lines that contain the header string (it could be many of them
				// as different files could be loaded each starting with the header)
				// (the string starts with 'Session ID')
//				val noHeaderFile = file.filter(!_.startsWith("Session ID"))

				// remove lines that do not contain at least 7 columns
//				var removedShort = noHeaderFile.map(line => line.split("\t")).filter(n=>n.length >= 7)

				// keep only TopicView/ful
				// store as (sessionID, topicView, topicTitle)
//				val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full")).map(m => (m(0), m(3), m(6)))

				// count occurences for single topics
				//val titleCounts = topicFullSessions.map(n => (n._3, 1)).reduceByKey(_+_, nPartitions)
				//	println(titleCounts.first)

				// order results by decreasing count
				//val sortedTitleCounts = titleCounts.map { case (title, count) => (count, title) }.sortByKey(false,nPartitions)

				//		sortedTitleCounts.take(10).foreach(println)

				// store single counts to files
				//sortedTitleCounts.saveAsTextFile("results/counts-single.txt")

				// group nodes by sessionID
//				val groupedBySession = topicFullSessions.groupBy(_._1)

				// for each session create a sequence of topic titles
//				val sequences = groupedBySession.map { case (sessionID, nodes) => nodes.map(_._3) }

			// generate all sequence combinations for sessions
			// (for now we just constrain max sequnce length to 5 due to possible large sessions => ~1k nodes)
//			val sequenceCombinations = sequences.map(sequence => (2 to math.min(sequence.size,5)).map(winSize => sequence.sliding(winSize)).flatMap(seqIter => seqIter)).flatMap(iter => iter)

//				val sequenceCombinationsCounts = sequenceCombinations.filter(s => s.size > 1).map(s => (s, 1)).reduceByKey(_+_)

//				val sortedSequenceCombinationsCounts = sequenceCombinationsCounts.map{ case (comb, count) => (count, comb) }.sortByKey(false)

//				sortedSequenceCombinationsCounts.saveAsTextFile("results/comb_counts.txt")




				// create batches of two sequential elements to calculate time diff
				// (to do this, lines first have to be collected and then paralleized again)
				// val slidedLines = sc.parallelize(noHeaderFile.collect.iterator.sliding(2).toList)

				// split lines to columns (take only the first 7 columns as the last one could be empty, which messes up cols access indexes)
				// val cols = slidedLines.map(line => line(0).split("\t").slice(0,8) ++ line(1).split("\t").slice(0,8))

				// create action nodes (for each node we also calculate time distance in seconds between two consequtive nodes in seconds)
				//val actionNodes = cols.map(cols => ActionNode(cols(0), cols(3), cols(6), ((dateFormat.parse(cols(12)).getTime() - dateFormat.parse(cols(4)).getTime()) / 1000).toInt))
				// val actionNodes = noHeaderFile.map(line => line.split("\t")).map(cols => ActionNode(cols(0), cols(3), cols(6), 0))

				// group nodes by sessionID
				// val groupedBySession = actionNodes.groupBy(_.sessionID)

				// foreach group (session) check if the time diff between two nodes was longer than a predefined threshold and optionally explode
				// them to additional subsession
				// val groupedSplitSessions = groupedBySession.values.map(nodes => {
				// 	var cnt = 1
				// 	nodes.map(node => {
				// 		if(node.timeDiffInSeconds > sessionThreshold) { 
				// 			cnt = cnt + 1
				// 		} 

				// 		new ActionNode(node.sessionID + "_" + cnt.toString, node.topicView, node.topicTitle, node.timeDiffInSeconds)
				// 	})
				// })

				// flatten grouped sessions to a single level collection
				// val splitSessions = groupedSplitSessions.flatMap(node => node)

				// filter out all topic nodes that are not TopicView/full
				// val topicFullSessions = actionNodes.filter(_.topicView.contains("TopicView/full"))

				// group sessions again, this time using also the newly created subsessions
				// val groupedBySplitSessions = topicFullSessions.groupBy(_.sessionID)

				// for each session create a sequence of topic titles
				// val sequences = groupedBySplitSessions.map {case (sessionID, nodes) => {
				// 		(sessionID, nodes.map(_.topicTitle))
				// 	}
				// }

				// print out created sequences
				//sequences.values.foreach(println)

				// get frequencies of individual topics 
				// val titleCounts = sequences.values.flatMap(n=>n).map(n=>(n, 1)).reduceByKey(_+_).map {case (title, count) => (count, title) }

				// order results by decreasing count
				// val sortedTitleCounts = titleCounts.sortByKey(false)

				// store the results to a file
				// sortedTitleCounts.saveAsTextFile("results/counts.txt")
*/
				// stop timing execution
				val t1 = System.nanoTime()

				println("##########")
				printf("Processed '%s' with threshold: %d in %d seconds.\n", input, sessionThreshold, (t1 - t0)/1000000000)
				println("##########")
			}
		}
