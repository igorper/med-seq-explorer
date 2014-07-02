import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


object SequenceGenerator {

	case class ActionNode(sessionID: String, topicView: String, topicTitle: String, timeDiffInSeconds: Int)
	val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

	def main(args: Array[String]) {
		// start timing execution
    val t0 = System.nanoTime()

		
		val conf = ConfigFactory.load
		val input = conf.getString("input");
		val sessionThreshold = conf.getInt("sessionThreshold")
		val clusterUrl = conf.getString("clusterUrl")

		val config = new SparkConf()
             .setMaster(clusterUrl)
             .setAppName("SequenceGenerator")
             .set("spark.executor.memory", "4g")


		val sc = new SparkContext(config)

		// read data
		val file = sc.textFile(input)
		println(file.count)

		// remove lines that contain the header string (it could be many of them
		// as different files could be loaded each starting with the header)
		// (the string starts with 'Session ID')
		// val noHeaderFile = file.filter(!_.startsWith("Session ID"))

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

		// stop timing execution
	  val t1 = System.nanoTime()
	  
		println("##########")
		printf("Processed '%s' with threshold: %d in %d seconds.\n", input, sessionThreshold, (t1 - t0)/1000000000)
		println("##########")
	}
}
