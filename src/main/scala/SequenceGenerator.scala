import org.apache.spark._
import org.apache.spark.SparkContext._

import com.typesafe.config.ConfigFactory
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


object SequenceGenerator {

	case class ActionNode(sessionID: String, topicView: String, topicTitle: String, timeDiffInSeconds: Int)
	val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

	def main(args: Array[String]) {
		
		val conf = ConfigFactory.load
		val input = conf.getString("input");
		val sessionThreshold = conf.getInt("sessionThreshold")			

		val sc = new SparkContext("local[4]", "SequenceGenerator")

		// read data
		val file = sc.textFile(input)

		// drop the header line
		val lines = file.mapPartitionsWithIndex { (index, iter) => if (index == 0) iter.drop(1) else iter }

		// create batches of two sequential elements to calculate time diff
		// (to do this, lines first have to be collected and then paralleized again)
		val slidedLines = sc.parallelize(lines.collect.iterator.sliding(2).toList)

		// split lines to columns (take only the first 7 columns as the last one could be empty, which messes up cols access indexes)
		val cols = slidedLines.map(line => line(0).split("\t").slice(0,6) ++ line(1).split("\t").slice(0,6))

		// create action nodes (for each node we also calculate time distance in seconds between two consequtive nodes in seconds)
		val actionNodes = cols.map(cols => ActionNode(cols(0), cols(3), cols(6), ((dateFormat.parse(cols(10)).getTime() - dateFormat.parse(cols(4)).getTime()) / 1000).toInt))

		// group nodes by sessionID
		val groupedBySession = actionNodes.groupBy(_.sessionID)

		// foreach group (session) check if the time diff between two nodes was longer than a predefined threshold and optionally explode
		// them to additional subsession
		val groupedSplitSessions = groupedBySession.values.map(nodes => {
			var cnt = 1
			nodes.map(node => {
				if(node.timeDiffInSeconds > sessionThreshold) { 
					cnt = cnt + 1
				} 

				new ActionNode(node.sessionID + "_" + cnt.toString, node.topicView, node.topicTitle, node.timeDiffInSeconds)
			})
		})

		// flatten grouped sessions to a single level collection
		val splitSessions = groupedSplitSessions.flatMap(node => node)

		println("##########")
		printf("Processed '%s' with threshold: %d\n", input, sessionThreshold)
		println("##########")
	}
}