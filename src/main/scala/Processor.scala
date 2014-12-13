package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory

/*
 TODO: 
	- create test to make sure preprocessing works OK (start with SearchTopics), carry on for the others
*/

/*
This is the main file that submitted to the spark cluster. It loads the configuration and
runs the appropriate action (task).
*/
object Processor {

	def registerProcessors() : Map[String, ActionRunner] = {
		Map(
			"SearchTopics" -> SearchTopics, 
			"SearchTopicsByHour" -> SearchTopicsByHour,
			"DistinctSession" -> DistinctSession
			)
	}

	def main(args: Array[String]) {
		val registeredProcessors = registerProcessors()

		val config = new SparkConf().setAppName("Processor")
		val sc = new SparkContext(config)

		val conf = ConfigFactory.load
		val rawFolder = conf.getString("rawFolder")
		val resultsFolder = conf.getString("resultsFolder")
		val minSeq = conf.getInt("minSequence")
		val maxSeq = conf.getInt("maxSequence")
		val maxResults = conf.getInt("resultsLimit")
		val action = conf.getString("action")
		val processingOutputType = conf.getString("processingOutputType")

		val actionRunner = try { 
		  registeredProcessors(action)
		} catch {
		  case e: java.util.NoSuchElementException => {
		  	println("Invalid action.")
		  	return
		  }
		}
		
		actionRunner.initialize(sc, action, minSeq, maxSeq, rawFolder, resultsFolder, maxResults, processingOutputType)

		actionRunner.preprocessToSearchSequences()
		actionRunner.process()
		// println(actionRunner.SearchPrefix)

		println("Processor")
	}

}