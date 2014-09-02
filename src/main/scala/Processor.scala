package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory

/*
This is the main file that submitted to the spark cluster. It loads the configuration and
runs the appropriate action (task).
*/
object Processor {

	def registerProcessors() : Map[String, ActionRunner] = {
		Map("SearchTopics" -> SearchTopics, "SearchTopicsByHour" -> SearchTopicsByHour)
	}

	def main(args: Array[String]) {
		val registeredProcessors = registerProcessors()

		val config = new SparkConf().setAppName("Processor")
		val sc = new SparkContext(config)

		val conf = ConfigFactory.load
		val rawFolder = conf.getString("rawFolder")
		val prepSequencesFolder = conf.getString("preprocessedSequencesFolder")
		val minSeq = conf.getInt("minSequence")
		val maxSeq = conf.getInt("maxSequence")
		val maxResults = conf.getInt("resultsLimit")
		val action = conf.getString("action")

		val actionRunner = try { 
		  registeredProcessors(action)
		} catch {
		  case e: java.util.NoSuchElementException => {
		  	println("Invalid action.")
		  	return
		  }
		}

		
		actionRunner.initialize(sc, action, minSeq, maxSeq)

		actionRunner.preprocessToSearchSequences(rawFolder, prepSequencesFolder)
		actionRunner.process()
		// println(actionRunner.SearchPrefix)

		println("Processor")
	}

}