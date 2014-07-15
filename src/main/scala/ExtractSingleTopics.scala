package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory

/*
Extracts single TopicView/full items and saves them to an output file.
*/
object ExtractSingleTopics {

	def main(args: Array[String]) {
		// start timing execution
		val t0 = System.nanoTime()

		val conf = ConfigFactory.load
		val input = conf.getString("extractSingleTopics_input");
		val output = conf.getString("extractSingleTopics_output")

		val config = new SparkConf()
		.setAppName("ExtractSingleTopics")
		
		val sc = new SparkContext(config)

		// read data
		val file = sc.textFile(input,500)

		println(input)

		// remove lines that contain the header string (it could be many of them
		// as different files could be loaded each starting with the header)
		// (the string starts with 'Session ID')
		val noHeaderFile = file.filter(!_.startsWith("Session ID"))

		// remove lines that do not contain at least 7 columns
		var removedShort = noHeaderFile.map(line => line.split("\t")).filter(n=>n.length >= 7)

		// keep only TopicView/full ans Search/Lucene
		// store as (sessionID, topicView, topicTitle)
		val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full")).map(m => m(6))

		val topicsCounts = topicFullSessions.map(s => (s, 1)).reduceByKey(_+_, 500)

		val onlyTopics = topicsCounts.map(_._1)

		// save the to a single file 
		onlyTopics.coalesce(1).saveAsTextFile(output)

		// stop timing execution
		val t1 = System.nanoTime()

		println("##########")
		printf("Processed '%s' in %d seconds.\n", input, (t1 - t0)/1000000000)
		println("##########")
	}
}
