package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._

object DistinctSession extends ActionRunner {

	override def doPreprocessing(loadPath: String, outputPath: String) = {
		// read data
		val file = sparkContext.textFile(loadPath)

		// remove lines that contain the header string (it could be many of them
		// as different files could be loaded each starting with the header)
		// (the string starts with 'Session ID')
		val noHeaderFile = file.filter(!_.startsWith("Session ID"))

		// extract distinct session ids
		val distinctSessionIds = noHeaderFile.map(l=>l.split("\t")).map(m=>m(0)).distinct

		// save the combinations to a single file 
		distinctSessionIds.coalesce(1).saveAsTextFile(outputPath)
	}

	override def doProcessing() = {
		val file = sparkContext.textFile(this.preprocessingFolder + "*")

		val distinctSessionIds = map(l=>l.split("\t")).map(m=>m(0)).distinct

		distinctSessionIds.coalesce(1).saveAsTextFile(this.processingFolder)
	}
}