// TODO: do a control check (e.g. calculate the total of ordedred and set occurences with and withouth search query)
// combineCountList.map(m=>m._2._2).reduce(_+_)
// val totalList = orderedList.map(m=>m._1).reduce(_+_)
package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils;
import java.io.File

class SearchTopicsSuite extends FunSuite with BeforeAndAfter {

	val rawFolder = "/home/igor/test/test/"
	val resultsFolder = "/home/igor/test/unit/"
	val minSeq = 2
	val maxSeq = 5
	val maxResults = 500
	val action = "SearchTopics"
	val processingOutputType = "JSON"

	var actionRunner: ActionRunner = null
	var sc: SparkContext = null

	before {

		FileUtils.deleteDirectory(new File(resultsFolder))

		val config = new SparkConf().setMaster("local").setAppName("Processor")
		sc = new SparkContext(config)

		actionRunner = SearchTopics
		actionRunner.initialize(sc, action, minSeq, maxSeq, rawFolder, resultsFolder, maxResults, processingOutputType)
	}

	after {
		sc.stop()
	}

  test("SearchTopics preprocessing and processing should return correct results.") {

		actionRunner.preprocessToSearchSequences()

		// preprocessing files should have correct results
		val actual = scala.io.Source.fromFile("/home/igor/test/unit/SearchTopics/preprocrocessed/201101/part-00000").getLines().toList.map(_.split("\t").toList).toList
		val expected = List(List("2", "s_pulmonary hypertension", "t_Oph", "t_Oph1", "t_Oph2"), List("2", "s_pulmonary hypertension", "t_Oph", "t_Oph1"), List("2", "s_pulmonary hypertension", "t_Oph"))

		assert(expected == actual)

		actionRunner.process()

		// processing files should have correct results
  }
}