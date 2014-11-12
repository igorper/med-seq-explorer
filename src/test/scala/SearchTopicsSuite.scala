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
import scala.io.Source

import com.typesafe.config.ConfigFactory

class SearchTopicsSuite extends FunSuite with BeforeAndAfter {

	/*
		Test configuration. Currently, this is defined on the class level
		as we are running only one test. If multiple tests will be added,
		configuration can be moved to each individual test.
	*/
	val minSeq = 2
	val maxSeq = 5
	val maxResults = 500
	val action = "SearchTopics"
	val processingOutputType = "JSON"

	var rawFolder: String = null
	var resultsFolder: String = null
	var actionRunner: ActionRunner = null
	var sc: SparkContext = null

	before {
		// read configuration
		val conf = ConfigFactory.load("test-settings")
		val testTempFolder = conf.getString("testTempFolder")
		rawFolder = testTempFolder + "test/"
		resultsFolder = testTempFolder + "unit/"

		// remove any previous results
		FileUtils.deleteDirectory(new File(resultsFolder))

		// create a local spark context
		val config = new SparkConf().setMaster("local").setAppName("Processor")
		sc = new SparkContext(config)

		// initialize SearchTopics action
		actionRunner = SearchTopics
		actionRunner.initialize(sc, action, minSeq, maxSeq, rawFolder, resultsFolder, maxResults, processingOutputType)
	}

	after {
		sc.stop()
	}
/*
  test("SearchTopics preprocessing and processing should return correct results.") {

  	// check preprocessing
		actionRunner.preprocessToSearchSequences()

		// preprocessing files should have correct results
		val actual = Source.fromFile(resultsFolder + "SearchTopics/preprocrocessed/201101/part-00000").getLines().toList.map(_.split("\t").toList).toList
		val expected = List(List("2", "s_pulmonary hypertension", "t_Oph", "t_Oph1", "t_Oph2"), List("2", "s_pulmonary hypertension", "t_Oph", "t_Oph1"), List("2", "s_pulmonary hypertension", "t_Oph"))

		assert(expected == actual)

		// check processing (assert sequences of different lengths)
		actionRunner.process()

		// processing files should have correct results
		// length 2
		val actualProcessed2 = Source.fromFile(resultsFolder + "SearchTopics/processed/2/part-00000").getLines().toList
		val expectedProcessed2 = List("{sequenceCount: 2, sequence: 'oph, oph1', searchQueries: [{query: 'pulmonary hypertension', count: 2}]}")
		assert(expectedProcessed2 == actualProcessed2)

		// length 3
		val actualProcessed3 = Source.fromFile(resultsFolder + "SearchTopics/processed/3/part-00000").getLines().toList
		val expectedProcessed3 = List("{sequenceCount: 2, sequence: 'oph, oph1, oph2', searchQueries: [{query: 'pulmonary hypertension', count: 2}]}")
		assert(expectedProcessed3 == actualProcessed3)
		
		// length 4
		val actualProcessed4 = Source.fromFile(resultsFolder + "SearchTopics/processed/4/part-00000").getLines().toList
		val expectedProcessed4 = List()
		assert(expectedProcessed4 == actualProcessed4)
  }*/
}
