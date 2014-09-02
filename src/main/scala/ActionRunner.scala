package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._

/*
* A base class defining the structure for each Task.
*/
trait ActionRunner {

		// prefixes
		protected val SearchPrefix = "s_"
		protected val TopicPrefix = "t_"

		protected var sparkContext : SparkContext = null
		protected var action : String = null
		protected var minSeq : Int = 0
		protected var maxSeq : Int = 0
		// protected val minSeq = 2
		// protected val maxSeq = 5

		// used to pass SparkContext and other configuration objects/properties
		def initialize(sc: SparkContext, action: String, minSeq: Int, maxSeq: Int) = {
			sparkContext = sc
			this.action = action
			this.minSeq = minSeq
			this.maxSeq = maxSeq
		}

		// this one should only loop the raw files
		// this one should only be overriden if you want to somehow change the logic of getting
		// documents to process
		def preprocessToSearchSequences(input: String, output: String) = {
			println("ActionRunner preprocessToSearchSequences")

			val allMonths = new java.io.File(input).listFiles.filter(_.getName.endsWith(".txt")).map(_.getName.slice(0,6)).distinct
			var month = null
			for(month <- allMonths){

				val loadPath = input + month + "*.txt"
				val outputPath = output + action + "/" + month
				println("Processing action '" + action + "' for: " + loadPath)

				doPreprocessing(loadPath, outputPath)
			}
		}

		def process()

	  // this one should perform any preprocessing transformations
	  // it would make sense to override this one
	  def doPreprocessing(inputPath: String, outputPath: String)
	}