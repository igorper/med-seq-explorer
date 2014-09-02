package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import java.nio.file.{Paths, Files}

/*
* A base class defining the structure for each Task. Contanins some common code, such
* as looping through all input files.
*/
trait ActionRunner {

		// prefixes
		protected val SearchPrefix = "s_"
		protected val TopicPrefix = "t_"

		protected var sparkContext : SparkContext = null
		protected var action : String = null
		protected var minSeq : Int = 0
		protected var maxSeq : Int = 0
		protected var preprocessingFolder : String = null
		protected var input : String = null

		// used to pass SparkContext and other configuration objects/properties
		def initialize(sc: SparkContext, action: String, minSeq: Int, maxSeq: Int, input: String, output: String) = {
			sparkContext = sc
			this.action = action
			this.minSeq = minSeq
			this.maxSeq = maxSeq
			this.preprocessingFolder = output + "/" + action
			this.input = input
		}

		// this one should only loop the raw files
		// this one should only be overriden if you want to somehow change the logic of getting
		// documents to process
		def preprocessToSearchSequences() = {				
			if(Files.exists(Paths.get(this.preprocessingFolder))){
				println("Preprocessing folder already esists. Skipping preprocessing.")

			} else {

				val allMonths = new java.io.File(input).listFiles.filter(_.getName.endsWith(".txt")).map(_.getName.slice(0,6)).distinct
				var month = null
				for(month <- allMonths){

					// TODO: Do preprocessing only if output folder does not exist yet (to do preprocessing again the output folder should
					// removed or a force flag should be set)
					val loadPath = this.input + month + "*.txt"
					val outputPath = this.preprocessingFolder + "/" + month
					println("Processing action '" + action + "' for: " + loadPath)

					doPreprocessing(loadPath, outputPath)
				}
			}
		}

		def process()

	  // This method performs all the preprocessing and should be overriden by each individual task,
	  // as each task usually preprocesses files in it's own manner. Preprocessing is currently needed
	  // to load all individual data files and extract only sequences relevant to our analysis.
	  def doPreprocessing(inputPath: String, outputPath: String)
	}