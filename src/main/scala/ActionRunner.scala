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
		protected var resultsFolder : String = null
		protected var preprocessingFolder : String = null
		protected var processingFolder : String = null
		protected var input : String = null
		protected var maxResults : Int = 0
		protected var proccessingOutputType : String = null

		// TODO: For now, all possible configuration is passed to init. This is solution is not the best, as not all
		// tasks will need all the configuration. A better solution would be to have a base class that should have
		// the initialize method that should accept a base ConfigOptions object. The
		// object should contain only general stuff, such as sparkContext, input, output. All the other options should
		// go in classes extending ConfigOptions. Childs from ActionRunner should override the initialize method with
		// childs from ConfigOptions.
		// used to pass SparkContext and other configuration objects/properties
		def initialize(sc: SparkContext, action: String, minSeq: Int, maxSeq: Int, input: String, resultsFolder: String, maxResults : Int, proccessingOutputType: String) = {
			sparkContext = sc
			this.action = action
			this.minSeq = minSeq
			this.maxSeq = maxSeq
			this.input = input
			this.maxResults = maxResults
			this.proccessingOutputType = proccessingOutputType

			// store folders
			this.resultsFolder = resultsFolder + action + "/"
			this.preprocessingFolder = this.resultsFolder + "preprocrocessed/"
			this.processingFolder = this.resultsFolder + "processed/"
		}

		// this one should only loop the raw files
		// this one should only be overriden if you want to somehow change the logic of getting
		// documents to process
		def preprocessToSearchSequences() = {				
			println("Using preprocessing folder: " + this.preprocessingFolder)
			if(Files.exists(Paths.get(this.preprocessingFolder))){
				println("Preprocessing folder already esists. Skipping preprocessing.")

			} else {

				val allMonths = new java.io.File(input).listFiles.filter(_.getName.endsWith(".txt")).map(_.getName.slice(0,6)).distinct
				var month = null
				for(month <- allMonths){

					// TODO: Do preprocessing only if output folder does not exist yet (to do preprocessing again the output folder should
					// removed or a force flag should be set)
					val loadPath = this.input + month + "*.txt"
					val outputPath = this.preprocessingFolder + month
					println("Preprocessing action '" + action + "' for: " + loadPath)

					doPreprocessing(loadPath, outputPath)
				}
			}
		}

		def process() = {
			if(Files.exists(Paths.get(processingFolder))){
				println("Processed resultes folder already esists. Skipping processing.")

			} else {
				// TODO: add input and output path here (same as in preprocessing)
				doProcessing()
			}
		}

	  // This method performs all the preprocessing and should be overriden by each individual task,
	  // as each task usually preprocesses files in it's own manner. Preprocessing is currently needed
	  // to load all individual data files and extract only sequences relevant to our analysis.
	  def doPreprocessing(inputPath: String, outputPath: String)

	  def doProcessing()
	}
