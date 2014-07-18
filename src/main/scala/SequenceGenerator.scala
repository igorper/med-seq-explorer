package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory
import java.util.Date

object SequenceGenerator {

	def main(args: Array[String]) {
		val SearchPrefix = "S_"
		val TopicPrefix = "T_"

		// start timing execution
		val t0 = System.nanoTime()

		val conf = ConfigFactory.load
		val input = conf.getString("input");
		val output = conf.getString("outputFolder")
		val minSeq = conf.getInt("minSequence")
		val maxSeq = conf.getInt("maxSequence")

		val config = new SparkConf()
		.setAppName("SequenceGenerator")
		
		val sc = new SparkContext(config)

		// get a list with all the year + month part of the file names
		// files are named in the following format YYYYMMDD (e.g. 20110102.txt)
		val allMonths = new java.io.File(input).listFiles.filter(_.getName.endsWith(".txt")).map(_.getName.slice(0,6)).distinct
		var month = null
		for(month <- allMonths){
			val loadPath = input + month + "*.txt"
			println("Processing " + loadPath)

			// read data
			//val file = sc.textFile("/home/pernek/UpToDate/20110101.txt")
			val file = sc.textFile(loadPath)

			// remove lines that contain the header string (it could be many of them
			// as different files could be loaded each starting with the header)
			// (the string starts with 'Session ID')
			val noHeaderFile = file.filter(!_.startsWith("Session ID"))

			// remove lines that do not contain at least 7 columns
			var removedShort = noHeaderFile.map(line => line.split("\t")).filter(n=>n.length >= 7)

			// keep only TopicView/full ans Search/Lucene
			// store as (sessionID, topicView, topicTitle)
			val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full") || n(3).contains("Search/Lucene")).map(m => (m(0), m(3), if(m(3) == "Search/Lucene") SearchPrefix + m(5) else TopicPrefix + m(6)))

			// group nodes by sessionID
			val groupedBySession = topicFullSessions.groupBy(_._1)

			// for each session create a sequence of text with encoded event type (ditching sessionID)
			val sequences = groupedBySession.map { case (sessionID, nodes) => nodes.map(_._3) }

			// generate all sequence combinations for sessions
			// (for now we just constrain max sequnce length to maxSize due to possible large sessions => ~1k nodes)
			val sequenceCombinations = sequences.map(sequence => (minSeq to math.min(sequence.size, maxSeq)).map(winSize => sequence.sliding(winSize)).flatMap(seqIter => seqIter)).flatMap(iter => iter)

			// keep only sequences that start with a search query, which is the only search query in the sequence
			var filteredSequences = sequenceCombinations.map(m => m.toList).filter(f => f(0).startsWith(SearchPrefix) && f.count(i => i.startsWith(SearchPrefix)) == 1)

			// count combinations
			val sequenceCombinationsCounts =filteredSequences.map(s => (s, 1)).reduceByKey(_+_)

			// filter out combination that occur only once in the file
			// TODO: this is metodologically somehow vague (we remove subsequences that appear only once in the file,
			// although if the subsequence appears more than once in another file it will be present in the results -
			// but the number will be off). We introduced this as we were able to significantly shrink the size of the results
			// and made the processing more stable.
			val sequenceCombinationsFiltered = sequenceCombinationsCounts.filter{ case (key, count) => count > 1}

			// prepare combination to be saved to a TSV file (count should be first, the combination should follow)
			val reversedSequenceCombinationsCounts = sequenceCombinationsFiltered.map{ case (comb, count) => count + "\t" + comb.mkString("\t") }

			// save the combinations to a single file 
			reversedSequenceCombinationsCounts.coalesce(1)saveAsTextFile(output + month)
		}

		// stop timing execution
		val t1 = System.nanoTime()

		println("##########")
		printf("Processed '%s' in %d seconds.\n", input, (t1 - t0)/1000000000)
		println("##########")
	}
}
