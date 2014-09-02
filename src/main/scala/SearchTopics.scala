package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._

object SearchTopics extends ActionRunner {

	def doPreprocessing(loadPath: String, outputPath: String) = {
		// read data
		val file = sparkContext.textFile(loadPath)

		// remove lines that contain the header string (it could be many of them
		// as different files could be loaded each starting with the header)
		// (the string starts with 'Session ID')
		val noHeaderFile = file.filter(!_.startsWith("Session ID"))

		// remove lines that do not contain at least 7 columns
		var removedShort = noHeaderFile.map(line => line.split("\t")).filter(n=>n.length >= 7)

		// filter out only TopicView/full and Search/Lucene event types
		// store as (sessionID, eventType, eventText)
		val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full") || n(3).contains("Search/Lucene")).map(m => (m(0), m(3), if(m(3) == "Search/Lucene") SearchPrefix + m(5) else TopicPrefix + m(6)))

		// group nodes by sessionID
		val groupedBySession = topicFullSessions.groupBy(_._1)

		// ditch sessionID
		val sequences = groupedBySession.map { case (sessionID, nodes) => nodes.map(_._3) }

		// this is a string thing that has to be done for min and max sequence values to be used correctly in RDD
		// (otherwise, if used as var, they are set to 0 - probably init value)
		val minSeqVal = this.minSeq
		val maxSeqVal = this.maxSeq

		// slide a window to get combinations of sequences (between min and max sequence size)
		val sequenceCombinations = sequences.map(sequence => (minSeqVal to math.min(sequence.size, maxSeqVal)).map(winSize => sequence.sliding(winSize)).flatMap(seqIter => seqIter)).flatMap(iter => iter)

		// keep only sequences that start with a search query
		// also there should be only one search query in the sentence
		val filteredSequences = sequenceCombinations.map(m => m.toList).filter(f => f(0).startsWith(SearchPrefix) && f.count(i => i.startsWith(SearchPrefix)) == 1)

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
		reversedSequenceCombinationsCounts.coalesce(1).saveAsTextFile(outputPath)
	}

	def process() = {
		val file = sparkContext.textFile(this.preprocessingFolder)

		println(file.count)
	}
}