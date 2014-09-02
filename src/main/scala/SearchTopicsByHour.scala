package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.commons.lang3.time.FastDateFormat

object SearchTopicsByHour extends ActionRunner {

	protected val HourPrefix = "h_"

	val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

	def process() = {
		println("SearchTopicsByHour processor")	
	}

	override def doPreprocessing(loadPath: String, outputPath: String) = {
		// read data
		val file = sparkContext.textFile(loadPath)

		// remove lines that contain the header string (it could be many of them
		// as different files could be loaded each starting with the header)
		// (the string starts with 'Session ID')
		val noHeaderFile = file.filter(!_.startsWith("Session ID"))

		// remove lines that do not contain at least 7 columns
		var removedShort = noHeaderFile.map(line => line.split("\t")).filter(n=>n.length >= 7)

		// filter out only TopicView/full and Search/Lucene event types
		// store as (sessionID, date, eventType, eventText)
		val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full") || n(3).contains("Search/Lucene")).map(m => (m(0), m(4), m(3), if(m(3) == "Search/Lucene") SearchPrefix + m(5) else TopicPrefix + m(6)))

		// group nodes by sessionID
		val groupedBySession = topicFullSessions.groupBy(_._1)

		// keep only (user, ip address, timestamp, session sequence)
		val sequences = groupedBySession.map(m=> ( HourPrefix + dateFormat.parse(m._2.toList(0)._2).getHours().toString, m._2.map(x => x._4)))

		// this is a string thing that has to be done for min and max sequence values to be used correctly in RDD
		// (otherwise, if used as var, they are set to 0 - probably init value)
		val minSeqVal = this.minSeq
		val maxSeqVal = this.maxSeq

		// slide a window to get combinations of sequences (between min and max sequence size)
		val sequenceCombinations = sequences.map(m => (minSeqVal to math.min(m._2.size, maxSeqVal)).map(winSize =>  m._2.sliding(winSize)).flatMap(seqIter => seqIter.map(x=>(m._1, x)))).flatMap(y=>y)

		// keep only sequences that start with a search query
		// also there should be only one search query in the sentence
		val filteredSequences = sequenceCombinations.map(m => (m._1, m._2.toList)).filter(f => f._2(0).startsWith(SearchPrefix) && f._2.count(i => i.startsWith(SearchPrefix)) == 1)

		// count combinations
		val sequenceCombinationsCounts = filteredSequences.map(s => (s, 1)).reduceByKey(_+_)

		// filter out combination that occur only once in the file
		// TODO: this is metodologically somehow vague (we remove subsequences that appear only once in the file,
		// although if the subsequence appears more than once in another file it will be present in the results -
		// but the number will be off). We introduced this as we were able to significantly shrink the size of the results
		// and made the processing more stable.
		val sequenceCombinationsFiltered = sequenceCombinationsCounts.filter{ case (key, count) => count > 1}

		// prepare combination to be saved to a TSV file (count should be first, the combination should follow)
		val reversedSequenceCombinationsCounts =  sequenceCombinationsFiltered.map{ case (comb, count) => count + "\t" + comb._1 + "\t" + comb._2.mkString("\t") }

		// save the combinations to a single file 
		reversedSequenceCombinationsCounts.coalesce(1).saveAsTextFile(outputPath)
	}
}