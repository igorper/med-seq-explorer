package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.commons.lang3.time.FastDateFormat

object SearchTopicsByHour extends ActionRunner {

	protected val HourPrefix = "h_"

	val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

	val ListPrintFormatter = (m:(Int, (Iterable[String], List[(String, Int, Int)]))) => "*** [" + m._1 + "]\n" + 
			m._2._1.mkString(" --> ") + ":\n" + m._2._2.map(x=> "- " + x._1 + ", " + x._3 + ", " + x._2 ).mkString("\n") + "\n"
	val JSONFormatter = (m:(Int, (Iterable[String], List[(String, Int, Int)]))) => 
		"{\"sequenceCount\": "+ m._1 +", \"sequence\": \"" + m._2._1.mkString(";") + 
		"\", \"searchQueries\": [" + m._2._2.map(o => "{\"query\": \"" + o._1 + "\", \"count\": " + o._2 + ", \"hour\": "+ o._3 +"}").mkString(",") + "]},"
	val RemovePrefix = (x:(Int, (Iterable[String], List[(String, Int, Int)]))) => (x._1,(x._2._1.map(t=>t.slice(2,t.size)), x._2._2.map(s=>(s._1.slice(2,s._1.size),s._2, s._3))))
	val SplitToTopicAndSearch = (m:(Iterable[String], Int)) => (m._1.filter(f=>f.startsWith(TopicPrefix)), m._1.filter(f=>f.startsWith(SearchPrefix)).head, m._1.filter(f=>f.startsWith(HourPrefix)).head, m._2)

	override def doProcessing() = {
		val file = sparkContext.textFile(this.preprocessingFolder + "*")

		// Split count and sequence (at this point still search + topics). Also lowercase the sequence.
		val reducedSequences = file.map(m => m.split("\t")).map(m => (m.slice(1,m.size).map(i=>i.toLowerCase), m(0).toInt))

		// Reduce the sequences (this will take care of joining same sequences from different files).
		val reducedSequencesList = reducedSequences.map(m => (m._1.toList, m._2)).reduceByKey(_+_)

		// split search and topic sequence and cache the results
		val countSeparateList = reducedSequencesList.map(SplitToTopicAndSearch).cache
		//println(countSeparateList.first)

		val numberOfPreprocLines = file.count
		println("Started processing. There are " + numberOfPreprocLines + " preprocessed lines to be processed.")
		if(numberOfPreprocLines == 0) {
			println("No data to process. Exiting.")
		} else {
			// loop different sequence lengths
			for(l <- minSeq to (maxSeq - 1)) {
				// pick only sequences of the particular length
	 			val combineCountList = countSeparateList.filter(f => f._1.size == l).map(i=> (i._1.toList, (i._2, i._3.slice(2,i._3.size).toInt, i._4)))

				val itemNum = combineCountList.count
				println("Processing " + itemNum + " counts for sequnece length " + l)
	
				// the main part of the logic. count search occurences for each topic sequence.
				val orderedList = combineCountList.combineByKey( (v) => (List((v._1, v._3, v._2)),v._3),  (a: (List[(String,Int, Int)],Int), v) => (a._1 ++ List((v._1,v._3, v._2)), a._2 + v._3), (b: (List[(String,Int,Int)],Int), c: (List[(String,Int,Int)], Int)) => ((b._1 ++ c._1).sortBy(x=> (x._1, x._3)),b._2 + c._2)).map(m=>(m._2._2,(m._1,m._2._1))).sortByKey(false)

				// save to one file
				if(proccessingOutputType == "TEXT") {
					sparkContext.makeRDD(orderedList.map(RemovePrefix).map(ListPrintFormatter).take(maxResults)).coalesce(1).saveAsTextFile(this.processingFolder + l)
				} else if(proccessingOutputType == "JSON") {
					// TODO: currently, we have to manually append array brackets to output json ([])
					sparkContext.makeRDD(orderedList.map(RemovePrefix).map(JSONFormatter).take(maxResults)).coalesce(1).saveAsTextFile(this.processingFolder + l)
				}
  	 	}
		}	
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
		val maxSeqVal = this.maxSeq + 1

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
