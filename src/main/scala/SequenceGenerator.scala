package net.pernek.medexplorer

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object SequenceGenerator {

	val SearchPrefix = "s_"
	val TopicPrefix = "t_"
	val HourPrefix = "h_"

	val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

/* TODO
* - generate standalone classes with two functions (process and preprocess)
* - each class should be for a specific task (e.g. SearchTopicCount or SearchTopicCountByHour)
*/
	def getSequenceCombinations(removedShort : org.apache.spark.rdd.RDD[Array[String]], action : String, minSeq : Int, maxSeq: Int) : org.apache.spark.rdd.RDD[String] = {
			// refactor to enum or at least a list of strings
			if(action == "SearchTopicsCount"){
				val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full") || n(3).contains("Search/Lucene")).map(m => (m(0), m(3), if(m(3) == "Search/Lucene") SearchPrefix + m(5) else TopicPrefix + m(6)))

				// group nodes by sessionID
				val groupedBySession = topicFullSessions.groupBy(_._1)

				val sequences = groupedBySession.map { case (sessionID, nodes) => nodes.map(_._3) }

				val sequenceCombinations = sequences.map(sequence => (minSeq to math.min(sequence.size, maxSeq)).map(winSize => sequence.sliding(winSize)).flatMap(seqIter => seqIter)).flatMap(iter => iter)

				// keep only sequences that start with a search query, which is the only search query in the sequence
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
				return sequenceCombinationsFiltered.map{ case (comb, count) => count + "\t" + comb.mkString("\t") }


			} else if (action == "SearchTopicsCountByHour") {
				// store as (sessionID, topicView, topicTitle)
				val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full") || n(3).contains("Search/Lucene")).map(m => (m(0), m(4), m(3), if(m(3) == "Search/Lucene") SearchPrefix + m(5) else TopicPrefix + m(6)))

				// group nodes by sessionID
				val groupedBySession = topicFullSessions.groupBy(_._1)

				// TODO: split variables have to be preprocessed here already (IPs have to be transformed, dates have to be transformed, etc.)

				// keep only (user, ip address, timestamp, session sequence)
				val sequences = groupedBySession.map(m=> ( HourPrefix + dateFormat.parse(m._2.toList(0)._2).getHours().toString, m._2.map(x => x._4)))

				// generate all sequence combinations for sessions
				// (for now we just constrain max sequnce length to maxSize due to possible large sessions => ~1k nodes)
				val sequenceCombinations = sequences.map(m => (minSeq to math.min(m._2.size, maxSeq)).map(winSize =>  m._2.sliding(winSize)).flatMap(seqIter => seqIter.map(x=>(m._1, x)))).flatMap(y=>y)

				// keep only sequences that start with a search query, which is the only search query in the sequence
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
				return sequenceCombinationsFiltered.map{ case (comb, count) => count + "\t" + comb._1 + "\t" + comb._2.mkString("\t") }
			}

			return null
		}

		def main(args: Array[String]) {
		// start timing execution
		val t0 = System.nanoTime()

		val conf = ConfigFactory.load
		val input = conf.getString("input");
		val output = conf.getString("outputFolder")
		val minSeq = conf.getInt("minSequence")
		val maxSeq = conf.getInt("maxSequence")

		/* possible:
		* - SearchTopicsCount
		* - SearchTopicsCountByHour
		*/

		val action = conf.getString("action")

		val config = new SparkConf()
		.setAppName("SequenceGenerator")
		
		val sc = new SparkContext(config)

		// get a list with all the year + month part of the file names
		// files are named in the following format YYYYMMDD (e.g. 20110102.txt)
		val allMonths = new java.io.File(input).listFiles.filter(_.getName.endsWith(".txt")).map(_.getName.slice(0,6)).distinct
		var month = null
		for(month <- allMonths){
			val loadPath = input + month + "*.txt"
			println("Processing action '" + action + "' for: " + loadPath)

			// read data
			//val file = sc.textFile("/home/pernek/UpToDate/20110101.txt")
			val file = sc.textFile(loadPath)

			// remove lines that contain the header string (it could be many of them
			// as different files could be loaded each starting with the header)
			// (the string starts with 'Session ID')
			val noHeaderFile = file.filter(!_.startsWith("Session ID"))

			// remove lines that do not contain at least 7 columns
			var removedShort = noHeaderFile.map(line => line.split("\t")).filter(n=>n.length >= 7)

			//val sequenceCombinations = getSequenceCombinations(removedShort, action, minSeq, maxSeq)

			// store as (sessionID, topicView, topicTitle)
			//val topicFullSessions = removedShort.filter(n => n(3).contains("TopicView/full") || n(3).contains("Search/Lucene")).map(m => (m(0), m(1), m(2), m(4), m(3), if(m(3) == "Search/Lucene") SearchPrefix + m(5) else TopicPrefix + m(6)))

			// group nodes by sessionID
			//val groupedBySession = topicFullSessions.groupBy(_._1)

			// TODO: split variables have to be preprocessed here already (IPs have to be transformed, dates have to be transformed, etc.)

			// keep only (user, ip address, timestamp, session sequence)
			//val sequences = groupedBySession.map(m=> (m._2.toList(0), m._2.map(x => x._6))).map(m=> ((m._1._2, m._1._3, m._1._4), m._2))

			// generate all sequence combinations for sessions
			// (for now we just constrain max sequnce length to maxSize due to possible large sessions => ~1k nodes)
			//val sequenceCombinations = sequences.map((key, sequence) => (minSeq to math.min(sequence.size, maxSeq)).map(winSize => sequence.sliding(winSize)).flatMap(seqIter => seqIter)).flatMap(iter => iter)

			
			val reversedSequenceCombinationsCounts = getSequenceCombinations(removedShort, action, minSeq, maxSeq)

			// save the combinations to a single file 
			reversedSequenceCombinationsCounts.coalesce(1)saveAsTextFile(output + action + "/" + month)
		}

		// stop timing execution
		val t1 = System.nanoTime()

		println("##########")
		printf("Processed '%s' in %d seconds.\n", input, (t1 - t0)/1000000000)
		println("##########")
	}
}
