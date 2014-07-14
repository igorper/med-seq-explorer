import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.typesafe.config.ConfigFactory

/*
- reads generated sequences
- filters out all but triplets satisfying the following rule (case insensitive):
1. two items contain "drug information" string, one doesn't
2. two items containing "drug information" string should not be the same
*/
object DrugSequenceGenerator {

	def main(args: Array[String]) {
		// start timing execution
		val t0 = System.nanoTime()

		val conf = ConfigFactory.load		
		// TODO: rename this config element to smth. like 'preprocessed_folder'
		val prepFolder = conf.getString("outputFolder")

		// stop timing execution
		val t1 = System.nanoTime()

		println("##########")
		printf("Processed '%s' in %d seconds.\n", "input", (t1 - t0)/1000000000)
		println("##########")
	}
}
