sbt assembly && spark-submit --properties-file spark-conf.prop --master spark://linthinka:7077 --class SequenceGenerator target/scala-2.10/SequenceExplorer-assembly-1.0.jar