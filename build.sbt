name := "SequenceExplorer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0" % "provided"

libraryDependencies += "org.skife.com.typesafe.config" % "typesafe-config" % "0.3.0"

// used for the FastDateFormat with parsing capabilities
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"

retrieveManaged := true