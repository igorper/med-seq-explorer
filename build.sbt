name := "SequenceExplorer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0" % "provided"

libraryDependencies += "org.skife.com.typesafe.config" % "typesafe-config" % "0.3.0"

// used for the FastDateFormat with parsing capabilities
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3"

retrieveManaged := true