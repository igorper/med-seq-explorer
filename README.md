# Medical sequence explorer

A suite of tools for mining sequences from large med files.

## Running instructions

1. Before running the scripts create the _application.json_ config file. You can just copy the _application.json.default_ file.

    `cp src/main/resources/application.json.default src/main/resources/application.json`

1. Set relevant configuration in _application.json_

    - **input**: name of the input file with session records
    - **sessionThreshold**: max number of seconds between two consequtive actions of the same sessions. Sessions that exceed the specified threshold will be split in multiple subsessions.
    
1. Use **sbt** to run the application `sbt run`.

## Run in development
To run the application in **spark-shell** make sure required libraries are added to the classpath (or included with ADD_JARS). Check _build.sbt_ for the list of dependencies.

## TODO:
- SparkContext should be created with a configurable cluster