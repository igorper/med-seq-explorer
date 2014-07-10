#!/bin/bash

# spark cluster url
MASTER_URL="spark://linthinka:7077"

# set default action if no -a is provided
action="DrugSequenceGenerator"

while getopts ":a:" opt; do
  case $opt in
    a)
      action=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

echo "Submitting action: $action" >&1

sbt assembly
spark-submit --properties-file spark-conf.prop --master $MASTER_URL --class $action target/scala-2.10/SequenceExplorer-assembly-1.0.jar