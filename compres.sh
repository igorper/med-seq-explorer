#!/bin/bash

input_folder="/scratch/users/pernek/results/search_tseq_nint_*/"
tmp_folder="/scratch/users/pernek/results/tmp/"

while getopts ":i:" opt; do
  case $opt in
    i)
      input_folder=$OPTARG
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

echo "Compressing folder $input_folder"

mkdir -p $tmp_folder

for d in $input_folder ; do
	filename=$(basename $d)
	echo $filename

	cp "${d}part-00000" $tmp_folder$filename
done

cur_folder=$(pwd)
echo $cur_folder
cd $tmp_folder
zip -r "results.zip" .
mv "results.zip" $cur_folder

rm -r $tmp_folder
