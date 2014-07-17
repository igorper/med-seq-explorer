#!/bin/bash

input_folder="/scratch/users/pernek/results/search_tseq_nint_*/"
tmp_folder="/scratch/users/pernek/results/tmp/"

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
