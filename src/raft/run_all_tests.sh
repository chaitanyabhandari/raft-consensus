#!/bin/bash

# testName=$1
numIterations=$1
testsPerIteration=$2

for (( i=1; i<=$numIterations; i++ ))
do
	for (( j=1; j<=$testsPerIteration; j++ ))
        do
                go test -race > all_${i}_${j}.log;
                res=$(cat all_${i}_${j}.log | grep "Passed")
		if [ ! -z "${res}" ]
                then
                        echo "all_${i}_${j}.log passed!"
                        rm -rf all_${i}_${j}.log
                else
                echo "all_${i}_${j}.log failed! Retaining log."
fi
        done;
done
