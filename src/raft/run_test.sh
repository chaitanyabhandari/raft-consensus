#!/bin/bash

testName=$1
numIterations=$2
testsPerIteration=$3
for (( i=1; i<=$numIterations; i++ ))
do
        for (( j=1; j<=$testsPerIteration; j++ ))
        do
                go test -v -run ${testName} > ${testName}_${i}_${j}.log;
                res=$(cat ${testName}_${i}_${j}.log | grep "Passed")
                if [ ! -z "${res}" ]
                then
                        echo "${testName}_${i}_${j}.log passed!"
                        rm -rf ${testName}_${i}_${j}.log
                else
                echo "${testName}_${i}_${j}.log failed! Retaining log."
fi
        done;
done