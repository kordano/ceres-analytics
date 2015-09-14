#!/bin/bash
for i in `seq 10 31`;
do
    sh import-date.sh 2015-03-$i
done  
