#!/bin/bash
for i in `seq 1 9`;
do
    d=2015-03-0$i
    sh import-date.sh $d
    echo "IMPORTING $d"
done  

for i in `seq 10 31`;
do
    d=2015-03-$i
    sh import-date.sh $d
    echo "IMPORTING $d"
done  

for i in `seq 1 9`;
do
    d=2015-04-0$i
    sh import-date.sh $d
    echo "IMPORTING $d"
done  

for i in `seq 10 30`;
do
    d=2015-04-0$i
    sh import-date.sh $d
    echo "IMPORTING $d"
done  

for i in `seq 1 9`;
do
    d=2015-05-0$i
    sh import-date.sh $d
    echo "IMPORTING $d"
done  

