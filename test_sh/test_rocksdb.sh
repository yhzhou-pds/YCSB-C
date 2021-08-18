#/bin/bash

workload="./workloads/workloada.spec"
dbpath="/mnt/AEP1/ROCKSDB"


if [ -n "$dbpath" ];then
    rm -f $dbpath/*
fi
echo "load"
./ycsbc -db rocksdb -dbpath $dbpath -threads 1 -P $workload -load true -dboption 0

echo "run"
./ycsbc -db rocksdb -dbpath $dbpath -threads 1 -P $workload -run true -dboption 0
