#/bin/bash

workload="./workloads/$1.spec"
dbpath="/home/ubuntu/ssd/data/log"
datapath1="/home/ubuntu/ssd/data/data1"

rm -rf $dbpath
rm -rf $datapath1

if [ -n "$dbpath" ];then
	    rm -f $dbpath/*
fi
./ycsbc -db rocksdb -dbpath $dbpath -threads 1 -P $workload -load true -dboption $2 -dbstatistics true

du -sbm $datapath1

echo "run"
./ycsbc -db rocksdb -dbpath $dbpath -threads 1 -P $workload -run true -dboption $2 -dbstatistics true
echo "run"

