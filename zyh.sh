#/bin/bash
sudo rm -rf /home/data/vol1/zyh/*
sudo rm -rf /home/nvme0/zyh/*

date
./ycsbc -db rocksdb -dbpath /home/nvme0/zyh -threads 100 -P ./workloads/workloadzyh.spec -load true -dboption 0 -dbstatistics true | tee ./log/`date +%Y%m%d%H%M%S`.log

sleep 30
date
./ycsbc -db rocksdb -dbpath /home/nvme0/zyh -threads 10 -P ./workloads/workloadzyh.spec -run true -dboption 0 -dbstatistics true | tee ./log/`date +%Y%m%d%H%M%S`.log

date

sleep 60
tmux kill-session -t iostat
