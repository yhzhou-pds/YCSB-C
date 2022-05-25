sudo rm -rf /home/nvme0/zyh/*

date

./ycsbc -db rocksdb -dbpath /home/nvme0/zyh -threads 10 -P ./workloads/workloadzyh.spec -load true -dboption 0 -dbstatistics true | tee load`date +%Y%m%d%H%M%S`.log

date 

./ycsbc -db rocksdb -dbpath /home/nvme0/zyh -threads 1 -P ./workloads/workloadzyh.spec -run true -dboption 0 -dbstatistics true | tee run`date +%Y%m%d%H%M%S`.log
