sudo rm -rf ~/gp2/gp2*/*
sudo rm -rf ~/nvme/zyh/*

date

./ycsbc -db rocksdb -dbpath /home/ubuntu/nvme/zyh -threads 10 -P ./workloads/workloadzyh.spec -load true -dboption 2 -dbstatistics true | tee load`date +%Y%m%d%H%M%S`.log

date

./ycsbc -db rocksdb -dbpath /home/ubuntu/nvme/zyh -threads 10 -P ./workloads/workloadzyh.spec -run true -dboption 2 -dbstatistics true | tee run`date +%Y%m%d%H%M%S`.log

date

