# Yahoo! Cloud System Benchmark
# Workload C: Read only
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#                        
#   Read/update ratio: 100/0
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian
fieldcount=1
fieldlength=256

recordcount=10000000
operationcount=1000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0.86
updateproportion=0
scanproportion=0
insertproportion=0.14

requestdistribution=zipfian

sine_rate=true
sine_a=1000
sine_b=0.073
sine_d=3000
sine_mix_rate_interval_milliseconds=50
