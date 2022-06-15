//
//  Get Info
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//


#ifndef YCSB_C_STATISTICS_H_
#define YCSB_C_STATISTICS_H_

#include <stdio.h>
#include <cstdint>
#include <atomic>
#include <stdlib.h>
#include <thread> 
#include <hdr/hdr_histogram.h>
#include <sys/time.h>
#include <functional>
// #include "timer.h"

namespace ycsbc {

class Statistics {
 public:
  Statistics() {}
  // void Init();

  uint64_t get_now(){
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  // 统计IOPS, 原子增加减少以及输出.
  void AddIOPS() { iops_++; } 
  void SetIOPS(uint64_t start) { iops_.store(start); }

  void Init(){
    log_ = fopen("./statistics.log","a+");
    if(log_==NULL) {
      printf("open log file error\n");
      exit(1);
    }
    fprintf(log_, "time   IOPS   #mean    95th    99th    99.99th\n");

    mutex_.lock();
    // 统计信息 hdr初始化
    int r = hdr_init(1,INT64_C(3600000000),3,&hdr_);
    if(r!=0) {
      printf("new hdr_ error \n return %d\n",r);
      exit(1);
    }

    r = hdr_init(1,INT64_C(3600000000),3,&hdr_last_1s_);
    if(r!=0) {
      printf("new hdr_last_1s_ error \n return %d\n",r);
      exit(1);
    }

    r = hdr_init(1,INT64_C(3600000000),3,&hdr_read_);
    if(r!=0) {
      printf("new hdr_read_ error \n return %d\n",r);
      exit(1);
    }

      r = hdr_init(1,INT64_C(3600000000),3,&hdr_insert_);
    if(r!=0) {
      printf("new hdr_insert_ error \n return %d\n",r);
      exit(1);
    }

      r = hdr_init(1,INT64_C(3600000000),3,&hdr_update_);
    if(r!=0) {
      printf("new hdr_update_ error \n return %d\n",r);
      exit(1);
    }

    r = hdr_init(1,INT64_C(3600000000),3,&hdr_scan_);
    if(r!=0) {
      printf("new hdr_scan_ error \n return %d\n",r);
      exit(1);
    }

    r = hdr_init(1,INT64_C(3600000000),3,&hdr_rmw_);
    if(r!=0) {
      printf("new hdr_rmw_ error \n return %d\n",r);
      exit(1);
    } 


    r = hdr_init(1,INT64_C(3600000000),3,&hdr_iops_);
    if(r!=0) {
      printf("new hdr_iops_ error \n return %d\n",r);
      exit(1);
    } 

    mutex_.unlock(); 

    SetIOPS(0);
    OutPut_ = new std::thread(std::bind(&ycsbc::Statistics::PrintThread, this));

  }

  void PrintThread(){
    uint64_t tiktok_start = get_now();
    uint64_t tiktoks = 0;
    while(true) {
      if(stop_output_) 
        break;
      tiktoks = get_now() - tiktok_start; 
      if (tiktoks >= 1000000) { 
          tiktok_start = get_now();
          tiktoks = 0; 
          Print(); 
      }
    }
  }

  void Print(){
      // IOPS 
      uint64_t iops = iops_.exchange(0); 
      mutex_.lock();
      fprintf(log_, "%8.2f %8lu %-11.2lf %-8ld %-8ld %-8ld %-8ld\n",
                get_now()*1.0/1000000,
                iops,
                hdr_mean(hdr_last_1s_),
                hdr_value_at_percentile(hdr_last_1s_, 95),
                hdr_value_at_percentile(hdr_last_1s_, 99),
                hdr_value_at_percentile(hdr_last_1s_, 99.9),
                hdr_value_at_percentile(hdr_last_1s_, 99.99));
      hdr_reset(hdr_last_1s_);
      hdr_record_value(hdr_iops_, iops);
      mutex_.unlock();
      fflush(log_);
  }

  void RecordTime(int op,uint64_t tx_xtime) {
    if(tx_xtime > 3600000000) {
      printf("too large tx_xtime %lu\n",tx_xtime);
      return ;
    }

    mutex_.lock();
    hdr_record_value(hdr_, tx_xtime);
    hdr_record_value(hdr_last_1s_, tx_xtime);

    if(op == 1){
      hdr_record_value(hdr_read_, tx_xtime);
    } else if(op == 2) {
      hdr_record_value(hdr_insert_, tx_xtime);
    } else if(op == 3) {
      hdr_record_value(hdr_update_, tx_xtime);
    } else if(op == 4) {
      hdr_record_value(hdr_scan_, tx_xtime);
    } else if(op == 5) {
      hdr_record_value(hdr_rmw_, tx_xtime);
    } else {
      printf("op error\n");
    }
    mutex_.unlock();
  }

  ~Statistics() {

    fprintf(log_,"IOPS info is \n");
    fprintf(log_, "%-8ld %-8ld %-8ld %-8ld\n", 
                hdr_mean(hdr_iops_),
                hdr_value_at_percentile(hdr_iops_, 95),
                hdr_value_at_percentile(hdr_iops_, 99),
                hdr_value_at_percentile(hdr_iops_, 99.9) );
    printf("the iops info is : \n");
    printf("mean: %ld, P95: %ld, P99: %ld, P999: %ld",hdr_mean(hdr_iops_),
                hdr_value_at_percentile(hdr_iops_, 95),
                hdr_value_at_percentile(hdr_iops_, 99),
                hdr_value_at_percentile(hdr_iops_, 99.9));
    fclose(log_);
    free(hdr_);
    free(hdr_last_1s_);
    free(hdr_read_);
    free(hdr_insert_);
    free(hdr_update_);
    free(hdr_scan_);
    free(hdr_rmw_);
    stop_output_ = true;
    if(OutPut_){
      OutPut_->join();
      delete OutPut_;
      OutPut_=NULL;
    }
  }

private:
  std::atomic<uint64_t> iops_; // 统计IOPS
  FILE* log_;
  
  // 使用 hdr 进行 时延信息统计
  std::mutex mutex_;
  struct hdr_histogram* hdr_ = NULL;
  struct hdr_histogram* hdr_last_1s_ = NULL;
  struct hdr_histogram* hdr_read_= NULL;
  struct hdr_histogram* hdr_insert_= NULL;
  struct hdr_histogram* hdr_update_ = NULL;
  struct hdr_histogram* hdr_scan_ = NULL;
  struct hdr_histogram* hdr_rmw_ = NULL;
  struct hdr_histogram* hdr_iops_ = NULL;
  
  std::thread* OutPut_; // 输出线程，每秒输出一次。
  bool stop_output_= false;

};

} // ycsbc

#endif  // YCSB_C_STATISTICS_H_