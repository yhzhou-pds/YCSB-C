//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"

extern uint64_t ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1] ;    //操作个数
extern uint64_t ops_time[ycsbc::Operation::READMODIFYWRITE + 1] ;   //微秒

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) { }
  
  virtual bool DoInsert();
  virtual bool DoTransaction();
  
  virtual ~Client() { }
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();
  
  DB &db_;
  CoreWorkload &workload_;
};

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  uint64_t start_time = get_now_micros();
  int status = db_.Insert(workload_.NextTable(), key, pairs);
  uint64_t op_time = (get_now_micros() - start_time );
  workload_.GetInfo(1,op_time); 
  return (status == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  uint64_t start_time = get_now_micros();
  uint64_t op_time;
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      op_time = (get_now_micros() - start_time );
      ops_time[READ] += op_time;
      ops_cnt[READ]++;
      workload_.GetInfo(2,op_time);
      break;
    case UPDATE:
      status = TransactionUpdate();
      op_time = (get_now_micros() - start_time );
      ops_time[UPDATE] += op_time;
      ops_cnt[UPDATE]++;
      workload_.GetInfo(3,op_time);
      break;
    case INSERT:
      status = TransactionInsert();
      op_time = (get_now_micros() - start_time );
      ops_time[INSERT] += op_time;
      ops_cnt[INSERT]++;
      workload_.GetInfo(1,op_time);
      break;
    case SCAN:
      status = TransactionScan();
      op_time = (get_now_micros() - start_time );
      ops_time[SCAN] += op_time;
      ops_cnt[SCAN]++;
      workload_.GetInfo(4,op_time);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      op_time = (get_now_micros() - start_time );
      ops_time[READMODIFYWRITE] += op_time;
      ops_cnt[READMODIFYWRITE]++;
      workload_.GetInfo(5,op_time);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table, key, len, &fields, result);
  } else {
    return db_.Scan(table, key, len, NULL, result);
  }
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  // const std::string &key = workload_.NextSequenceKey();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values);
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
