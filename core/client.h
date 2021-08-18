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
  uint64_t pinode = workload_.NextSequenceKey();
  for(uint64_t i = 0 ; i < 1 ; i++){
    std::string fname = std::to_string(i);
    db_.Insert(pinode,fname,i);
  }  
  return true;
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
      db_.RecordTime(2,op_time);
      break;
    case UPDATE:
      status = TransactionUpdate();
      op_time = (get_now_micros() - start_time );
      ops_time[UPDATE] += op_time;
      ops_cnt[UPDATE]++;
      db_.RecordTime(3,op_time);
      break;
    case INSERT:
      status = TransactionInsert();
      op_time = (get_now_micros() - start_time );
      ops_time[INSERT] += op_time;
      ops_cnt[INSERT]++;
      db_.RecordTime(1,op_time);
      break;
    case SCAN:
      status = TransactionScan();
      op_time = (get_now_micros() - start_time );
      ops_time[SCAN] += op_time;
      ops_cnt[SCAN]++;
      db_.RecordTime(4,op_time);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      op_time = (get_now_micros() - start_time );
      ops_time[READMODIFYWRITE] += op_time;
      ops_cnt[READMODIFYWRITE]++;
      db_.RecordTime(5,op_time);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const uint64_t &pinode = workload_.NextTransactionKey();
  uint64_t inode;
  for(uint64_t i = 0 ; i < 1 ; i++){
    std::string fname = std::to_string(i);
    db_.Read(pinode,fname,&inode);
  }
  return 1;
}

inline int Client::TransactionReadModifyWrite() {
  const uint64_t pinode = workload_.NextTransactionKey();

  uint64_t inode ;
  for(uint64_t i = 0 ; i < 1 ; i++){
    std::string fname = std::to_string(i);
    db_.Read(pinode,fname,&inode);
    db_.Update(pinode,fname,inode+1);
  }

  return 1;
}

inline int Client::TransactionScan() {
  const uint64_t pinode = workload_.NextTransactionKey();
  std::vector<std::string> fnames;
  std::vector<uint64_t> inodes;
  return db_.Scan(pinode, fnames, inodes);
}

inline int Client::TransactionUpdate() {
  const uint64_t pinode = workload_.NextTransactionKey();
  for(uint64_t i = 0 ; i < 1 ; i++){
    std::string fname = std::to_string(i);
    db_.Update(pinode,fname,i+1);
  }
  return 1;
}

inline int Client::TransactionInsert() {
  const uint64_t pinode = workload_.NextTransactionKey();
  for(uint64_t i = 0 ; i < 1 ; i++){
    std::string fname = std::to_string(i);
    db_.Insert(pinode,fname,i);
  }
  return 1;
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
