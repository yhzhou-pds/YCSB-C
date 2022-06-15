//
// Created by wujy on 1/23/19.
//
#include <iostream>


#include "rocksdb_db.h"
#include "lib/coding.h"
#include "rocksdb/status.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include <hdr/hdr_histogram.h>
#include "rocksdb/persistent_cache.h"

//#define MUTANT
//#define PCACHE

using namespace std;

namespace ycsbc {
    RocksDB::RocksDB(const char *dbfilename, utils::Properties &props) :noResult(0){
    
        //set option
        rocksdb::Options options;
        SetOptions(&options, props);
	
        rocksdb::Status s = rocksdb::DB::Open(options,dbfilename,&db_);
        if(!s.ok()){
            cout<<"Can't open rocksdb "<<dbfilename<<" "<<s.ToString()<<endl;
            exit(0);
        }
    }

    void RocksDB::SetOptions(rocksdb::Options *options, utils::Properties &props) {

        //// 默认的Rocksdb配置
        options->create_if_missing = true;
        options->compression = rocksdb::kNoCompression;
        options->enable_pipelined_write = true;

        options->statistics = rocksdb::CreateDBStatistics();

        rocksdb::BlockBasedTableOptions block_based_options;
        options->max_bytes_for_level_base = 256ul * 1024 * 1024;
        options->write_buffer_size = 64 * 1024 * 1024;
        options->level_compaction_dynamic_level_bytes = 1;
        options->writable_file_max_buffer_size=128 * 1024 * 1024;
        options->target_file_size_base = 64 * 1024 * 1024;
        options->max_background_compactions = 4;
        options->max_background_flushes = 4;

        options->use_direct_reads=true;
        options->use_direct_io_for_flush_and_compaction=true;
	
	//// set block based cache 8k

        block_based_options.cache_index_and_filter_blocks = 0;
        std::shared_ptr<const rocksdb::FilterPolicy> filter_policy(rocksdb::NewBloomFilterPolicy(24, 0));
        block_based_options.filter_policy = filter_policy;
        block_based_options.block_cache = rocksdb::NewLRUCache(8*1024);	
        
        options->wal_dir = "/home/nvme0/zyh";	
        //
        int dboption = stoi(props["dboption"]);

        if ( dboption == 0) {  //RocksDB
                options->db_paths = {{"/home/data/vol1/zyh", 200L*1024*1024*1024}};
        
        } else if ( dboption == 1 ) { // two path and no cache
            options->db_paths = { {"/home/ubuntu/gp2/gp21", 60L*1024*1024*1024},
                                {"/home/ubuntu/gp2/gp22", 60L*1024*1024*1024},
                                {"/home/ubuntu/gp2/gp23", 60L*1024*1024*1024},
                                {"/home/ubuntu/gp2/gp24", 60L*1024*1024*1024},
                                {"/home/ubuntu/gp2/gp25", 60L*1024*1024*1024},
                                {"/home/ubuntu/gp2/gp26", 60L*1024*1024*1024}};	 
        } else {

        }

        options->table_factory.reset(
                rocksdb::NewBlockBasedTableFactory(block_based_options));
    }


    int RocksDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        rocksdb::Status s = db_->Get(rocksdb::ReadOptions(),key,&value);

	    if(s.ok()) {
            DeSerializeValues(value, result);
            return DB::kOK;
        }
        if(s.IsNotFound()){
            noResult++;
            //cerr<<"read not found:"<<noResult<<endl;
            return DB::kOK;
        }else{
            cerr<<"read error"<<endl;
            exit(0);
        }
    }


    int RocksDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        
        auto it=db_->NewIterator(rocksdb::ReadOptions());
        it->Seek(key);
        std::string val;
        std::string k;
        for(int i=0;i < len && it->Valid(); i++){
            k = it->key().ToString();
            val = it->value().ToString();
            it->Next();
        } 
        delete it;
        return DB::kOK;
    }

    int RocksDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        //fprintf(stderr,"Insert\n");
        rocksdb::Status s;
        string value;
        SerializeValues(values,value);
	    s = db_->Put(rocksdb::WriteOptions(), key, value);
        
        if(!s.ok()){
            cerr<<"insert error\n"<< s.ToString() << endl;
            exit(0);
        }
       //fprintf(stderr,"Insert End\n");
        return DB::kOK;
    }

    int RocksDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {

    	return Insert(table,key,values);
    }

    int RocksDB::Delete(const std::string &table, const std::string &key) {
        rocksdb::Status s;
        s = db_->Delete(rocksdb::WriteOptions(),key);
        if(!s.ok()){
            cerr<<"Delete error\n"<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    void RocksDB::PrintStats() {
        cout<<"read not found:"<<noResult<<endl;
        string stats;
        db_->GetProperty("rocksdb.stats",&stats);
        cout<<stats<<endl;
    }
 
	// cout << "-----------------------------------------------------" << endl;
	// cout << "SUMMARY latency (us) of this run with HDR measurement" << endl;
	// cout << "         ALL      GET      PUT      UPD      SCAN    RMW" << endl;
	// fprintf(stdout, "mean     %-8.3lf %-8.3lf %-8.3lf %-8.3lf %8.3lf %8.3lf\n",
	// 	hdr_mean(hdr_),
	// 	hdr_mean(hdr_get_),
	// 	hdr_mean(hdr_put_),
	// 	hdr_mean(hdr_update_),
	// 	hdr_mean(hdr_scan_),
	// 	hdr_mean(hdr_rmw_));
		
	// fprintf(stdout, "95th     %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
	// 	hdr_value_at_percentile(hdr_, 95),
    // 		hdr_value_at_percentile(hdr_get_, 95),
	// 	hdr_value_at_percentile(hdr_put_, 95),
	// 	hdr_value_at_percentile(hdr_update_, 95),
	// 	hdr_value_at_percentile(hdr_scan_, 95),
	// 	hdr_value_at_percentile(hdr_rmw_, 95));
    //     fprintf(stdout, "99th     %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
    //             hdr_value_at_percentile(hdr_, 99),
    //             hdr_value_at_percentile(hdr_get_, 99),
    //             hdr_value_at_percentile(hdr_put_, 99),
    //             hdr_value_at_percentile(hdr_update_, 99),
	// 	hdr_value_at_percentile(hdr_scan_, 99),
	// 	hdr_value_at_percentile(hdr_rmw_, 99));
    //     fprintf(stdout, "99.99th  %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
    //             hdr_value_at_percentile(hdr_, 99.99),
    //             hdr_value_at_percentile(hdr_get_, 99.99),
    //             hdr_value_at_percentile(hdr_put_, 99.99),
    //             hdr_value_at_percentile(hdr_update_, 99.99),
    //             hdr_value_at_percentile(hdr_scan_, 99.99),
    //             hdr_value_at_percentile(hdr_rmw_, 99.99));

	
	// int ret = hdr_percentiles_print(hdr_,f_hdr_output_,5,1.0,CLASSIC);
	// if( 0 != ret ){
	//     cout << "hdr percentile output print file error!" <<endl;
	// }
	// cout << "-------------------------------" << endl;
    // }

    bool RocksDB::HaveBalancedDistribution() {
        return true;
	//return db_->HaveBalancedDistribution();
    }

    RocksDB::~RocksDB() {
        printf("wait delete db\n");
	
        // free(hdr_);
        // free(hdr_last_1s_);
        // free(hdr_get_);
        // free(hdr_put_);
        // free(hdr_update_);
	    // free(hdr_scan_);
	    // free(hdr_rmw_);
        delete db_;
        printf("delete\n");
    }


    // void RocksDB::RecordTime(int op,uint64_t tx_xtime){
    // 	if(tx_xtime > 3600000000) {
	//     cout << "too large tx_xtime" << endl;
	// }

	// hdr_record_value(hdr_, tx_xtime);
	// hdr_record_value(hdr_last_1s_, tx_xtime);

	// if(op == 1){
	// 	hdr_record_value(hdr_put_, tx_xtime);
	// } else if(op == 2) {
	// 	hdr_record_value(hdr_get_, tx_xtime);
	// } else if(op == 3) {
	// 	hdr_record_value(hdr_update_, tx_xtime);
	// } else if(op == 4) {
	// 	hdr_record_value(hdr_scan_, tx_xtime);
	// } else if(op == 5) {
	// 	hdr_record_value(hdr_rmw_, tx_xtime);
	// } else {
	// 	cout << "record time err with op error" << endl;
	// }
    // }

    void RocksDB::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void RocksDB::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
}
