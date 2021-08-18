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
#include "rocksdb/status.h"

//#define MUTANT
#define PCACHE

using namespace std;

namespace ycsbc {

    void RocksDB::latency_hiccup(uint64_t iops) {
        //fprintf(f_hdr_hiccup_output_, "mean     95th     99th     99.99th   IOPS");
        fprintf(f_hdr_hiccup_output_, "%-11.2lf %-8ld %-8ld %-8ld %-8ld\n",
              hdr_mean(hdr_last_1s_),
              hdr_value_at_percentile(hdr_last_1s_, 95),
              hdr_value_at_percentile(hdr_last_1s_, 99),
              hdr_value_at_percentile(hdr_last_1s_, 99.99),
			  iops);
        hdr_reset(hdr_last_1s_);
        fflush(f_hdr_hiccup_output_);
    }

    RocksDB::RocksDB(const char *dbfilename, utils::Properties &props) :noResult(0){
    
        int r = hdr_init(1,INT64_C(3600000000),3,&hdr_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_last_1s_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_get_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_put_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_update_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_scan_);
        r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_rmw_);

        if((0 != r) || (NULL == hdr_) || (NULL == hdr_last_1s_) 
		    || (NULL == hdr_get_) || (NULL == hdr_put_)
		    || (NULL == hdr_scan_) || (NULL == hdr_rmw_) 
		    || (NULL == hdr_update_) || (23552 < hdr_->counts_len)) {
	      cout << "DEBUG- init hdrhistogram failed." << endl;
      	      cout << "DEBUG- r=" << r << endl;
	      cout << "DEBUG- histogram=" << &hdr_ << endl;
	      cout << "DEBUG- counts_len=" << hdr_->counts_len << endl;
	      cout << "DEBUG- counts:" << hdr_->counts << ", total_c:" << hdr_->total_count << endl;
      	      cout << "DEBUG- lowest:" << hdr_->lowest_discernible_value << ", max:" <<hdr_->highest_trackable_value << endl;
      	      free(hdr_);
      	      exit(0);
        }
        
        f_hdr_output_= std::fopen("/mnt/AEP1/ROCKSDB/rocksdb-lat.hgrm", "w+");
    	if(!f_hdr_output_) {
      	    std::perror("hdr output file opening failed");
      	    exit(0);
        }
	
        f_hdr_hiccup_output_ = std::fopen("/mnt/AEP1/ROCKSDB/rocksdb-lat.hiccup", "w+");	
        if(!f_hdr_hiccup_output_) {
      	    std::perror("hdr hiccup output file opening failed");
      	    exit(0);
    	}   
    	fprintf(f_hdr_hiccup_output_, "#mean       95th    99th    99.99th    IOPS\n");

        //set option
        rocksdb::Options options;
        SetOptions(&options, props);
        printf("begin open db\n");	
        rocksdb::Status s = rocksdb::DB::Open(options,dbfilename,&db_);
        printf("end open db\n");
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

	rocksdb::BlockBasedTableOptions block_based_options;
	options->max_bytes_for_level_base = 512ul * 1024 * 1024;
	options->write_buffer_size = 128 * 1024 * 1024;
	options->level_compaction_dynamic_level_bytes = 1;
	options->target_file_size_base = 8 * 1024 * 1024;
	options->max_background_compactions = 2;
	options->max_background_flushes = 2;

	options->use_direct_reads=true;
	options->use_direct_io_for_flush_and_compaction=true;
	
	//// set block based cache 8k

	block_based_options.cache_index_and_filter_blocks = 0;
 	std::shared_ptr<const rocksdb::FilterPolicy> filter_policy(rocksdb::NewBloomFilterPolicy(24, 0));
	block_based_options.filter_policy = filter_policy;
	block_based_options.block_cache = rocksdb::NewLRUCache(8*1024);	
	
	//
    int dboption = stoi(props["dboption"]);

    if ( dboption == 1) {  //RocksDB
       	    options->db_paths = {{"/home/ubuntu/ssd/data/data1", 200L*1024*1024*1024}};
	
	} else if ( dboption == 2 ) { // two path and no cache
	    options->db_paths = {{"/home/ubuntu/ssd/data/data1", 200L*1024*1024*1024},
	                         {"/home/ubuntu/zyh/data/data2", 200L*1024*1024*1024}};	  
	
	} else if( dboption == 3 ) { // mutant
#ifdef MUTANT 
	    printf("set mutant options\n");
	    options->db_paths = {{"/home/ubuntu/ssd/data/data1", 200L*1024*1024*1024},                               	    {"/home/ubuntu/zyh/data/data2", 200L*1024*1024*1024}};
	    options->mutant_options.monitor_temp = true;
	    options->mutant_options.migrate_sstables = true;
	    options->mutant_options.calc_sst_placement = true;
	    options->mutant_options.stg_cost_list = {0.528, 0.045};
	    options->mutant_options.stg_cost_slo = 0.3;
   	    options->mutant_options.stg_cost_slo_epsilon = 0.1;            
#endif
	} else if( dboption == 4 ) { // two path and has cache
	
            options->db_paths = {{"/home/ubuntu/ssd/data/data1", 200L*1024*1024*1024},                                    {"/home/ubuntu/zyh/data/data2", 800L*1024*1024*1024}};

#ifdef PCACHE	    
	    // set pcache
	    printf("set pcache\n");
            rocksdb::Status status;
            rocksdb::Env* env = rocksdb::Env::Default();
            status = env->CreateDirIfMissing("/home/ubuntu/ssd/data/pcache");
            assert(status.ok());
            std::shared_ptr<rocksdb::Logger> read_cache_logger;
	    uint64_t pcache_size = 3*1024*1024*1024ul;
            status = rocksdb::NewPersistentCache(env,"/home/ubuntu/ssd/data/pcache",pcache_size, read_cache_logger,
                            true, &block_based_options.persistent_cache);
            assert(status.ok());
#endif 	
	}
        
	options->table_factory.reset(
               rocksdb::NewBlockBasedTableFactory(block_based_options));
    }


    int RocksDB::Read(uint64_t pinode,std::string& fname,uint64_t* inode) {
        std::string value;
        std::string key = std::to_string(pinode) + fname;
        rocksdb::Status s = db_->Get(rocksdb::ReadOptions(),key,&value);

        if(s.ok()) {
            *inode = *(uint64_t*)value.c_str();
            return DB::kOK;
        }
        if(s.IsNotFound()){
            noResult++;
            return DB::kOK;
        }else{
            cout <<"RocksDB GET() ERROR! error string: "<< s.ToString() << endl;
            exit(0);
        }
    }


    int RocksDB::Scan(uint64_t pinode , std::vector<std::string>& fnames,std::vector<uint64_t>& inodes) {
        std::string key = std::to_string(pinode);
        auto it=db_->NewIterator(rocksdb::ReadOptions());
        it->Seek(key);
        std::string val;
        std::string k;
        for(int i=0;it->Valid(); i++){
            k = it->key().ToString();
            if(key.compare(k) > 0){
                break;
            }
            val = it->value().ToString();
            fnames.push_back(k);
            inodes.push_back(*(uint64_t*)val.c_str());
            it->Next();
        } 
        delete it;
        return DB::kOK;
    }

    int RocksDB::Insert(uint64_t pinode , std::string& fname , uint64_t inode){
        rocksdb::Status s;
        std::string key = std::to_string(pinode)+fname;
        string value = std::to_string(inode);
        s = db_->Put(rocksdb::WriteOptions(), key, value);
        
        if(!s.ok()){
            cout <<"RocksDB PUT() ERROR! error string: "<< s.ToString() << endl;
            exit(0);
        }
       
        return DB::kOK;
    }

    int RocksDB::Update(uint64_t pinode , std::string& fname , uint64_t inode) {
    	return Insert(pinode,fname,inode);
    }

    int RocksDB::Delete(uint64_t pinode , std::string& fname ) {
        rocksdb::Status s;
        std::string key = std::to_string(pinode)+fname;
        s = db_->Delete(rocksdb::WriteOptions(),key);
        if(!s.ok()){
            cout <<"RocksDB DEL() ERROR! error string: "<< s.ToString() << endl;
            exit(0);
        }
        return DB::kOK;
    }

    void RocksDB::PrintStats() {
        cout<<"read not found:"<<noResult<<endl;
        string stats;
        db_->GetProperty("rocksdb.stats",&stats);
        cout<<stats<<endl;
 
	cout << "-----------------------------------------------------" << endl;
	cout << "SUMMARY latency (us) of this run with HDR measurement" << endl;
	cout << "         ALL      GET      PUT      UPD      SCAN    RMW" << endl;
	fprintf(stdout, "mean     %-8.3lf %-8.3lf %-8.3lf %-8.3lf %8.3lf %8.3lf\n",
		hdr_mean(hdr_),
		hdr_mean(hdr_get_),
		hdr_mean(hdr_put_),
		hdr_mean(hdr_update_),
		hdr_mean(hdr_scan_),
		hdr_mean(hdr_rmw_));
		
	fprintf(stdout, "95th     %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
		hdr_value_at_percentile(hdr_, 95),
    		hdr_value_at_percentile(hdr_get_, 95),
		hdr_value_at_percentile(hdr_put_, 95),
		hdr_value_at_percentile(hdr_update_, 95),
		hdr_value_at_percentile(hdr_scan_, 95),
		hdr_value_at_percentile(hdr_rmw_, 95));
        fprintf(stdout, "99th     %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
                hdr_value_at_percentile(hdr_, 99),
                hdr_value_at_percentile(hdr_get_, 99),
                hdr_value_at_percentile(hdr_put_, 99),
                hdr_value_at_percentile(hdr_update_, 99),
		hdr_value_at_percentile(hdr_scan_, 99),
		hdr_value_at_percentile(hdr_rmw_, 99));
        fprintf(stdout, "99.99th  %-8ld %-8ld %-8ld %-8ld %-8ld %-8ld\n",
                hdr_value_at_percentile(hdr_, 99.99),
                hdr_value_at_percentile(hdr_get_, 99.99),
                hdr_value_at_percentile(hdr_put_, 99.99),
                hdr_value_at_percentile(hdr_update_, 99.99),
                hdr_value_at_percentile(hdr_scan_, 99.99),
                hdr_value_at_percentile(hdr_rmw_, 99.99));

	
	int ret = hdr_percentiles_print(hdr_,f_hdr_output_,5,1.0,CLASSIC);
	if( 0 != ret ){
	    cout << "hdr percentile output print file error!" <<endl;
	}
	cout << "-------------------------------" << endl;
    }

    bool RocksDB::HaveBalancedDistribution() {
        return true;
	//return db_->HaveBalancedDistribution();
    }

    RocksDB::~RocksDB() {
        printf("wait delete db\n");
	
        free(hdr_);
        free(hdr_last_1s_);
        free(hdr_get_);
        free(hdr_put_);
        free(hdr_update_);
	free(hdr_scan_);
	free(hdr_rmw_);

        delete db_;
        printf("delete\n");
    }


    void RocksDB::RecordTime(int op,uint64_t tx_xtime){
    	if(tx_xtime > 3600000000) {
	    cout << "too large tx_xtime" << endl;
	}

	hdr_record_value(hdr_, tx_xtime);
	hdr_record_value(hdr_last_1s_, tx_xtime);

	if(op == 1){
		hdr_record_value(hdr_put_, tx_xtime);
	} else if(op == 2) {
		hdr_record_value(hdr_get_, tx_xtime);
	} else if(op == 3) {
		hdr_record_value(hdr_update_, tx_xtime);
	} else if(op == 4) {
		hdr_record_value(hdr_scan_, tx_xtime);
	} else if(op == 5) {
		hdr_record_value(hdr_rmw_, tx_xtime);
	} else {
		cout << "record time err with op error" << endl;
	}
    }

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
