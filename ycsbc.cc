//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "core/client.h"
#include "core/core_workload.h"
#include "core/timer.h"
#include "core/utils.h"
#include "db/db_factory.h"
#include "core/tpcc_workload.h"
#include <cstring>
#include <future>
#include <iostream>
#include <string>
#include <vector>

#define TPCC 1

using namespace std;

typedef struct WorkloadProperties {
   string            filename;
   bool              preloaded;
   utils::Properties props;
} WorkloadProperties;

std::map<string, string> default_props = {
   {"threadcount", "1"},
   {"dbname", "basic"},
   {"progress", "none"},

   //
   // Basicdb config defaults
   //
   {"basicdb.verbose", "0"},

   //
   // splinterdb config defaults
   //
   {"splinterdb.filename", "splinterdb.db"},
   {"splinterdb.cache_size_mb", "4096"},
   // {"splinterdb.cache_size_mb", "163840"},
   {"splinterdb.disk_size_gb", "1024"},

   {"splinterdb.max_key_size", "24"},
   {"splinterdb.use_log", "1"},

   // All these options use splinterdb's internal defaults
   {"splinterdb.page_size", "0"},
   {"splinterdb.extent_size", "0"},
   {"splinterdb.io_flags", "16450"}, // O_CREAT | O_RDWR | O_DIRECT
   {"splinterdb.io_perms", "0"},
   {"splinterdb.io_async_queue_depth", "0"},
   {"splinterdb.cache_use_stats", "0"},
   {"splinterdb.cache_logfile", "0"},
   {"splinterdb.btree_rough_count_height", "0"},
   {"splinterdb.filter_remainder_size", "0"},
   {"splinterdb.filter_index_size", "0"},
   {"splinterdb.memtable_capacity", "0"},
   {"splinterdb.fanout", "0"},
   {"splinterdb.max_branches_per_node", "0"},
   {"splinterdb.use_stats", "0"},
   {"splinterdb.reclaim_threshold", "0"},
   {"splinterdb.isolation_level", "1"},

   {"rocksdb.database_filename", "rocksdb.db"},
   //    {"rocksdb.isolation_level", "3"},
};

void
UsageMessage(const char *command);
bool
StrStartWith(const char *str, const char *pre);
void
ParseCommandLine(int                         argc,
                 const char                 *argv[],
                 utils::Properties          &props,
                 WorkloadProperties         &load_workload,
                 vector<WorkloadProperties> &run_workloads);

typedef enum progress_mode {
   no_progress,
   hash_progress,
   percent_progress,
} progress_mode;

static inline void
ReportProgress(progress_mode      pmode,
               uint64_t           total_ops,
               volatile uint64_t *global_op_counter,
               uint64_t           stepsize,
               volatile uint64_t *last_printed)
{
   uint64_t old_counter = __sync_fetch_and_add(global_op_counter, stepsize);
   uint64_t new_counter = old_counter + stepsize;
   if (100 * old_counter / total_ops != 100 * new_counter / total_ops) {
      if (pmode == hash_progress) {
         cout << "#" << flush;
      } else if (pmode == percent_progress) {
         uint64_t my_percent = 100 * new_counter / total_ops;
         while (*last_printed + 1 != my_percent) {
         }
         cout << 100 * new_counter / total_ops << "%\r" << flush;
         *last_printed = my_percent;
      }
   }
}

static inline void
ProgressUpdate(progress_mode      pmode,
               uint64_t           total_ops,
               volatile uint64_t *global_op_counter,
               uint64_t           i,
               volatile uint64_t *last_printed)
{
   uint64_t sync_interval = 0 < total_ops / 1000 ? total_ops / 1000 : 1;
   if ((i % sync_interval) == 0) {
      ReportProgress(
         pmode, total_ops, global_op_counter, sync_interval, last_printed);
   }
}

static inline void
ProgressFinish(progress_mode      pmode,
               uint64_t           total_ops,
               volatile uint64_t *global_op_counter,
               uint64_t           i,
               volatile uint64_t *last_printed)
{
   uint64_t sync_interval = 0 < total_ops / 1000 ? total_ops / 1000 : 1;
   ReportProgress(
      pmode, total_ops, global_op_counter, i % sync_interval, last_printed);
}

int
DelegateClient(int                  id,
               ycsbc::DB           *db,
               ycsbc::CoreWorkload *wl,
               const uint64_t       num_ops,
               bool                 is_loading,
               progress_mode        pmode,
               uint64_t             total_ops,
               volatile uint64_t   *global_op_counter,
               volatile uint64_t   *last_printed,
               uint64_t            *txn_cnt,
               uint64_t            *abort_cnt)
{
   // ! Hoping this atomic shared variable is not bottlenecked.
   // static std::atomic<bool> run_bench;
   // run_bench.store(true);
   db->Init();
   ycsbc::Client client(id, *db, *wl);
   uint64_t      oks = 0;

   if (is_loading) {
      for (uint64_t i = 0; i < num_ops; ++i) {
         oks += client.DoInsert();
         ProgressUpdate(pmode, total_ops, global_op_counter, i, last_printed);
      }
   } else {
      if (wl->max_txn_count() > 0) {
         // while (oks < wl->max_txn_count() && run_bench.load()) {
         while (oks < wl->max_txn_count()) {
            oks += client.DoTransaction();
            ProgressUpdate(
               pmode, total_ops, global_op_counter, oks, last_printed);
         }
         ProgressFinish(pmode, total_ops, global_op_counter, oks, last_printed);
         // run_bench.store(false);
      } else {
         for (uint64_t i = 0; i < num_ops; ++i) {
            oks += client.DoTransaction();
            ProgressUpdate(
               pmode, total_ops, global_op_counter, i, last_printed);
         }
         ProgressFinish(
            pmode, total_ops, global_op_counter, num_ops, last_printed);
      }
   }
   db->Close();

   if (txn_cnt) {
      *txn_cnt = client.GetTxnCnt();
   }

   if (abort_cnt) {
      *abort_cnt = client.GetAbortCnt();
   }

   return oks;
}

struct tpcc_stats {
   uint64_t txn_cnt;
   uint64_t abort_cnt;
   uint64_t abort_cnt_payment;
   uint64_t abort_cnt_new_order;
   uint64_t attempts_payment;
   uint64_t attempts_new_order;
};

int
DelegateTPCCClient(uint32_t            thread_id,
                   ycsbc::DB          *db,
                   tpcc::TPCCWorkload *wl,
                   tpcc_stats         *stats)
{
   db->Init();

   tpcc::TPCCClient client(thread_id, wl);
   client.run_transactions();

   if (stats) {
      stats->txn_cnt             = client.GetTxnCnt();
      stats->abort_cnt           = client.GetAbortCnt();
      stats->abort_cnt_payment   = client.GetAbortCntPayment();
      stats->abort_cnt_new_order = client.GetAbortCntNewOrder();
      stats->attempts_payment    = client.GetAttemptsPayment();
      stats->attempts_new_order  = client.GetAttemptsNewOrder();
   }

   db->Close();


   return 0;
}


int
main(const int argc, const char *argv[])
{
   utils::Properties          props;
   WorkloadProperties         load_workload;
   vector<WorkloadProperties> run_workloads;
   ParseCommandLine(argc, argv, props, load_workload, run_workloads);

   const unsigned int num_threads = stoi(props.GetProperty("threadcount", "1"));
   progress_mode      pmode       = no_progress;
   if (props.GetProperty("progress", "none") == "hash") {
      pmode = hash_progress;
   } else if (props.GetProperty("progress", "none") == "percent") {
      pmode = percent_progress;
   }
   vector<future<int>>  actual_ops;
   uint64_t             record_count;
   uint64_t             total_ops;
   uint64_t             total_txn_count;
   utils::Timer<double> timer;

   ycsbc::DB *db;
   if (props.GetProperty("benchmark") == "tpcc") {
      db = new ycsbc::TransactionalSplinterDB(props,
                                              load_workload.preloaded,
                                              tpcc::tpcc_merge_tuple,
                                              tpcc::tpcc_merge_tuple_final);

      tpcc::TPCCWorkload tpcc_wl = tpcc::TPCCWorkload();
      tpcc_wl.init((ycsbc::TransactionalSplinterDB *)db,
                   num_threads); // loads TPCC tables into DB

      std::vector<tpcc_stats> _tpcc_stats(num_threads);

      timer.Start();
      {
         for (unsigned int i = 0; i < num_threads; ++i) {
            actual_ops.emplace_back(async(launch::async,
                                          DelegateTPCCClient,
                                          i,
                                          db,
                                          &tpcc_wl,
                                          &_tpcc_stats[i]));
         }
         assert(actual_ops.size() == num_threads);
         for (auto &n : actual_ops) {
            assert(n.valid());
            n.get();
         }
         if (pmode != no_progress) {
            cout << "\n";
         }
      }

      double run_duration = timer.End();

      tpcc_wl.deinit();

      uint64_t total_committed_cnt         = 0;
      uint64_t total_aborted_cnt           = 0;
      uint64_t total_aborted_cnt_payment   = 0;
      uint64_t total_aborted_cnt_new_order = 0;
      uint64_t total_attempts_payment      = 0;
      uint64_t total_attempts_new_order    = 0;

      for (unsigned int i = 0; i < num_threads; ++i) {
         cout << "[Client " << i << "] txn_cnt: " << _tpcc_stats[i].txn_cnt
              << ", abort_cnt: " << _tpcc_stats[i].abort_cnt << endl;
         total_committed_cnt += _tpcc_stats[i].txn_cnt;
         total_aborted_cnt += _tpcc_stats[i].abort_cnt;
         total_aborted_cnt_payment += _tpcc_stats[i].abort_cnt_payment;
         total_aborted_cnt_new_order += _tpcc_stats[i].abort_cnt_new_order;
         total_attempts_payment += _tpcc_stats[i].attempts_payment;
         total_attempts_new_order += _tpcc_stats[i].attempts_new_order;
      }

      cout << "# Transaction goodput (KTPS)" << endl;
      cout << props["dbname"] << " TransactionalSplinterDB" << '\t'
           << num_threads << '\t';
      cout << total_committed_cnt / run_duration / 1000 << endl;
      cout << "Run duration (sec):\t" << run_duration << endl;

      cout << "# Abort count:\t" << total_aborted_cnt << '\n';
      cout << "Abort rate:\t"
           << (double)total_aborted_cnt
                 / (total_aborted_cnt + total_committed_cnt)
           << "\n";

      cout << "# (Payment) Abort rate(%):\t"
           << total_aborted_cnt_payment * 100.0 / total_aborted_cnt << '\n';
      cout << "# (NewOrder) Abort rate(%):\t"
           << total_aborted_cnt_new_order * 100.0 / total_aborted_cnt << '\n';

      cout << "# (Payment) Failure rate(%):\t"
           << total_aborted_cnt_payment * 100.0 / total_attempts_payment
           << '\n';
      cout << "# (NewOrder) Failure rate(%):\t"
           << total_aborted_cnt_new_order * 100.0 / total_attempts_new_order
           << '\n';

      cout << "# (Payment) Total attempts:\t" << total_attempts_payment << '\n';
      cout << "# (NewOrder) Total attempts:\t" << total_attempts_new_order
           << '\n';

      db->PrintDBStats();
   } else {
      db = ycsbc::DBFactory::CreateDB(props, load_workload.preloaded);
      if (!db) {
         cout << "Unknown database name " << props["dbname"] << endl;
         exit(0);
      }

      record_count =
         stoi(load_workload.props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
      uint64_t batch_size = sqrt(record_count);
      if (record_count / batch_size < num_threads)
         batch_size = record_count / num_threads;
      if (batch_size < 1)
         batch_size = 1;

      ycsbc::BatchedCounterGenerator key_generator(
         load_workload.preloaded ? record_count : 0, batch_size);
      ycsbc::CoreWorkload wls[num_threads];

      unsigned int thr_i;
      for (thr_i = 0; thr_i < num_threads; ++thr_i) {
         wls[thr_i].InitLoadWorkload(
            load_workload.props, num_threads, thr_i, &key_generator);
      }

      // Perform the Load phase
      if (!load_workload.preloaded) {
         timer.Start();
         {
            cout << "# Loading records:\t" << record_count << endl;
            uint64_t load_progress = 0;
            uint64_t last_printed  = 0;
            for (thr_i = 0; thr_i < num_threads; ++thr_i) {
               uint64_t start_op = (record_count * thr_i) / num_threads;
               uint64_t end_op   = (record_count * (thr_i + 1)) / num_threads;
               actual_ops.emplace_back(async(launch::async,
                                             DelegateClient,
                                             thr_i,
                                             db,
                                             &wls[thr_i],
                                             end_op - start_op,
                                             true,
                                             pmode,
                                             record_count,
                                             &load_progress,
                                             &last_printed,
                                             nullptr,
                                             nullptr));
            }
            assert(actual_ops.size() == num_threads);
            total_txn_count = 0;
            for (auto &n : actual_ops) {
               assert(n.valid());
               total_txn_count += n.get();
            }
            if (pmode != no_progress) {
               cout << "\n";
            }
         }
         double load_duration = timer.End();
         cout << "# Load throughput (KTPS)" << endl;
         cout << props["dbname"] << '\t' << load_workload.filename << '\t'
              << num_threads << '\t';
         cout << total_txn_count / load_duration / 1000 << endl;
         cout << "Load duration (sec):\t" << load_duration << endl;

         db->PrintDBStats();
      }

      uint64_t ops_per_transactions = 1;
      // Perform any Run phases
      for (const auto &workload : run_workloads) {
         unsigned int num_run_threads = stoi(
            workload.props.GetProperty("threads", std::to_string(num_threads)));
         std::vector<std::thread> init_threads;
         for (thr_i = 0; thr_i < num_run_threads; ++thr_i) {
            init_threads.emplace_back(std::thread(
               [&wls = wls, &workload = workload, num_run_threads, thr_i]() {
                  wls[thr_i].InitRunWorkload(
                     workload.props, num_run_threads, thr_i);
               }));
         }
         for (auto &t : init_threads) {
            t.join();
         }
         actual_ops.clear();

         ops_per_transactions   = stoi(workload.props.GetProperty(
            ycsbc::CoreWorkload::OPS_PER_TRANSACTION_PROPERTY,
            ycsbc::CoreWorkload::OPS_PER_TRANSACTION_DEFAULT));
         uint64_t max_txn_count = stoi(workload.props.GetProperty(
            ycsbc::CoreWorkload::MAX_TXN_COUNT_PROPERTY, "0"));
         total_ops =
            max_txn_count > 0
               ? max_txn_count * num_run_threads * ops_per_transactions
               : stoi(workload
                         .props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
         uint64_t              run_progress = 0;
         uint64_t              last_printed = 0;
         std::vector<uint64_t> txn_cnts(num_run_threads, 0);
         std::vector<uint64_t> abort_cnts(num_run_threads, 0);

         timer.Start();
         {
            for (thr_i = 0; thr_i < num_run_threads; ++thr_i) {
               uint64_t start_op = (total_ops * thr_i) / num_run_threads;
               uint64_t end_op   = (total_ops * (thr_i + 1)) / num_run_threads;
               uint64_t num_transactions =
                  (end_op - start_op) / ops_per_transactions;
               actual_ops.emplace_back(async(launch::async,
                                             DelegateClient,
                                             thr_i,
                                             db,
                                             &wls[thr_i],
                                             num_transactions,
                                             false,
                                             pmode,
                                             total_ops,
                                             &run_progress,
                                             &last_printed,
                                             &txn_cnts[thr_i],
                                             &abort_cnts[thr_i]));
            }
            assert(actual_ops.size() == num_run_threads);
            total_txn_count = 0;
            for (auto &n : actual_ops) {
               assert(n.valid());
               total_txn_count += n.get();
            }
            if (pmode != no_progress) {
               cout << "\n";
            }
         }
         double run_duration = timer.End();

         for (thr_i = 0; thr_i < num_run_threads; ++thr_i) {
            wls[thr_i].DeinitRunWorkload();
         }

         const uint64_t total_commit_cnt =
            std::accumulate(txn_cnts.begin(), txn_cnts.end(), 0);
         cout << "# Transaction count:\t" << total_txn_count << endl;
         for (thr_i = 0; thr_i < num_run_threads; ++thr_i) {
            cout << "[Client " << thr_i << "] txn_cnt: " << txn_cnts[thr_i]
                 << ", abort_cnt: " << abort_cnts[thr_i] << endl;
         }

         cout << "# Transaction throughput (KTPS)" << endl;
         cout << props["dbname"] << '\t' << workload.filename << '\t'
              << num_run_threads << '\t';
         cout << total_commit_cnt / run_duration / 1000 << endl;
         cout << "Run duration (sec):\t" << run_duration << endl;

         const uint64_t total_abort_cnt =
            std::accumulate(abort_cnts.begin(), abort_cnts.end(), 0);
         cout << "# Abort count:\t" << total_abort_cnt << '\n';
         cout << "Abort rate:\t"
              << (double)total_abort_cnt / (total_abort_cnt + total_commit_cnt)
              << "\n";

         db->PrintDBStats();
      }
   }

   delete db;
}

void
ParseCommandLine(int                         argc,
                 const char                 *argv[],
                 utils::Properties          &props,
                 WorkloadProperties         &load_workload,
                 vector<WorkloadProperties> &run_workloads)
{
   bool                saw_load_workload = false;
   WorkloadProperties *last_workload     = NULL;
   int                 argindex          = 1;

   load_workload.filename  = "";
   load_workload.preloaded = false;

   props.SetProperty("benchmark", "ycsb");

   for (auto const &[key, val] : default_props) {
      props.SetProperty(key, val);
   }

   while (argindex < argc && StrStartWith(argv[argindex], "-")) {
      if (strcmp(argv[argindex], "-threads") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("threadcount", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-db") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("dbname", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-progress") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("progress", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-host") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("host", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-port") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("port", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-slaves") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("slaves", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-benchmark") == 0) {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         props.SetProperty("benchmark", argv[argindex]);
         argindex++;
      } else if (strcmp(argv[argindex], "-upserts") == 0) {
         tpcc::g_use_upserts = true;
         argindex++;
      } else if (strcmp(argv[argindex], "-W") == 0
                 || strcmp(argv[argindex], "-P") == 0
                 || strcmp(argv[argindex], "-L") == 0)
      {
         WorkloadProperties workload;
         workload.preloaded = strcmp(argv[argindex], "-P") == 0;
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         workload.filename.assign(argv[argindex]);
         ifstream input(argv[argindex]);
         try {
            workload.props.Load(input);
         } catch (const string &message) {
            cout << message << endl;
            exit(0);
         }
         input.close();
         argindex++;
         if (strcmp(argv[argindex - 2], "-W") == 0) {
            run_workloads.push_back(workload);
            last_workload = &run_workloads[run_workloads.size() - 1];
         } else if (saw_load_workload) {
            UsageMessage(argv[0]);
            exit(0);
         } else {
            saw_load_workload = true;
            load_workload     = workload;
            last_workload     = &load_workload;
         }
      } else if (strcmp(argv[argindex], "-p") == 0
                 || strcmp(argv[argindex], "-w") == 0)
      {
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         std::string propkey = argv[argindex];
         argindex++;
         if (argindex >= argc) {
            UsageMessage(argv[0]);
            exit(0);
         }
         std::string propval = argv[argindex];
         if (strcmp(argv[argindex - 2], "-w") == 0) {
            if (last_workload) {
               last_workload->props.SetProperty(propkey, propval);
            } else {
               UsageMessage(argv[0]);
               exit(0);
            }
         } else {
            props.SetProperty(propkey, propval);
         }
         argindex++;
      } else {
         cout << "Unknown option '" << argv[argindex] << "'" << endl;
         exit(0);
      }
   }

   if (argindex == 1 || argindex != argc
       || (props.GetProperty("benchmark") == "ycsb" && !saw_load_workload))
   {
      UsageMessage(argv[0]);
      exit(0);
   }
}

void
UsageMessage(const char *command)
{
   cout << "Usage: " << command << " [options]"
        << "-L <load-workload.spec> [-W run-workload.spec] ..." << endl;
   cout << "       Perform the given Load workload, then each Run workload"
        << endl;
   cout << "Usage: " << command << " [options]"
        << "-P <load-workload.spec> [-W run-workload.spec] ... " << endl;
   cout << "       Perform each given Run workload on a database that has been "
           "preloaded with the given Load workload"
        << endl;
   cout << "Options:" << endl;
   cout << "  -threads <n>: execute using <n> threads (default: "
        << default_props["threadcount"] << ")" << endl;
   cout << "  -db <dbname>: specify the name of the DB to use (default: "
        << default_props["dbname"] << ")" << endl;
   cout
      << "  -L <file>: Initialize the database with the specified Load workload"
      << endl;
   cout << "  -P <file>: Indicates that the database has been preloaded with "
           "the specified Load workload"
        << endl;
   cout << "  -W <file>: Perform the Run workload specified in <file>" << endl;
   cout << "  -p <prop> <val>: set property <prop> to value <val>" << endl;
   cout << "  -w <prop> <val>: set a property in the previously specified "
           "workload"
        << endl;
   cout << "Exactly one Load workload is allowed, but multiple Run workloads "
           "may be given.."
        << endl;
   cout << "Run workloads will be executed in the order given on the command "
           "line."
        << endl;
}

inline bool
StrStartWith(const char *str, const char *pre)
{
   return strncmp(str, pre, strlen(pre)) == 0;
}
