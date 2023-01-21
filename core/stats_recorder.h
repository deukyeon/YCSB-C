//
//  stat_recoder.h
//  YCSB-C
//
//  Created by Deukyeon Hwang on 01/20/23.
//

#ifndef YCSB_C_STATS_RECORDER_H
#define YCSB_C_STATS_RECORDER_H

#include "timer.h"
#include <atomic>
#include <cstddef>
#include <iostream>
#include <numeric>
#include <vector>

namespace ycsbc {

template <typename T> class StatsRecorder {
public:
  StatsRecorder() = delete;
  void operator=(const StatsRecorder &) = delete;

  static void init(std::size_t num_txns) {
    if (instance == nullptr) {
      instance = new StatsRecorder<T>(num_txns);
    }
  }

  static void deinit() {
    assert(instance);
    delete instance;
    instance = nullptr;
  }

  static inline StatsRecorder<T> *getInstance() {
    assert(instance);
    return instance;
  }

  std::size_t getNextStatId() { return next_stat_id.fetch_add(1); }

  void recordLatency(std::size_t stat_id, T latency) {
    stats[stat_id].latency = latency;
  }

  void recordAbortTime(std::size_t stat_id, T abort_time) {
    stats[stat_id].time_by_abort += abort_time;
    ++stats[stat_id].abort_count;
  }

  inline std::size_t getTotalNumberOfTransactions() const {
    // return stats.size();
    return next_stat_id;
  }

  inline T getLatencyPerTxn(std::size_t stat_id) const {
    return stats[stat_id].latency;
  }

  inline T getAbortTimePerTxn(std::size_t stat_id) const {
    return stats[stat_id].time_by_abort;
  }

  inline std::size_t getAbortCountPerTxn(std::size_t stat_id) const {
    return stats[stat_id].abort_count;
  }

  inline T getTotalElapsedTime() const {
    T sum = 0;
    for (std::size_t i = 0; i < stats.size(); ++i) {
      sum += getLatencyPerTxn(i);
    }
    return sum;
  }

  inline T getAverageLatency() const {
    return getTotalElapsedTime() / getTotalNumberOfTransactions();
  }

  inline T getTotalAbortTime() const {
    T sum = 0;
    for (std::size_t i = 0; i < stats.size(); ++i) {
      sum += getAbortTimePerTxn(i);
    }
    return sum;
  }

  inline std::size_t getTotalAbortCount() const {
    std::size_t sum = 0;
    for (std::size_t i = 0; i < stats.size(); ++i) {
      sum += getAbortCountPerTxn(i);
    }
    return sum;
  }

  inline T getAverageAbortTime() const {
    return getTotalAbortTime() / getTotalNumberOfTransactions();
  }

  inline double getAverageAbortCount() const {
    return (double)getTotalAbortCount() / getTotalNumberOfTransactions();
  }

  inline double getAbortRate() const {
    return (double)getTotalAbortCount() /
           (getTotalAbortCount() + getTotalNumberOfTransactions());
  }

  void printSummary() {
    std::cout << "Total number of transactions:\t"
              << getTotalNumberOfTransactions() << "\n";
    std::cout << "Time per transaction (sec):\t" << getAverageLatency() << "\n";
    std::cout << "Total abort count:\t\t" << getTotalAbortCount() << "\n";
    std::cout << "Abort time per transaction (sec):\t" << getAverageAbortTime() << "\n";
    std::cout << "Abort count per transaction:\t\t" << getAverageAbortCount() << "\n";
    std::cout << "Abort rate:\t\t" << getAbortRate() << "\n";
  }

  void printAllStatsAsCSV(std::ostream &os = std::cout) {
    os << "latency,time_by_abort,abort_count\n";
    for (auto &stat : stats) {
      os << stat.latency << "," << stat.time_by_abort << "," << stat.abort_count
         << "\n";
    }
  }

protected:
  StatsRecorder(std::size_t total_num_txns) {
    stats.resize(total_num_txns);
    next_stat_id = 0;
  }

  static StatsRecorder *instance;

  struct StatPerTxn {
    T latency;
    T time_by_abort;
    std::size_t abort_count = 0;
  };
  std::vector<StatPerTxn> stats;
  std::atomic<std::size_t> next_stat_id;
};

} // namespace ycsbc

#endif // YCSB_C_STATS_RECORDER_H
