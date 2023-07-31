
#ifndef YCSB_C_NUMAUTILS_H_
#define YCSB_C_NUMAUTILS_H_

#include <thread>
#include <vector>
#include <cassert>
#include <numa.h>

namespace numautils {

inline size_t
num_lcores_per_numa_node()
{
   return static_cast<size_t>(numa_num_configured_cpus()
                              / numa_num_configured_nodes());
}

std::vector<size_t>
get_lcores_for_numa_node(size_t numa_node)
{
   assert(numa_node <= static_cast<size_t>(numa_max_node()));

   std::vector<size_t> ret;
   size_t num_lcores = static_cast<size_t>(numa_num_configured_cpus());

   for (size_t i = 0; i < num_lcores; i++) {
      if (numa_node == static_cast<size_t>(numa_node_of_cpu(i))) {
         ret.push_back(i);
      }
   }
   return ret;
}

size_t
bind_to_core(std::thread &thread, size_t numa_node, size_t numa_local_index)
{
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   assert(numa_node <= (size_t)numa_num_configured_nodes());

   const std::vector<size_t> lcore_vec = get_lcores_for_numa_node(numa_node);
   assert(numa_local_index < lcore_vec.size());

   const size_t global_index = lcore_vec.at(numa_local_index);

   CPU_SET(global_index, &cpuset);
   int rc = pthread_setaffinity_np(
      thread.native_handle(), sizeof(cpu_set_t), &cpuset);
   assert(rc == 0);
   return global_index;
}

void
clear_affinity_for_process()
{
   cpu_set_t mask;
   CPU_ZERO(&mask);
   const size_t num_cpus = std::thread::hardware_concurrency();
   for (size_t i = 0; i < num_cpus; i++)
      CPU_SET(i, &mask);

   int ret = sched_setaffinity(0 /* whole-process */, sizeof(cpu_set_t), &mask);
   assert(ret == 0);
}

} // namespace numautils
#endif