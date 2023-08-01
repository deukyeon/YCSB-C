#pragma once

#include "client.h"

namespace ycsbc {

class TxnClient : public Client {
public:
   TxnClient(int id, DB &db, CoreWorkload &wl) : Client(id, db, wl) {}

protected:
   void
   GenerateClientTransactionalOperations()
   {
      ClientOperation client_op;
      int             num_ops = workload_.ops_per_transaction();

      if (workload_.read_proportion() > 0) {
         int read_ops = num_ops * workload_.read_proportion() + 0.5;
         read_ops += (read_ops == 0);
         for (int i = 0; i < read_ops; ++i) {
            GenerateClientOperationRead(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.insert_proportion() > 0) {
         int insert_ops = num_ops * workload_.insert_proportion() + 0.5;
         insert_ops += (insert_ops == 0);
         for (int i = 0; i < insert_ops; ++i) {
            GenerateClientOperationInsert(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.update_proportion() > 0) {
         int update_ops = num_ops * workload_.update_proportion() + 0.5;
         update_ops += (update_ops == 0);
         for (int i = 0; i < update_ops; ++i) {
            GenerateClientOperationUpdate(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.scan_proportion() > 0) {
         int scan_ops = num_ops * workload_.scan_proportion() + 0.5;
         scan_ops += (scan_ops == 0);
         for (int i = 0; i < scan_ops; ++i) {
            GenerateClientOperationScan(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.readmodifywrite_proportion() > 0) {
         int readmodifywrite_ops =
            num_ops * workload_.readmodifywrite_proportion() + 0.5;
         readmodifywrite_ops += (readmodifywrite_ops == 0);
         for (int i = 0; i < readmodifywrite_ops; ++i) {
            GenerateClientOperationReadModifyWrite(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      // for (auto &op : operations_in_transaction) {
      //    std::cout << op.op << " " << op.key << std::endl;
      // }
      // std::cout << "---" << std::endl;
   }
};

} // namespace ycsbc