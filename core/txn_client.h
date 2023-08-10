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
      size_t          num_ops          = workload_.ops_per_transaction();
      size_t          num_ops_to_store = 0;

      if (workload_.read_proportion() > 0) {
         int read_ops = num_ops * workload_.read_proportion() + 0.5;
         read_ops += (read_ops == 0);
         num_ops_to_store += read_ops;
         while (operations_in_transaction.size() < num_ops_to_store) {
            GenerateClientOperationRead(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.insert_proportion() > 0) {
         int insert_ops = num_ops * workload_.insert_proportion() + 0.5;
         insert_ops += (insert_ops == 0);
         num_ops_to_store += insert_ops;
         while (operations_in_transaction.size() < num_ops_to_store) {
            GenerateClientOperationInsert(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.update_proportion() > 0) {
         int update_ops = num_ops * workload_.update_proportion() + 0.5;
         update_ops += (update_ops == 0);
         num_ops_to_store += update_ops;
         while (operations_in_transaction.size() < num_ops_to_store) {
            GenerateClientOperationUpdate(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.scan_proportion() > 0) {
         int scan_ops = num_ops * workload_.scan_proportion() + 0.5;
         scan_ops += (scan_ops == 0);
         num_ops_to_store += scan_ops;
         while (operations_in_transaction.size() < num_ops_to_store) {
            GenerateClientOperationScan(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      if (workload_.readmodifywrite_proportion() > 0) {
         int readmodifywrite_ops =
            num_ops * workload_.readmodifywrite_proportion() + 0.5;
         readmodifywrite_ops += (readmodifywrite_ops == 0);
         num_ops_to_store += readmodifywrite_ops;
         while (operations_in_transaction.size() < num_ops_to_store) {
            GenerateClientOperationReadModifyWrite(client_op);
            operations_in_transaction.emplace(client_op);
         }
      }

      assert(num_ops == operations_in_transaction.size());

      // for (auto &op : operations_in_transaction) {
      //    std::cout << op.op << " " << op.key << std::endl;
      // }
      // std::cout << "---" << std::endl;
   }
};

} // namespace ycsbc