#pragma once

#include "client.h"

namespace ycsbc {

// This class generates transactions with 50% reads and 50%
// inserts. It ignores the workload configuration.

class ExpClient : public Client {
public:
   ExpClient(int id, DB &db, CoreWorkload &wl) : Client(id, db, wl) {}

protected:
   void
   GenerateClientTransactionalOperations()
   {
      ClientOperation client_op;
      for (int i = 0; i < workload_.ops_per_transaction() / 2; ++i) {
         GenerateClientOperationRead(client_op);
         operations_in_transaction.emplace(client_op);
         GenerateClientOperationInsert(client_op);
         operations_in_transaction.emplace(client_op);
      }

      //   for (auto &op : operations_in_transaction) {
      //      std::cout << op.op << " " << op.key << std::endl;
      //   }
      //   std::cout << "---" << std::endl;
   }
};

} // namespace ycsbc