//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include "core_workload.h"
#include "db.h"
#include "utils.h"
#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <string>
#include <thread>

namespace ycsbc {

struct ClientOperation {
  enum Operation op;
  std::string table;
  std::string key;
  size_t len;
  std::vector<std::string> read_fields;
  std::vector<DB::KVPair> values;

  inline bool operator==(const ClientOperation &other) const {
    return table == other.table && key == other.key && op == other.op;
  }
};

class Client {
public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {
    workload_.InitKeyBuffer(key);
    workload_.InitPairs(pairs);

    abort_cnt = 0;
  }

  virtual bool DoInsert();
  virtual bool DoTransaction();

  virtual ~Client() {
    // std::cout << abort_cnt << "\n";
    Client::total_abort_cnt += abort_cnt;
  }

  static std::atomic<unsigned long> total_abort_cnt;

protected:
  virtual bool DoOperation();
  virtual bool DoTransactionalOperations();

  virtual int TransactionRead(Transaction *txn, ClientOperation &client_op);
  virtual int TransactionReadModifyWrite(Transaction *txn,
                                         ClientOperation &client_op);
  virtual int TransactionScan(Transaction *txn, ClientOperation &client_op);
  virtual int TransactionUpdate(Transaction *txn, ClientOperation &client_op);
  virtual int TransactionInsert(Transaction *txn, ClientOperation &client_op);

  virtual void GenerateClientOperationRead(ClientOperation &client_op);
  virtual void
  GenerateClientOperationReadModifyWrite(ClientOperation &client_op);
  virtual void GenerateClientOperationScan(ClientOperation &client_op);
  virtual void GenerateClientOperationUpdate(ClientOperation &client_op);
  virtual void GenerateClientOperationInsert(ClientOperation &client_op);

  DB &db_;
  CoreWorkload &workload_;
  std::string key;
  std::vector<DB::KVPair> pairs;

  std::vector<ClientOperation> operations_in_transaction;
  unsigned long abort_cnt;
};

inline bool Client::DoInsert() {
  workload_.NextSequenceKey(key);
  workload_.UpdateValues(pairs);
  int status = -1;
  if (db_.IsTransactionSupported()) {
    Transaction *txn = NULL;
    db_.Begin(&txn);
    status = db_.Insert(txn, workload_.NextTable(), key, pairs);
    db_.Commit(&txn);
  } else {
    status = db_.Insert(NULL, workload_.NextTable(), key, pairs);
  }

  return (status == DB::kOK);
}

inline bool Client::DoTransaction() {
  if (db_.IsTransactionSupported()) {
    return DoTransactionalOperations();
  }

  return DoOperation();
}

inline bool Client::DoOperation() {
  int status = -1;

  ClientOperation client_op;

  switch (workload_.NextOperation()) {
  case READ:
    GenerateClientOperationRead(client_op);
    status = TransactionRead(NULL, client_op);
    break;
  case UPDATE:
    GenerateClientOperationUpdate(client_op);
    status = TransactionUpdate(NULL, client_op);
    break;
  case INSERT:
    GenerateClientOperationInsert(client_op);
    status = TransactionInsert(NULL, client_op);
    break;
  case SCAN:
    GenerateClientOperationScan(client_op);
    status = TransactionScan(NULL, client_op);
    break;
  case READMODIFYWRITE:
    GenerateClientOperationReadModifyWrite(client_op);
    status = TransactionReadModifyWrite(NULL, client_op);
    break;
  default:
    throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);

  return true;
}

inline bool Client::DoTransactionalOperations() {
  for (int i = 0; i < workload_.ops_per_transaction(); ++i) {
    Operation op = workload_.NextOperation();

  RETRY : {
    ClientOperation client_op;

    switch (op) {
    case READ:
      GenerateClientOperationRead(client_op);
      break;
    case UPDATE:
      GenerateClientOperationUpdate(client_op);
      break;
    case INSERT:
      GenerateClientOperationInsert(client_op);
      break;
    case SCAN:
      GenerateClientOperationScan(client_op);
      break;
    case READMODIFYWRITE:
      GenerateClientOperationReadModifyWrite(client_op);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
    }
    const bool same_op_exist =
        std::find(operations_in_transaction.begin(),
                  operations_in_transaction.end(),
                  client_op) != operations_in_transaction.end();
    if (same_op_exist) {
      goto RETRY;
    }
    operations_in_transaction.emplace_back(client_op);
  }
  }

  // for (auto &op : operations_in_transaction) {
  //   std::cout << op.key << std::endl;
  // }
  // std::cout << "---" << std::endl;

  bool need_retry = false;
  int retry = 0;
  do {
    int status = -1;
    Transaction *txn = NULL;
    db_.Begin(&txn);

    for (ClientOperation &client_op : operations_in_transaction) {
      switch (client_op.op) {
      case READ:
        status = TransactionRead(txn, client_op);
        break;
      case UPDATE:
        status = TransactionUpdate(txn, client_op);
        break;
      case INSERT:
        status = TransactionInsert(txn, client_op);
        break;
      case SCAN:
        status = TransactionScan(txn, client_op);
        break;
      case READMODIFYWRITE:
        status = TransactionReadModifyWrite(txn, client_op);
        break;
      default:
        throw utils::Exception("Operation request is not recognized!");
      }
      assert(status >= 0);
    }

    if ((need_retry = db_.Commit(&txn) == DB::kErrorConflict)) {
      ++abort_cnt;
      const int sleep_for =
          std::min((int)std::pow(2.0, (float)retry++),
          workload_.max_txn_retry_ms());
      std::this_thread::sleep_for(std::chrono::milliseconds(sleep_for));
    }
  } while (need_retry);

  operations_in_transaction.clear();

  return true;
}

inline int Client::TransactionRead(Transaction *txn,
                                   ClientOperation &client_op) {
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(txn, client_op.table, client_op.key, &fields, result);
  } else {
    return db_.Read(txn, client_op.table, client_op.key, NULL, result);
  }
}

inline int Client::TransactionReadModifyWrite(Transaction *txn,
                                              ClientOperation &client_op) {
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    db_.Read(txn, client_op.table, client_op.key, &client_op.read_fields,
             result);
  } else {
    db_.Read(txn, client_op.table, client_op.key, NULL, result);
  }

  return db_.Update(txn, client_op.table, client_op.key, client_op.values);
}

inline int Client::TransactionScan(Transaction *txn,
                                   ClientOperation &client_op) {
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    return db_.Scan(txn, client_op.table, client_op.key, client_op.len,
                    &client_op.read_fields, result);
  } else {
    return db_.Scan(txn, client_op.table, client_op.key, client_op.len, NULL,
                    result);
  }
}

inline int Client::TransactionUpdate(Transaction *txn,
                                     ClientOperation &client_op) {
  return db_.Update(txn, client_op.table, client_op.key, client_op.values);
}

inline int Client::TransactionInsert(Transaction *txn,
                                     ClientOperation &client_op) {
  return db_.Insert(txn, client_op.table, client_op.key, client_op.values);
}

inline void Client::GenerateClientOperationRead(ClientOperation &client_op) {
  client_op.op = READ;
  client_op.table = workload_.NextTable();
  client_op.key = workload_.NextTransactionKey();
}

inline void
Client::GenerateClientOperationReadModifyWrite(ClientOperation &client_op) {
  client_op.op = READMODIFYWRITE;
  client_op.table = workload_.NextTable();
  client_op.key = workload_.NextTransactionKey();

  if (!workload_.read_all_fields()) {
    client_op.read_fields.push_back("field" + workload_.NextFieldName());
  }

  if (workload_.write_all_fields()) {
    workload_.BuildValues(client_op.values);
  } else {
    workload_.BuildUpdate(client_op.values);
  }
}

inline void Client::GenerateClientOperationScan(ClientOperation &client_op) {
  client_op.op = SCAN;
  client_op.table = workload_.NextTable();
  client_op.key = workload_.NextTransactionKey();
  client_op.len = workload_.NextScanLength();

  if (!workload_.read_all_fields()) {
    client_op.read_fields.push_back("field" + workload_.NextFieldName());
  }
}

inline void Client::GenerateClientOperationUpdate(ClientOperation &client_op) {
  client_op.op = UPDATE;
  client_op.table = workload_.NextTable();
  client_op.key = workload_.NextTransactionKey();

  if (workload_.write_all_fields()) {
    workload_.BuildValues(client_op.values);
  } else {
    workload_.BuildUpdate(client_op.values);
  }
}

inline void Client::GenerateClientOperationInsert(ClientOperation &client_op) {
  workload_.NextSequenceKey(key);
  client_op.op = INSERT;
  client_op.table = workload_.NextTable();
  client_op.key = key;
  workload_.BuildValues(client_op.values);
}

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
