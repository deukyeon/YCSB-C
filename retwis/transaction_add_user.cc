#include "transaction.h"

namespace retwis {

TransactionAddUser::TransactionAddUser(ycsbc::DB  *db,
                                       std::string next_user_id,
                                       std::string username,
                                       std::string password)
   : Transaction(db, next_user_id), username(username), password(password)
{}


bool
TransactionAddUser::run()
{
   debug("TransactionAddUser::run()\n");

   db->Begin(&txn);

   // Check if the selected username is already in use.
   // debug("Checking if username %s is already in use.\n", username.c_str());
   value->first  = "list";
   value->second = default_empty_buffer;
   run_and_return_if_abort(
      db->Read(txn, "dummy_table", key_users(), nullptr, values));
   std::vector<std::string> active_users = string_to_vector(values[0].second);
   for (auto &user : active_users) {
      if (user == username) {
         return (db->Commit(&txn) == ycsbc::DB::kErrorConflict);
      }
   }

   // debug("Everything is ok, Register the user %s!.\n", username.c_str());

   active_users.push_back(username);
   // ! The list type value is stored as a string, but it becomes too large.
   // ! So, we maintains just a few elements.
   trim_vector(active_users, max_list_type_value_size);
   value->first  = "list";
   value->second = vector_to_string(active_users);
   run_and_return_if_abort(db->Update(txn, "dummy_table", key_users(), values));
   // debug("Updated active users list.\n");

   std::string authsecret = generate_hash_string();
   value->first           = "hash";
   value->second          = "username:" + username + ",password:" + password
                   + ",authsecret:" + authsecret;
   run_and_return_if_abort(
      db->Insert(txn, "dummy_table", key_user(me), values));

   // debug("Inserted user %s.\n", key_user(me).c_str());

   value->first  = "string";
   value->second = me;
   run_and_return_if_abort(
      db->Insert(txn, "dummy_table", key_auth(authsecret), values));

   // debug("Inserted auth %s.\n", key_auth(authsecret).c_str());

   return (db->Commit(&txn) == ycsbc::DB::kErrorConflict);
}

} // namespace retwis