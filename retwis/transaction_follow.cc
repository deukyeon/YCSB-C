#include "transaction.h"

namespace retwis {

TransactionFollow::TransactionFollow(ycsbc::DB  *db,
                                     std::string uid,
                                     std::string target_uid)
   : Transaction(db, uid), target_uid(target_uid)
{}

bool
TransactionFollow::run()
{
   debug("TransactionFollow::run()\n");

   db->Begin(&txn);

   value->first  = "hash";
   value->second = default_empty_buffer;
   int ret =
      db->Read(txn, "dummy_table", key_user(target_uid), nullptr, values);
   if (ret == ycsbc::DB::kErrorNoData) {
      // No target user with the given uid.
      return (db->Commit(&txn) == ycsbc::DB::kErrorConflict);
   } else if (ret == ycsbc::DB::kErrorConflict) {
      return true;
   }

   value->first  = "list";
   value->second = default_empty_buffer;
   run_and_return_if_abort(
      db->Read(txn, "dummy_table", key_followers(target_uid), nullptr, values));
   std::vector<std::string> followers = string_to_vector(value->second);

   bool is_follow = true;
   for (size_t i = 0; i < followers.size(); i++) {
      if (followers[i] == me) {
         is_follow = false;
         followers.erase(followers.begin() + i);
         break;
      }
   }

   if (is_follow) {
      // value->first = "list";
      // value->second = default_empty_buffer;
      // run_and_return_if_abort(db->Read(txn, "dummy_table",
      // key_followers(target_uid), nullptr, values)); std::vector<std::string>
      // followers = string_to_vector(value->second);
      followers.push_back(me);
      // ! The list type value is stored as a string, but it becomes too large.
      // ! So, we maintains just a few elements.
      trim_vector(followers, max_list_type_value_size);
      numeric_sort_vector_of_string(followers);

      value->first  = "list";
      value->second = vector_to_string(followers);
      run_and_return_if_abort(
         db->Update(txn, "dummy_table", key_followers(target_uid), values));

      value->first  = "list";
      value->second = default_empty_buffer;
      run_and_return_if_abort(
         db->Read(txn, "dummy_table", key_following(me), nullptr, values));
      std::vector<std::string> followings = string_to_vector(value->second);
      followings.push_back(target_uid);
      // ! The list type value is stored as a string, but it becomes too large.
      // ! So, we maintains just a few elements.
      trim_vector(followings, max_list_type_value_size);
      numeric_sort_vector_of_string(followings);

      value->first  = "list";
      value->second = vector_to_string(followings);
      run_and_return_if_abort(
         db->Update(txn, "dummy_table", key_following(me), values));
   } else {
      // value->first = "list";
      // value->second = default_empty_buffer;
      // run_and_return_if_abort(db->Read(txn, "dummy_table",
      // key_followers(target_uid), nullptr, values)); std::vector<std::string>
      // followers = string_to_vector(value->second); for (int i = 0; i <
      // followers.size(); i++) {
      //     if (followers[i] == me) {
      //         followers.erase(followers.begin() + i);
      //         break;
      //     }
      // }

      value->first  = "list";
      value->second = vector_to_string(followers);
      run_and_return_if_abort(
         db->Update(txn, "dummy_table", key_followers(target_uid), values));

      value->first  = "list";
      value->second = default_empty_buffer;
      run_and_return_if_abort(
         db->Read(txn, "dummy_table", key_following(me), nullptr, values));
      std::vector<std::string> followings = string_to_vector(value->second);
      for (size_t i = 0; i < followings.size(); i++) {
         if (followings[i] == target_uid) {
            followings.erase(followings.begin() + i);
            break;
         }
      }

      value->first  = "list";
      value->second = vector_to_string(followings);
      run_and_return_if_abort(
         db->Update(txn, "dummy_table", key_following(me), values));
   }

   return (db->Commit(&txn) == ycsbc::DB::kErrorConflict);
}

} // namespace retwis