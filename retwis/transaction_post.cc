#include "transaction.h"

namespace retwis {

TransactionPost::TransactionPost(ycsbc::DB  *db,
                                 std::string uid,
                                 std::string next_post_id)
   : Transaction(db, uid), next_post_id(next_post_id)
{}

bool
TransactionPost::run()
{
   debug("TransactionPost::run()\n");

   db->Begin(&txn);

   value->first  = "hash";
   value->second = "user_id:" + me + ",time:" + std::to_string(time(nullptr))
                   + ",body:status";
   run_and_return_if_abort(
      db->Insert(txn, "dummy_table", key_post(next_post_id), values));

   value->first  = "list";
   value->second = default_empty_buffer;
   run_and_return_if_abort(
      db->Read(txn, "dummy_table", key_followers(me), nullptr, values));
   std::vector<std::string> followers = string_to_vector(value->second);
   followers.push_back(me);

   std::vector<std::string> posts;
   for (std::string &fid : followers) {
      value->first  = "list";
      value->second = default_empty_buffer;
      run_and_return_if_abort(
         db->Read(txn, "dummy_table", key_user_posts(fid), nullptr, values));
      posts = string_to_vector(value->second);
      posts.push_back(next_post_id);
      // ! The list type value is stored as a string, but it becomes too large.
      // ! So, we maintains just a few elements.
      trim_vector(posts, max_list_type_value_size);
      value->first  = "list";
      value->second = vector_to_string(posts);
      run_and_return_if_abort(
         db->Update(txn, "dummy_table", key_user_posts(fid), values));
   }

   // Push the post on the timeline, and trim the timeline to the
   // newest 1000 elements.

   value->first  = "list";
   value->second = default_empty_buffer;
   run_and_return_if_abort(
      db->Read(txn, "dummy_table", key_timeline(), nullptr, values));
   posts = string_to_vector(value->second);
   posts.push_back(next_post_id);
   // ! The list type value is stored as a string, but it becomes too large.
   // ! So, we maintains just a few elements.
   trim_vector(posts, max_list_type_value_size);

   value->first  = "list";
   value->second = vector_to_string(posts);
   run_and_return_if_abort(
      db->Update(txn, "dummy_table", key_timeline(), values));

   return (db->Commit(&txn) == ycsbc::DB::kErrorConflict);
}
} // namespace retwis