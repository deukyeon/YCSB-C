#include "transaction.h"

namespace retwis {


TransactionTimeline::TransactionTimeline(ycsbc::DB  *db,
                                         std::string uid,
                                         int         count)
   : Transaction(db, uid), count(count)
{}

bool
TransactionTimeline::run()
{
   debug("TransactionTimeline::run()\n");

   db->Begin(&txn);

   value->first  = "list";
   value->second = default_empty_buffer;
   run_and_return_if_abort(
      db->Read(txn, "dummy_table", key_user_posts(me), nullptr, values));
   std::vector<std::string> posts = string_to_vector(value->second);

   // showPost
   int c = 0;
   for (std::string &post_id : posts) {
      value->first  = "hash";
      value->second = default_empty_buffer;
      run_and_return_if_abort(
         db->Read(txn, "dummy_table", key_post(post_id), nullptr, values));
      std::vector<std::string> post = string_to_vector(value->second);
      if (post.empty()) {
         continue;
      }
      std::string user_id;
      for (const std::string &post_elem : post) {
         std::string key = post_elem.substr(0, post_elem.find(":"));
         if (key == "user_id") {
            user_id = post_elem.substr(post_elem.find(":") + 1);
            break;
         }
      }
      value->first  = "hash";
      value->second = default_empty_buffer;
      run_and_return_if_abort(
         db->Read(txn, "dummy_table", key_user(user_id), nullptr, values));

      c++;
      if (c == count) {
         break;
      }
   }

   return (db->Commit(&txn) == ycsbc::DB::kErrorConflict);
}

} // namespace retwis