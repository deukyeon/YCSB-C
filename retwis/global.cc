#include "global.h"

#include <sstream>
#include <iterator>
#include <algorithm>
#include <iostream>

namespace retwis {

std::atomic<uint64_t> global_uid;
std::atomic<uint64_t> global_pid;

int max_value_buffer_size    = 4096;
int max_list_type_value_size = 20;

uint64_t
get_next_global_uid()
{
   return global_uid.fetch_add(1);
}

uint64_t
get_current_global_uid()
{
   return global_uid.load();
}

uint64_t
get_next_global_pid()
{
   return global_pid.fetch_add(1);
}

std::string
key_users()
{
   return "users";
}

std::string
key_user(const std::string &uid)
{
   return "user:" + uid;
}

std::string
key_followers(const std::string &uid)
{
   return "followers:" + uid;
}

std::string
key_following(const std::string &uid)
{
   return "following:" + uid;
}

std::string
key_post(const std::string &pid)
{
   return "post:" + pid;
}

std::string
key_user_posts(const std::string &uid)
{
   return "posts:" + uid + ":";
}

std::string
key_auth(const std::string &auth_key)
{
   return "auth:" + auth_key;
}

std::string
key_timeline()
{
   return "timeline";
}

std::string
vector_to_string(const std::vector<std::string> &vec)
{
   std::ostringstream oss;
   std::copy(
      vec.begin(), vec.end(), std::ostream_iterator<std::string>(oss, ","));
   return oss.str();
};

std::string
strstrip(const std::string &str, const std::string &chars = " \t\n\r")
{
   size_t start = str.find_first_not_of(chars);
   if (start == std::string::npos) {
      return ""; // Entire string is composed of characters to be stripped.
   }

   size_t end = str.find_last_not_of(chars);

   return str.substr(start, end - start + 1);
}

std::vector<std::string>
string_to_vector(const std::string &str)
{
   std::vector<std::string> vec;
   std::istringstream       iss(str);
   std::string              token;
   while (std::getline(iss, token, ',')) {
      token = strstrip(token);
      if (token.empty()) {
         continue;
      }
      vec.push_back(token);
   }
   return vec;
}

void
numeric_sort_vector_of_string(std::vector<std::string> &vec)
{
   if (vec.size() < 2) {
      return;
   }
   std::sort(vec.begin(), vec.end(), [](std::string &a, std::string &b) {
      return std::stoi(a) < std::stoi(b);
   });
}

void
trim_vector(std::vector<std::string> &vec, size_t max_size)
{
   if (vec.size() <= max_size) {
      return;
   }
   vec.erase(vec.begin(), vec.begin() + (vec.size() - max_size));
}

std::string
generate_hash_string(uint64_t size)
{
   static const char alphanum[] = "0123456789"
                                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                  "abcdefghijklmnopqrstuvwxyz";
   std::string       tmp_s;
   tmp_s.reserve(size);
   for (uint64_t i = 0; i < size; ++i) {
      tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
   }
   return tmp_s;
}

} // namespace retwis