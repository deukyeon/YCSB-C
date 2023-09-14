#pragma once

#include <string>
#include <vector>
#include <atomic>

namespace retwis {

extern std::atomic<uint64_t> global_uid;
extern std::atomic<uint64_t> global_pid;

extern int max_value_buffer_size;
extern int max_list_type_value_size;

uint64_t
get_next_global_uid();
uint64_t
get_current_global_uid();
uint64_t
get_next_global_pid();

std::string
key_users();
std::string
key_user(const std::string &uid);
std::string
key_followers(const std::string &uid);
std::string
key_following(const std::string &uid);
std::string
key_post(const std::string &pid);
std::string
key_user_posts(const std::string &uid);
std::string
key_auth(const std::string &auth_key);
std::string
key_timeline();

// utils

std::string
vector_to_string(const std::vector<std::string> &vec);
std::vector<std::string>
string_to_vector(const std::string &str);
void
numeric_sort_vector_of_string(std::vector<std::string> &vec);
void
trim_vector(std::vector<std::string> &vec, size_t max_size);

std::string
generate_hash_string(uint64_t size = 32);

} // namespace retwis