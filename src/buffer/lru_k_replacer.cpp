//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  BUSTUB_ASSERT(frame_id != nullptr, "frame_id is nullptr");
  std::lock_guard<std::mutex> lk(latch_);
  for (auto it = --histroy_list_.end(); it != histroy_list_.end(); it--) {
    if (it->evictable_) {
      *frame_id = it->key_;
      histroy_list_.erase(it);
      map_.erase(*frame_id);
      curr_size_--;
      return true;
    }
  }

  for (auto it = --cache_list_.end(); it != cache_list_.end(); it--) {
    if (it->evictable_) {
      *frame_id = it->key_;
      cache_list_.erase(it);
      map_.erase(*frame_id);
      curr_size_--;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "frame is invalid");
  std::lock_guard<std::mutex> lk(latch_);
  auto access_pos = map_.find(frame_id);
  if (access_pos == map_.end()) {
    if (IsFull()) {
      // TODO(lsh)
      return;
    }
    Entry new_entry = {
        .key_ = frame_id,
    };
    histroy_list_.emplace_front(new_entry);
    map_.emplace(frame_id, histroy_list_.begin());
    access_pos = map_.find(frame_id);
  }

  auto &entry = access_pos->second;
  entry->cnt_++;

  if (entry->cnt_ < k_) {
    return;
  }

  auto &del_list = entry->is_in_history_list_ ? histroy_list_ : cache_list_;
  auto &insert_list = cache_list_;
  entry->is_in_history_list_ = false;
  Entry new_entry = *entry;
  del_list.erase(entry);
  map_.erase(frame_id);
  insert_list.emplace_front(new_entry);
  map_.emplace(frame_id, insert_list.begin());
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lk(latch_);
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "frame is invalid");
  auto set_pos = map_.find(frame_id);
  if (set_pos == map_.end()) {
    return;
  }

  auto &entry = set_pos->second;
  if (entry->evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!entry->evictable_ && set_evictable) {
    curr_size_++;
  }
  entry->evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_), "frame is invalid");
  std::lock_guard<std::mutex> lk(latch_);
  auto del_pos = map_.find(frame_id);
  if (del_pos == map_.end()) {
    return;
  }

  auto &entry = del_pos->second;
  BUSTUB_ASSERT(entry->evictable_, "frame should be evictable");
  auto &op_list = entry->is_in_history_list_ ? histroy_list_ : cache_list_;
  op_list.erase(entry);
  map_.erase(del_pos);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

auto LRUKReplacer::IsFull() -> bool { return curr_size_ >= replacer_size_; }

}  // namespace bustub
