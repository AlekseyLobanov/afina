#include "MapBasedGlobalLockImpl.h"

#include <mutex>

namespace Afina {
namespace Backend {

void MapBasedGlobalLockImpl::delete_if_needed() {
  while (_backend.size() > _max_size) {
    std::string key_to_remove = _to_remove.front();
    _to_remove.pop_front();

    _backend.erase(_backend.find(key_to_remove));
  }
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Put(const std::string &key,
                                 const std::string &value) {
  std::unique_lock<std::mutex> guard(_lock);

  auto key_pos = _backend.find(key);

  if (_backend.find(key) == _backend.end()) {
    ValueInfo val_info;

    val_info.val = value;
    _to_remove.push_back(value);
    val_info.it = prev(_to_remove.end());
    _backend[key] = val_info;
    delete_if_needed();
  } else {
    key_pos->second.val = value;

    _to_remove.erase(key_pos->second.it);
    _to_remove.push_back(value);
    key_pos->second.it = prev(_to_remove.end());
  }

  return true;
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::PutIfAbsent(const std::string &key,
                                         const std::string &value) {
  std::unique_lock<std::mutex> guard(_lock);
  if (_backend.find(key) != _backend.end())
    return false;

  ValueInfo val_info;

  val_info.val = value;
  _to_remove.push_back(value);
  val_info.it = prev(_to_remove.end());
  _backend[key] = val_info;
  delete_if_needed();
  return true;
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Set(const std::string &key,
                                 const std::string &value) {
  std::unique_lock<std::mutex> guard(_lock);

  auto key_pos = _backend.find(key);

  if (key_pos == _backend.end()) {
    return false;
  } else {
    key_pos->second.val = value;

    _to_remove.erase(key_pos->second.it);
    _to_remove.push_back(value);
    key_pos->second.it = prev(_to_remove.end());
  }
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Delete(const std::string &key) {
  std::unique_lock<std::mutex> guard(_lock);

  auto key_pos = _backend.find(key);

  if (key_pos == _backend.end()) {
    return false;
  } else {
    _to_remove.erase(key_pos->second.it);
    _backend.erase(key_pos);
    return true;
  }
}

// See MapBasedGlobalLockImpl.h
bool MapBasedGlobalLockImpl::Get(const std::string &key,
                                 std::string &value) const {
  std::unique_lock<std::mutex> guard(*const_cast<std::mutex *>(&_lock));

  auto key_pos = _backend.find(key);

  if (key_pos == _backend.end()) {
    return false;
  } else {
    value = key_pos->second.val;
    return true;
  }
}

} // namespace Backend
} // namespace Afina
