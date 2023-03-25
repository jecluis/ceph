/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#ifndef RGW_DRIVER_SFS_MULTIPART_TYPES_H
#define RGW_DRIVER_SFS_MULTIPART_TYPES_H

#include <filesystem>

#include "rgw/driver/sfs/uuid_path.h"

#define MULTIPART_PART_SUFFIX_LEN (6 + 1)  // '-' + len(10000) + '\0'

namespace rgw::sal::sfs {
enum class MultipartState {
  NONE = 0,
  INIT,
  INPROGRESS,
  COMPLETE,
  AGGREGATING,
  DONE,
  ABORTED,
  LAST_VALUE = ABORTED
};

class MultipartPartPath : public UUIDPath {
  std::filesystem::path partpath;

 public:
  MultipartPartPath(const uuid_d& uuid, int32_t num) : UUIDPath(uuid) {
    char suffix[MULTIPART_PART_SUFFIX_LEN];
    std::snprintf(suffix, sizeof(suffix), "-%d", num);
    partpath = UUIDPath::to_path().concat(suffix);
  }

  virtual std::filesystem::path to_path() const override { return partpath; }
};
}  // namespace rgw::sal::sfs

inline std::ostream& operator<<(
    std::ostream& out, const rgw::sal::sfs::MultipartPartPath& p
) {
  return out << p.to_path();
}

#endif  // RGW_DRIVER_SFS_MULTIPART_TYPES_H
