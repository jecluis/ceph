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

#include "common/ceph_crypto.h"
#include "rgw/driver/sfs/uuid_path.h"
#include "rgw/rgw_common.h"

#define MULTIPART_PART_SUFFIX_LEN (6 + 1)  // '-' + len(10000) + '\0'

namespace rgw::sal::sfs {

using TOPNSPC::crypto::MD5;

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

class ETagBuilder {
  MD5 hash;

 public:
  ETagBuilder() = default;
  ~ETagBuilder() = default;

  void update(const std::string& val) {
    char buf[CEPH_CRYPTO_MD5_DIGESTSIZE];
    hex_to_buf(val.c_str(), buf, CEPH_CRYPTO_MD5_DIGESTSIZE);
    hash.Update((const unsigned char*)buf, sizeof(buf));
  }

  void update(ceph::bufferlist& bl) {
    hash.Update((const unsigned char*)bl.c_str(), bl.length());
  }

  const std::string final() {
    // final string contains twice as many bytes because it's storing each byte
    // as an hex string (i.e., one 8 bit char for each 4 bit per byte).
    char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2];
    hash.Final((unsigned char*)final_etag);
    buf_to_hex((unsigned char*)final_etag, sizeof(final_etag), final_etag_str);
    return final_etag_str;
  }
};

}  // namespace rgw::sal::sfs

inline std::ostream& operator<<(
    std::ostream& out, const rgw::sal::sfs::MultipartPartPath& p
) {
  return out << p.to_path();
}

#endif  // RGW_DRIVER_SFS_MULTIPART_TYPES_H
