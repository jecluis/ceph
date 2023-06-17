// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#pragma once

#include <string>

#include "rgw/driver/sfs/multipart_types.h"
#include "rgw/rgw_common.h"

namespace rgw::sal::sfs::sqlite {

using BLOB = std::vector<char>;

struct DBMultipart {
  int id;
  std::string bucket_id;
  std::string upload_id;
  MultipartState state;
  ceph::real_time state_change_time;
  std::string object_name;
  uuid_d object_uuid;
  std::string meta_str;

  std::string owner_id;
  std::string owner_display_name;
  ceph::real_time mtime;
  BLOB attrs;
  std::string placement_name;
  std::string placement_storage_class;
};

struct DBMultipartPart {
  int id;
  std::string upload_id;
  uint32_t part_num;
  uint64_t size;
  std::optional<std::string> etag;
  std::optional<ceph::real_time> mtime;

  inline bool is_finished() const { return etag.has_value(); }
};

struct DBOPMultipart {
  int id;
  std::string bucket_id;
  std::string upload_id;
  MultipartState state;
  ceph::real_time state_change_time;
  std::string object_name;
  uuid_d object_uuid;
  std::string meta_str;

  ACLOwner owner_id;
  ceph::real_time mtime;
  rgw::sal::Attrs attrs;
  rgw_placement_rule placement;
};

}  // namespace rgw::sal::sfs::sqlite
