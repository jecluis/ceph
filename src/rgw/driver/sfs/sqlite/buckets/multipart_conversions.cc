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
#include "multipart_conversions.h"

#include "../conversion_utils.h"
#include "rgw/driver/sfs/multipart_types.h"

namespace rgw::sal::sfs::sqlite {

DBMultipart get_db_multipart(const DBOPMultipart& mp) {
  auto db_mp = DBMultipart{
      .id = mp.id,
      .bucket_id = mp.bucket_id,
      .upload_id = mp.upload_id,
      .state = mp.state,
      .state_change_time = mp.state_change_time,
      .obj_name = mp.obj_name,
      .obj_uuid = mp.obj_uuid,
      .meta_str = mp.meta_str,
      .owner_id = mp.owner_id.get_id().id,
      .owner_display_name = mp.owner_id.get_display_name(),
      .mtime = mp.mtime,
      .placement_name = mp.placement.name,
      .placement_storage_class = mp.placement.storage_class,
  };

  assign_db_value(mp.attrs, db_mp.attrs);

  return db_mp;
}

DBOPMultipart get_rgw_multipart(const DBMultipart& mp) {
  auto mp_op = DBOPMultipart{
      .id = mp.id,
      .bucket_id = mp.bucket_id,
      .upload_id = mp.upload_id,
      .state = mp.state,
      .state_change_time = mp.state_change_time,
      .obj_name = mp.obj_name,
      .obj_uuid = mp.obj_uuid,
      .meta_str = mp.meta_str,
      .mtime = mp.mtime,
  };

  mp_op.owner_id.get_id().id = mp.owner_id;
  mp_op.owner_id.set_name(mp.owner_display_name);
  mp_op.placement.name = mp.placement_name;
  mp_op.placement.storage_class = mp.placement_storage_class;

  assign_value(mp.attrs, mp_op.attrs);

  return mp_op;
}

}  // namespace rgw::sal::sfs::sqlite
