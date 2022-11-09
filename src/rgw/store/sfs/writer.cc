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
#include <memory>
#include "rgw_sal.h"
#include "rgw_sal_sfs.h"
#include "store/sfs/bucket.h"
#include "store/sfs/writer.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

SFSAtomicWriter::SFSAtomicWriter(
  const DoutPrefixProvider *_dpp,
  optional_yield _y,
  std::unique_ptr<rgw::sal::Object> _head_obj,
  SFStore *_store,
  sfs::BucketRef _bucketref,
  const rgw_user& _owner,
  const rgw_placement_rule *_ptail_placement_rule,
  uint64_t _olh_epoch,
  const std::string &_unique_tag
) : StoreWriter(_dpp, _y), store(_store),
    obj(_store, _head_obj->get_key(), _head_obj->get_bucket(), _bucketref, false),
    bucketref(_bucketref),
    owner(_owner),
    placement_rule(_ptail_placement_rule), olh_epoch(_olh_epoch),
    unique_tag(_unique_tag), bytes_written(0) {

  lsfs_dout(dpp, 10) << "head_obj: " << _head_obj->get_key()
                     << ", bucket: " << _head_obj->get_bucket()->get_name()
                     << dendl;
}

int SFSAtomicWriter::prepare(optional_yield y) {
  objref = bucketref->get_or_create(obj.get_key());

  std::filesystem::path object_path =
      store->get_data_path() / objref->get_storage_path();
  std::filesystem::create_directories(object_path.parent_path());

  lsfs_dout(dpp, 10) << "truncate file at " << object_path << dendl;
  std::ofstream ofs(object_path, std::ofstream::trunc);
  ofs.seekp(0);
  ofs.flush();
  ofs.close();
  return 0;
}

int SFSAtomicWriter::process(bufferlist &&data, uint64_t offset) {
  lsfs_dout(dpp, 10) << "data len: " << data.length()
                     << ", offset: " << offset << dendl;

  objref->metadata_change_version_state(store, ObjectState::WRITING);

  std::filesystem::path object_path =
      store->get_data_path() / objref->get_storage_path();
  ceph_assert(std::filesystem::exists(object_path));

  lsfs_dout(dpp, 10) << "write to object at " << object_path << dendl;

  auto mode = \
    std::ofstream::binary | \
    std::ofstream::out | \
    std::ofstream::app;
  std::ofstream ofs(object_path, mode);
  ofs.seekp(offset);
  data.write_stream(ofs);
  ofs.flush();
  ofs.close();
  bytes_written += data.length();

  if (data.length() == 0) {
    lsfs_dout(dpp, 10) << "final piece, wrote " << bytes_written << " bytes"
                       << dendl;
  }
  
  return 0;
}

int SFSAtomicWriter::complete(
  size_t accounted_size,
  const std::string &etag,
  ceph::real_time *mtime,
  ceph::real_time set_mtime,
  std::map<std::string, bufferlist> &attrs,
  ceph::real_time delete_at,
  const char *if_match,
  const char *if_nomatch,
  const std::string *user_data,
  rgw_zone_set *zones_trace,
  bool *canceled,
  optional_yield y
) {
  lsfs_dout(dpp, 10) << "accounted_size: " << accounted_size
                     << ", etag: " << etag
                     << ", mtime: " << to_iso_8601(*mtime)
                     << ", set_mtime: " << to_iso_8601(set_mtime)
                     << ", attrs: " << attrs
                     << ", delete_at: " << to_iso_8601(delete_at)
                     << ", if_match: " << if_match
                     << ", if_nomatch: " << if_nomatch
                     << dendl;

  ceph_assert(bytes_written == accounted_size);

  sfs::Object::Meta &meta = objref->meta;
  meta.size = accounted_size;
  meta.etag = etag;
  meta.mtime = ceph::real_clock::now();
  meta.set_mtime = set_mtime;
  meta.delete_at = delete_at;
  meta.attrs = attrs;
  bucketref->finish(dpp, obj.get_name());

  *mtime = meta.mtime;
  objref->metadata_finish(store);
  return 0;
}

SFSMultipartWriter::SFSMultipartWriter(
  const DoutPrefixProvider *_dpp,
  optional_yield y,
  MultipartUpload *upload,
  std::unique_ptr<rgw::sal::Object> _head_obj,
  const SFStore *store,
  const rgw_user &_owner,
  const rgw_placement_rule *_ptail_placement_rule,
  uint64_t _part_num,
  const std::string &_part_num_str
) : StoreWriter(_dpp, y) { }

int SFSMultipartWriter::prepare(optional_yield y) {
  ldpp_dout(dpp, 10) << "multipart_writer::" << __func__
                     << ": unimplemented, return success" << dendl;
  return 0;
}

int SFSMultipartWriter::process(bufferlist &&data, uint64_t offset) {
  ldpp_dout(dpp, 10) << "multipart_writer::" << __func__
                     << ": unimplemented, return success" << dendl;
  ldpp_dout(dpp, 10) << "multipart_writer::" << __func__
                     << ": data len: " << data.length() << ", offset: " << offset << dendl;
  return 0;
}

int SFSMultipartWriter::complete(
  size_t accounted_size,
  const std::string &etag,
  ceph::real_time *mtime,
  ceph::real_time set_mtime,
  std::map<std::string, bufferlist> &attrs,
  ceph::real_time delete_at,
  const char *if_match,
  const char *if_nomatch,
  const std::string *user_data,
  rgw_zone_set *zones_trace,
  bool *canceled,
  optional_yield y
) {
  ldpp_dout(dpp, 10) << "multipart_writer::" << __func__
                     << ": unimplemented, return success" << dendl;
  return 0;
}

} // ns rgw::sal