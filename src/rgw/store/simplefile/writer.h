// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * Simple filesystem SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#ifndef RGW_STORE_SIMPLEFILE_WRITER_H
#define RGW_STORE_SIMPLEFILE_WRITER_H

#include <memory>
#include "rgw_sal.h"
#include "rgw_sal_store.h"

namespace rgw::sal {

class SimpleFileStore;

class SimpleFileAtomicWriter : public StoreWriter {

 protected:
  rgw::sal::SimpleFileStore *store;
  SimpleFileObject obj;
  const rgw_user &owner;
  const rgw_placement_rule *placement_rule;
  uint64_t olh_epoch;
  const std::string &unique_tag;
  uint64_t bytes_written;

 public:
  SimpleFileAtomicWriter(
    const DoutPrefixProvider *_dpp,
    optional_yield _y,
    std::unique_ptr<rgw::sal::Object> _head_obj,
    SimpleFileStore *_store,
    const rgw_user& _owner,
    const rgw_placement_rule *_ptail_placement_rule,
    uint64_t _olh_epoch,
    const std::string &_unique_tag
  );
  ~SimpleFileAtomicWriter() = default;

  virtual int prepare(optional_yield y) override;
  virtual int process(bufferlist &&data, uint64_t offset) override;
  virtual int complete(
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
  ) override;

  const std::string get_cls_name() const { return "atomic_writer"; }

};

class SimpleFileMultipartWriter : public StoreWriter {

 protected:
  const rgw::sal::SimpleFileStore *store;

 public:
  SimpleFileMultipartWriter(
    const DoutPrefixProvider *_dpp,
    optional_yield y,
    MultipartUpload *upload,
    std::unique_ptr<rgw::sal::Object> _head_obj,
    const SimpleFileStore *store,
    const rgw_user &_owner,
    const rgw_placement_rule *_ptail_placement_rule,
    uint64_t _part_num,
    const std::string &_part_num_str
  );
  ~SimpleFileMultipartWriter() = default;

  virtual int prepare(optional_yield y) override;
  virtual int process(bufferlist &&data, uint64_t offset) override;
  virtual int complete(
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
  ) override;

};

} // ns rgw::sal

#endif // RGW_STORE_SIMPLEFILE_WRITER_H