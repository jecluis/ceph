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
#include "driver/sfs/writer.h"

#include <errno.h>
#include <fmt/ostream.h>
#include <unistd.h>

#include <filesystem>
#include <memory>
#include <ranges>
#include <system_error>

#include "driver/sfs/bucket.h"
#include "driver/sfs/writer.h"
#include "rgw/driver/sfs/fmt.h"
#include "rgw/driver/sfs/multipart_types.h"
#include "rgw/driver/sfs/sqlite/sqlite_multipart.h"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_sal_sfs.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static int close_fd_for(
    int fd, const DoutPrefixProvider* dpp, const std::string& whom,
    bool* io_failed
) noexcept {
  ceph_assert(fd >= 0);
  int result = 0;
  int ret;

  ret = ::fsync(fd);
  if (ret < 0) {
    lsfs_dout_for(dpp, -1, whom)
        << fmt::format(
               "failed to fsync fd:{}: {}. continuing.", fd, cpp_strerror(ret)
           )
        << dendl;
  }

  ret = ::close(fd);
  fd = -1;
  if (ret < 0) {
    lsfs_dout_for(dpp, -1, whom)
        << fmt::format(
               "failed closing fd:{}: {}. continuing.", fd, cpp_strerror(ret)
           )
        << dendl;
    switch (ret) {
      case -EDQUOT:
      case -ENOSPC:
        result = -ERR_QUOTA_EXCEEDED;
        break;
      default:
        result = -ERR_INTERNAL_ERROR;
    }
    if (io_failed) {
      *io_failed = true;
    }
  }
  return result;
}

namespace rgw::sal {
SFSAtomicWriter::SFSAtomicWriter(
    const DoutPrefixProvider* _dpp, optional_yield _y,
    rgw::sal::Object* _head_obj, SFStore* _store, sfs::BucketRef _bucketref,
    const rgw_user& _owner, const rgw_placement_rule* _ptail_placement_rule,
    uint64_t _olh_epoch, const std::string& _unique_tag
)
    : StoreWriter(_dpp, _y),
      store(_store),
      obj(_store, _head_obj->get_key(), _head_obj->get_bucket(), _bucketref,
          false),
      bucketref(_bucketref),
      owner(_owner),
      placement_rule(_ptail_placement_rule),
      olh_epoch(_olh_epoch),
      unique_tag(_unique_tag),
      bytes_written(0),
      io_failed(false),
      fd(-1) {
  lsfs_dout(dpp, 10) << fmt::format(
                            "head_obj: {}, bucket: {}", _head_obj->get_key(),
                            _head_obj->get_bucket()->get_name()
                        )
                     << dendl;
}

SFSAtomicWriter::~SFSAtomicWriter() {
  if (fd >= 0) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "BUG: fd:{} still open. closing. (io_failed:{}, object_path:{})",
               fd, io_failed, object_path.string()
           )
        << dendl;
    close();
  }
}

int SFSAtomicWriter::open() noexcept {
  std::error_code ec;
  std::filesystem::create_directories(object_path.parent_path(), ec);
  if (ec) {
    lsfs_dout(dpp, -1) << "failed to mkdir object path " << object_path << ": "
                       << ec << dendl;
    switch (ec.value()) {
      case ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }

  int ret;
  ret = ::open(
      object_path.c_str(), O_CREAT | O_TRUNC | O_CLOEXEC | O_WRONLY, 0644
  );
  if (ret < 0) {
    lsfs_dout(dpp, -1) << "error opening file " << object_path << ": "
                       << cpp_strerror(-fd) << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  fd = ret;
  return 0;
}

int SFSAtomicWriter::close() noexcept {
  return close_fd_for(fd, dpp, get_cls_name(), &io_failed);
}

void SFSAtomicWriter::cleanup() noexcept {
  lsfs_dout(dpp, -1) << fmt::format(
                            "cleaning up failed upload to file {}. "
                            "returning error.",
                            object_path.string()
                        )
                     << dendl;

  std::error_code ec;
  std::filesystem::remove(object_path, ec);
  if (ec) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed deleting file {}: {} {}. ignoring.",
                              object_path.string(), ec.message(), ec.value()
                          )
                       << dendl;
  }

  const auto dir_fd = ::open(object_path.parent_path().c_str(), O_RDONLY);
  int ret = ::fsync(dir_fd);
  if (ret < 0) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "failed fsyncing dir {} fd:{} for obj file {}: {}. ignoring.",
               object_path.parent_path().string(), dir_fd, object_path.string(),
               cpp_strerror(ret)
           )
        << dendl;
  }

  try {
    objref->delete_object_version(store);
  } catch (const std::system_error& e) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "failed to remove failed upload version from database {}: {}",
               store->db_conn->get_storage().filename(), e.what()
           )
        << dendl;
  }
}

int SFSAtomicWriter::prepare(optional_yield y) {
  if (store->filesystem_stats_avail_bytes.load() <
      store->min_space_left_for_data_write_ops_bytes) {
    lsfs_dout(dpp, 10) << fmt::format(
                              "filesystem stat reservation check hit. "
                              "avail_bytes:{} avail_pct:{} total_bytes:{}. "
                              "returning quota error.",
                              store->filesystem_stats_avail_bytes,
                              store->filesystem_stats_avail_percent,
                              store->filesystem_stats_total_bytes
                          )
                       << dendl;
    return -ERR_QUOTA_EXCEEDED;
  }

  try {
    objref = bucketref->create_version(obj.get_key());
  } catch (const std::system_error& e) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "exception while fetching obj ref from bucket {} db:{}: {}. "
               "failing operation.",
               bucketref->get_bucket_id(),
               store->db_conn->get_storage().filename(), e.what()
           )
        << dendl;
    switch (e.code().value()) {
      case -EDQUOT:
      case -ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }
  object_path = store->get_data_path() / objref->get_storage_path();

  lsfs_dout(dpp, 10) << "creating file at " << object_path << dendl;

  return open();
}

int SFSAtomicWriter::process(bufferlist&& data, uint64_t offset) {
  lsfs_dout(dpp, 10)
      << fmt::format(
             "data len: {}, offset: {}, io_failed: {}, fd: {}, fn: {}",
             data.length(), offset, io_failed, fd, object_path.string()
         )
      << dendl;
  if (io_failed) {
    return -ERR_INTERNAL_ERROR;
  }

  if (data.length() == 0) {
    lsfs_dout(dpp, 10) << "final piece, wrote " << bytes_written << " bytes"
                       << dendl;
    return 0;
  }

  ceph_assert(fd >= 0);
  int write_ret = data.write_fd(fd, offset);
  if (write_ret < 0) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed to write size:{} offset:{} to fd:{}: {}. "
                              "marking writer failed. "
                              "failing future io. "
                              "will delete partial data on completion. "
                              "returning internal error.",
                              data.length(), offset, fd, cpp_strerror(write_ret)
                          )
                       << dendl;
    io_failed = true;
    close();
    cleanup();
    switch (write_ret) {
      case -EDQUOT:
      case -ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }
  bytes_written += data.length();
  return 0;
}

int SFSAtomicWriter::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y
) {
  lsfs_dout(dpp, 10)
      << fmt::format(
             "accounted_size: {}, etag: {}, set_mtime: {}, attrs: {}, "
             "delete_at: {}, if_match: {}, if_nomatch: {}",
             accounted_size, etag, to_iso_8601(set_mtime),
             fmt::join(std::views::keys(attrs), ", "), to_iso_8601(delete_at),
             if_match ? if_match : "NA", if_nomatch ? if_nomatch : "NA"
         )
      << dendl;

  const auto now = ceph::real_clock::now();
  if (bytes_written != accounted_size) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "data written != accounted size. {} vs. {}. failing operation. "
               "returning internal error.",
               bytes_written, accounted_size
           )
        << dendl;
    close();
    cleanup();
    return -ERR_INTERNAL_ERROR;
  }

  int result = close();
  if (io_failed) {
    cleanup();
    return result;
  }

  // for object-locking enabled buckets, set the bucket's object-locking
  // profile when not defined on the object itself
  if (bucketref->get_info().obj_lock_enabled() &&
      bucketref->get_info().obj_lock.has_rule()) {
    auto iter = attrs.find(RGW_ATTR_OBJECT_RETENTION);
    if (iter == attrs.end()) {
      real_time lock_until_date =
          bucketref->get_info().obj_lock.get_lock_until_date(now);
      string mode = bucketref->get_info().obj_lock.get_mode();
      RGWObjectRetention obj_retention(mode, lock_until_date);
      bufferlist bl;
      obj_retention.encode(bl);
      attrs[RGW_ATTR_OBJECT_RETENTION] = bl;
    }
  }

  objref->update_attrs(attrs);
  objref->update_meta(
      {.size = accounted_size,
       .etag = etag,
       .mtime = now,
       .set_mtime = set_mtime,
       .delete_at = delete_at}
  );

  if (mtime != nullptr) {
    *mtime = now;
  }
  try {
    objref->metadata_finish(store, bucketref->get_info().versioning_enabled());
  } catch (const std::system_error& e) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "failed to update db object {}: {}. "
                              "failing operation. ",
                              objref->name, e.what()
                          )
                       << dendl;
    io_failed = true;
    cleanup();
    switch (e.code().value()) {
      case -EDQUOT:
      case -ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }
  return 0;
}

namespace sfs {

SFSMultipartWriterV2::~SFSMultipartWriterV2() {
  if (fd > 0) {
    close();
  }
}

int SFSMultipartWriterV2::close() noexcept {
  return close_fd_for(fd, dpp, get_cls_name(), nullptr);
}

int SFSMultipartWriterV2::prepare(optional_yield y) {
  lsfs_dout(dpp, 10) << fmt::format(
                            "upload_id: {}, part: {}", upload_id, part_num
                        )
                     << dendl;

  // check if store has enough space.
  if (store->filesystem_stats_avail_bytes.load() <
      store->min_space_left_for_data_write_ops_bytes) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "filesystem stat reservation check hit. avail_bytes: {}, "
               "avail_pct: {}, total_bytes: {} -- return quota error.",
               store->filesystem_stats_avail_bytes,
               store->filesystem_stats_avail_percent,
               store->filesystem_stats_total_bytes
           )
        << dendl;
    return -ERR_QUOTA_EXCEEDED;
  }

  sqlite::SQLiteMultipart mpdb(store->db_conn);

  // create part entry if it doesn't exist. Will also move the upload to "in
  // progress" if it's still in "init".
  std::string error_str;
  auto entry = mpdb.create_or_reset_part(upload_id, part_num, &error_str);
  if (!entry.has_value()) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "error adding/replacing part {} in db, upload_id: {}: {}",
               part_num, upload_id, error_str
           )
        << dendl;
    return -ERR_NO_SUCH_UPLOAD;
  }

  // prepare upload's file paths.
  auto mp = mpdb.get_multipart(upload_id);
  if (!mp.has_value()) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "multipart upload {} not found!", upload_id
                          )
                       << dendl;
    return -ERR_NO_SUCH_UPLOAD;
  }
  if (mp->state != MultipartState::INPROGRESS) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "multipart upload {} not available -- raced with "
                              "abort or complete!",
                              upload_id
                          )
                       << dendl;
    return -ERR_NO_SUCH_UPLOAD;
  }

  MultipartPartPath partpath(mp->object_uuid, part_num);
  std::filesystem::path path = store->get_data_path() / partpath.to_path();

  std::error_code ec;
  std::filesystem::create_directories(path.parent_path(), ec);
  if (ec) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "error creating multipart upload's part paths: {}", ec.message()
           )
        << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  // truncate file

  int ret =
      ::open(path.c_str(), O_CREAT | O_TRUNC | O_CLOEXEC | O_WRONLY, 0600);
  if (ret < 0) {
    lsfs_dout(
        dpp, -1
    ) << fmt::format("error opening file {}: {}", path, cpp_strerror(errno))
      << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  fd = ret;

  ret = ::fsync(fd);
  if (ret < 0) {
    lsfs_dout(
        dpp, -1
    ) << fmt::format("error sync'ing opened file: {}", cpp_strerror(errno))
      << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  return 0;
}

int SFSMultipartWriterV2::process(bufferlist&& data, uint64_t offset) {
  auto len = data.length();

  lsfs_dout(dpp, 10)
      << fmt::format(
             "upload_id: {}, part: {}, data(len: {}, offset: {}), written: {}",
             upload_id, part_num, len, offset, bytes_written
         )
      << dendl;

  sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto mp = mpdb.get_multipart(upload_id);
  if (!mp.has_value()) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "multipart upload {} not found!", upload_id
                          )
                       << dendl;
    return -ERR_NO_SUCH_UPLOAD;
  }
  if (mp->state != MultipartState::INPROGRESS) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "multipart upload {} not available -- raced with "
                              "abort or complete!",
                              upload_id
                          )
                       << dendl;
    return -ERR_NO_SUCH_UPLOAD;
  }

  if (len == 0) {
    lsfs_dout(dpp, 10) << "nothing to write" << dendl;
    return 0;
  }

  ceph_assert(fd >= 0);
  int write_ret = data.write_fd(fd, offset);
  if (write_ret < 0) {
    lsfs_dout(dpp, -1)
        << fmt::format(
               "failed to write size: {}, offset: {}, to fd: {}: {}", len,
               offset, fd, cpp_strerror(write_ret)
           )
        << dendl;
    switch (write_ret) {
      case -EDQUOT:
      case -ENOSPC:
        return -ERR_QUOTA_EXCEEDED;
      default:
        return -ERR_INTERNAL_ERROR;
    }
  }
  bytes_written += len;
  return 0;
}

int SFSMultipartWriterV2::complete(
    size_t accounted_size, const std::string& etag, ceph::real_time* mtime,
    ceph::real_time set_mtime, std::map<std::string, bufferlist>& attrs,
    ceph::real_time delete_at, const char* if_match, const char* if_nomatch,
    const std::string* user_data, rgw_zone_set* zones_trace, bool* canceled,
    optional_yield y
) {
  // NOTE(jecluis): ignored parameters:
  //  * set_mtime
  //  * attrs
  //  * delete_at
  //  * if_match
  //  * if_nomatch
  //  * user_data
  //  * zones_trace
  //  * canceled

  lsfs_dout(dpp, 10) << fmt::format(
                            "accounted_size: {}, etag: {}, set_mtime: {}, "
                            "delete_at: {}, if_match: {}, if_nomatch: {}",
                            accounted_size, etag, to_iso_8601(set_mtime),
                            to_iso_8601(delete_at), if_match ? if_match : "N/A",
                            if_nomatch ? if_nomatch : "N/A"
                        )
                     << dendl;
  lsfs_dout(dpp, 10) << "attrs: " << attrs << dendl;

  if (bytes_written != accounted_size) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "bytes_written != accounted_size, expected {} "
                              "byte, found {} byte.",
                              bytes_written, accounted_size
                          )
                       << dendl;
    return -ERR_INTERNAL_ERROR;
  }

  // finish part in db
  sqlite::SQLiteMultipart mpdb(store->db_conn);
  auto res = mpdb.finish_part(upload_id, part_num, etag, bytes_written);
  if (!res) {
    lsfs_dout(dpp, -1) << fmt::format(
                              "unable to finish upload_id {}, part_num {}",
                              upload_id, part_num
                          )
                       << dendl;
    return -ERR_INTERNAL_ERROR;
  }
  auto entry = mpdb.get_part(upload_id, part_num);
  ceph_assert(entry.has_value());
  ceph_assert(entry->mtime.has_value());

  if (mtime) {
    *mtime = *entry->mtime;
  }

  return 0;
}

}  // namespace sfs

}  // namespace rgw::sal
