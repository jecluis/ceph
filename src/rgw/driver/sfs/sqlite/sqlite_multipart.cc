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
#include "rgw/driver/sfs/sqlite/sqlite_multipart.h"

#include "rgw/driver/sfs/multipart_types.h"
#include "rgw/driver/sfs/sqlite/buckets/bucket_definitions.h"
#include "rgw/driver/sfs/sqlite/buckets/multipart_conversions.h"

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteMultipart::SQLiteMultipart(DBConnRef _conn) : conn(_conn) {}

std::optional<std::vector<DBOPMultipart>> SQLiteMultipart::list_multiparts(
    const std::string& bucket_name, const std::string& prefix,
    const std::string& marker, const std::string& delim, const int& max_uploads,
    bool* is_truncated
) const {
  std::vector<DBOPMultipart> entries;

  auto storage = conn->get_storage();

  auto bucket_entries = storage.get_all<DBBucket>(
      where(is_equal(&DBBucket::bucket_name, bucket_name))
  );
  if (bucket_entries.size() == 0) {
    return std::nullopt;
  }
  ceph_assert(bucket_entries.size() == 1);
  auto bucket_id = bucket_entries.front().bucket_id;

  auto db_entries = storage.get_all<DBMultipart>(
      where(
          is_equal(&DBMultipart::bucket_id, bucket_id) and
          greater_or_equal(&DBMultipart::state, MultipartState::INIT) and
          lesser_than(&DBMultipart::state, MultipartState::COMPLETE) and
          greater_or_equal(&DBMultipart::meta_str, marker) and
          like(&DBMultipart::object_name, fmt::format("{}%", prefix))
      ),
      order_by(&DBMultipart::meta_str), limit(max_uploads + 1)
  );

  ceph_assert(max_uploads >= 0);
  if (db_entries.size() > static_cast<size_t>(max_uploads)) {
    ceph_assert(db_entries.size() == static_cast<size_t>(max_uploads + 1));
    db_entries.pop_back();
    if (is_truncated) {
      *is_truncated = true;
    }
  }

  for (const auto& e : db_entries) {
    entries.push_back(get_rgw_multipart(e));
  }

  return entries;
}

int SQLiteMultipart::abort_multiparts(const std::string& bucket_name) const {
  auto storage = conn->get_storage();
  auto bucket_ids_vec = storage.select(
      &DBBucket::bucket_id, where(is_equal(&DBBucket::bucket_name, bucket_name))
  );
  if (bucket_ids_vec.size() == 0) {
    return -ERR_NO_SUCH_BUCKET;
  }
  ceph_assert(bucket_ids_vec.size() == 1);
  auto bucket_id = bucket_ids_vec.front();

  uint64_t num_changes = 0;
  storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::ABORTED,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::bucket_id, bucket_id) and
            greater_or_equal(&DBMultipart::state, MultipartState::INIT) and
            lesser_than(&DBMultipart::state, MultipartState::COMPLETE)
        )
    );
    num_changes = storage.changes();
    return true;
  });

  return num_changes;
}

std::optional<DBOPMultipart> SQLiteMultipart::get_multipart(
    const std::string& upload_id
) const {
  if (upload_id.empty()) {
    return std::nullopt;
  }

  auto storage = conn->get_storage();
  auto entries = storage.get_all<DBMultipart>(
      where(is_equal(&DBMultipart::upload_id, upload_id))
  );
  ceph_assert(entries.size() <= 1);
  std::optional<DBOPMultipart> mp;
  if (entries.size() == 1) {
    mp = get_rgw_multipart(entries[0]);
  }
  return mp;
}

uint SQLiteMultipart::insert(const DBOPMultipart& mp) const {
  auto storage = conn->get_storage();
  auto db_mp = get_db_multipart(mp);
  return storage.insert(db_mp);
}

std::vector<DBMultipartPart> SQLiteMultipart::list_parts(
    const std::string& upload_id, int num_parts, int marker, int* next_marker,
    bool* truncated
) const {
  auto storage = conn->get_storage();
  std::vector<DBMultipartPart> db_entries;
  db_entries = storage.get_all<DBMultipartPart>(
      where(
          is_equal(&DBMultipartPart::upload_id, upload_id) and
          is_not_null(&DBMultipartPart::etag) and
          c(&DBMultipartPart::id) >= marker
      ),
      order_by(&DBMultipartPart::id), limit(num_parts + 1)
  );

  ceph_assert(num_parts >= 0);
  if (db_entries.size() > static_cast<size_t>(num_parts)) {
    ceph_assert(db_entries.size() == static_cast<size_t>(num_parts + 1));
    // we got a next marker on the last row
    if (truncated) {
      *truncated = true;
    }
    auto next = db_entries.back();
    db_entries.pop_back();
    if (next_marker) {
      *next_marker = next.id;
    }
  }

  return db_entries;
}

std::vector<DBMultipartPart> SQLiteMultipart::get_parts(
    const std::string& upload_id
) const {
  auto storage = conn->get_storage();
  auto db_entries = storage.get_all<DBMultipartPart>(
      where(is_equal(&DBMultipartPart::upload_id, upload_id)),
      order_by(&DBMultipartPart::part_num)
  );
  return db_entries;
}

std::optional<DBMultipartPart> SQLiteMultipart::get_part(
    const std::string& upload_id, uint32_t part_num
) const {
  auto storage = conn->get_storage();
  auto entries = storage.get_all<DBMultipartPart>(where(
      is_equal(&DBMultipartPart::upload_id, upload_id) and
      is_equal(&DBMultipartPart::part_num, part_num)
  ));
  if (entries.empty()) {
    return std::nullopt;
  }
  ceph_assert(entries.size() == 1);
  return entries.front();
}

std::optional<DBMultipartPart> SQLiteMultipart::create_or_reset_part(
    const std::string& upload_id, uint32_t part_num, std::string* error_str
) const {
  auto storage = conn->get_storage();
  std::optional<DBMultipartPart> entry;

  storage.transaction([&]() mutable {
    auto cnt = storage.count<DBMultipart>(where(
        is_equal(&DBMultipart::upload_id, upload_id) and
        (is_equal(&DBMultipart::state, MultipartState::INPROGRESS) or
         is_equal(&DBMultipart::state, MultipartState::INIT))
    ));
    if (cnt != 1) {
      if (error_str) {
        *error_str = "could not find upload";
      }
      return false;
    }

    // set multipart upload as being in progress
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::INPROGRESS,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::upload_id, upload_id) and
            is_equal(&DBMultipart::state, MultipartState::INIT)
        )
    );

    // find if there's already a part with said upload_id/part_num combination
    auto parts = storage.get_all<DBMultipartPart>(where(
        is_equal(&DBMultipartPart::upload_id, upload_id) and
        is_equal(&DBMultipartPart::part_num, part_num)
    ));
    DBMultipartPart part;

    if (parts.size() > 0) {
      ceph_assert(parts.size() == 1);
      // reset part entry
      part = parts.front();
      part.size = 0;
      part.etag = std::nullopt;
      part.mtime = std::nullopt;
      try {
        storage.replace(part);
      } catch (const std::system_error& e) {
        if (error_str) {
          *error_str = e.what();
        }
        return false;
      }
    } else {
      part = DBMultipartPart{
          .upload_id = upload_id, .part_num = part_num, .size = 0};
      try {
        storage.insert(part);
      } catch (const std::system_error& e) {
        if (error_str) {
          *error_str = e.what();
        }
        return false;
      }
    }

    entry = part;
    return true;
  });

  return entry;
}

bool SQLiteMultipart::finish_part(
    const std::string& upload_id, uint32_t part_num, const std::string& etag,
    uint64_t bytes_written
) const {
  auto storage = conn->get_storage();
  bool committed = storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipartPart::etag) = etag,
            c(&DBMultipartPart::mtime) = ceph::real_time::clock::now(),
            c(&DBMultipartPart::size) = bytes_written),
        where(
            is_equal(&DBMultipartPart::upload_id, upload_id) and
            is_equal(&DBMultipartPart::part_num, part_num) and
            is_null(&DBMultipartPart::etag)
        )
    );
    if (storage.changes() != 1) {
      return false;
    }
    return true;
  });
  return committed;
}

bool SQLiteMultipart::abort(const std::string& upload_id) const {
  auto storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::ABORTED,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::upload_id, upload_id) and
            greater_or_equal(&DBMultipart::state, MultipartState::INIT) and
            lesser_than(&DBMultipart::state, MultipartState::COMPLETE)
        )
    );
    auto num_aborted = storage.changes();
    if (num_aborted == 0) {
      return false;
    }
    ceph_assert(num_aborted == 1);
    return true;
  });

  return committed;
}

static int _mark_complete(
    rgw::sal::sfs::sqlite::Storage& storage, const std::string& upload_id
) {
  storage.update_all(
      set(c(&DBMultipart::state) = MultipartState::COMPLETE,
          c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
      where(
          is_equal(&DBMultipart::upload_id, upload_id) and
          greater_or_equal(&DBMultipart::state, MultipartState::INIT) and
          lesser_or_equal(&DBMultipart::state, MultipartState::INPROGRESS)
      )
  );
  return storage.changes();
}

bool SQLiteMultipart::mark_complete(const std::string& upload_id) const {
  auto storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    auto num_complete = _mark_complete(storage, upload_id);
    if (num_complete == 0) {
      return false;
    }
    ceph_assert(num_complete == 1);
    return true;
  });

  return committed;
}

bool SQLiteMultipart::mark_complete(
    const std::string& upload_id, bool* duplicate
) const {
  ceph_assert(duplicate != nullptr);
  auto storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    auto entries = storage.get_all<DBMultipart>(
        where(is_equal(&DBMultipart::upload_id, upload_id))
    );
    ceph_assert(entries.size() <= 1);
    if (entries.size() == 0) {
      return false;
    }
    auto entry = entries.front();
    if (entry.state == MultipartState::DONE) {
      *duplicate = true;
      return true;
    }

    auto num_complete = _mark_complete(storage, upload_id);
    if (num_complete == 0) {
      return false;
    }
    ceph_assert(num_complete == 1);
    return true;
  });
  return committed;
}

bool SQLiteMultipart::mark_aggregating(const std::string& upload_id) const {
  auto storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::AGGREGATING,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::upload_id, upload_id) and
            is_equal(&DBMultipart::state, MultipartState::COMPLETE)
        )
    );
    auto num_changed = storage.changes();
    if (num_changed == 0) {
      return false;
    }
    ceph_assert(num_changed == 1);
    return true;
  });

  return committed;
}

bool SQLiteMultipart::mark_done(const std::string& upload_id) const {
  auto storage = conn->get_storage();
  auto committed = storage.transaction([&]() mutable {
    storage.update_all(
        set(c(&DBMultipart::state) = MultipartState::DONE,
            c(&DBMultipart::state_change_time) = ceph::real_time::clock::now()),
        where(
            is_equal(&DBMultipart::upload_id, upload_id) and
            is_equal(&DBMultipart::state, MultipartState::AGGREGATING)
        )
    );
    auto num_changed = storage.changes();
    if (num_changed == 0) {
      return false;
    }
    ceph_assert(num_changed == 1);
    return true;
  });
  return committed;
}

}  // namespace rgw::sal::sfs::sqlite
