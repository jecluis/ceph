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
#include "dbconn.h"

#include <sqlite3.h>

#include <filesystem>
#include <system_error>

#include "common/dout.h"

namespace rgw::sal::sfs::sqlite {

static int get_version(
    CephContext* cct, rgw::sal::sfs::sqlite::Storage& storage
) {
  try {
    return storage.pragma.user_version();
  } catch (const std::system_error& e) {
    lsubdout(cct, rgw, -1) << "error opening db: " << e.code().message() << " ("
                           << e.code().value() << "), " << e.what() << dendl;
    throw e;
  }
}

static int upgrade_metadata_from_v1(sqlite3* db, std::string* errmsg) {
  auto rc = sqlite3_exec(
      db,
      fmt::format(
          "CREATE TABLE '{}' ("
          "'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
          "'bucket_id' TEXT NOT NULL,"
          "'upload_id' TEXT NOT NULL,"
          "'state' INTEGER NOT NULL ,"
          "'state_change_time' INTEGER NOT NULL,"
          "'object_name' TEXT NOT NULL,"
          "'object_uuid' TEXT NOT NULL,"
          "'meta_str' TEXT NOT NULL,"
          "'owner_id' TEXT NOT NULL,"
          "'owner_display_name' TEXT NOT NULL,"
          "'mtime' INTEGER NOT NULL,"
          "'attrs' BLOB NOT NULL,"
          "'placement_name' TEXT NOT NULL,"
          "'placement_storage_class' TEXT NOT NULL,"
          "UNIQUE(upload_id),"
          "UNIQUE(bucket_id, upload_id),"
          "UNIQUE(object_uuid),"
          "FOREIGN KEY('bucket_id') REFERENCES '{}' ('bucket_id')"
          ")",
          MULTIPARTS_TABLE, BUCKETS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );
  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error creating '{}' table: {}", MULTIPARTS_TABLE, sqlite3_errmsg(db)
      );
    }
    return -1;
  }

  rc = sqlite3_exec(
      db,
      fmt::format(
          "CREATE TABLE '{}' ("
          "'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
          "'upload_id' TEXT NOT NULL,"
          "'part_num' INTEGER NOT NULL,"
          "'len' INTEGER NOT NULL,"
          "'etag' TEXT,"
          "'mtime' INTEGER,"
          "UNIQUE(upload_id, part_num),"
          "FOREIGN KEY('upload_id') REFERENCES '{}'('upload_id')"
          ")",
          MULTIPARTS_PARTS_TABLE, MULTIPARTS_TABLE
      )
          .c_str(),
      nullptr, nullptr, nullptr
  );
  if (rc != SQLITE_OK) {
    if (errmsg != nullptr) {
      *errmsg = fmt::format(
          "Error creating '{}' table: {}", MULTIPARTS_PARTS_TABLE,
          sqlite3_errmsg(db)
      );
    }
    return -1;
  }
  return 0;
}

static void upgrade_metadata(
    CephContext* cct, rgw::sal::sfs::sqlite::Storage& storage, sqlite3* db
) {
  while (true) {
    int cur_version = get_version(cct, storage);
    ceph_assert(cur_version <= SFS_METADATA_VERSION);
    ceph_assert(cur_version >= SFS_METADATA_MIN_VERSION);
    if (cur_version == SFS_METADATA_VERSION) {
      break;
    }

    if (cur_version == 1) {
      std::string errmsg;
      auto rc = upgrade_metadata_from_v1(db, &errmsg);
      if (rc < 0) {
        auto err = fmt::format("Error upgrading from version 1: {}", errmsg);
        lsubdout(cct, rgw, 10) << err << dendl;
        throw sqlite_sync_exception(err);
      }
    }

    lsubdout(cct, rgw, 1)
        << fmt::format(
               "upgraded metadata from version {} to version {}", cur_version,
               cur_version + 1
           )
        << dendl;
    storage.pragma.user_version(cur_version + 1);
  }
}

void DBConn::maybe_upgrade_metadata(CephContext* cct) {
  int db_version = get_version(cct, storage);
  lsubdout(cct, rgw, 10) << "db user version: " << db_version << dendl;

  if (db_version == 0) {
    // must have just been created, set version!
    storage.pragma.user_version(SFS_METADATA_VERSION);
  } else if (db_version < SFS_METADATA_VERSION && db_version >= SFS_METADATA_MIN_VERSION) {
    // perform schema update
    upgrade_metadata(cct, storage, sqlite_db);
  } else if (db_version < SFS_METADATA_MIN_VERSION) {
    throw sqlite_sync_exception(
        "Existing metadata too far behind! Unable to upgrade schema!"
    );
  } else if (db_version > SFS_METADATA_VERSION) {
    // we won't be able to read a database in the future.
    throw sqlite_sync_exception(
        "Existing metadata too far ahead! Please upgrade!"
    );
  }
}

}  // namespace rgw::sal::sfs::sqlite
