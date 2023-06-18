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

// example function to upgrade db
//
// static void upgrade_metadata_from_v1(sqlite3* db) {
//   sqlite3_exec(db, "ALTER TABLE foo ...", nullptr, nullptr, nullptr);
// }

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

    // example check to upgrade db
    // if (cur_version == 1) {
    //   upgrade_metadata_from_v1(db);
    // }

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
