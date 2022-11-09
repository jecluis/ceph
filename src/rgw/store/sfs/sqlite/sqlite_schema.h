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

#include "sqlite_orm.h"
#include "users/users_definitions.h"
#include "buckets/bucket_definitions.h"
#include "objects/object_definitions.h"
#include "versioned_object/versioned_object_definitions.h"

namespace rgw::sal::sfs::sqlite  {

constexpr std::string_view SCHEMA_DB_NAME = "s3gw.db";

constexpr std::string_view USERS_TABLE = "users";
constexpr std::string_view BUCKETS_TABLE = "buckets";
constexpr std::string_view OBJECTS_TABLE = "objects";
constexpr std::string_view VERSIONED_OBJECTS_TABLE = "versioned_objects";

class SQLiteSchema {
 public:
  explicit SQLiteSchema(CephContext *cct);
  virtual ~SQLiteSchema() = default;

  SQLiteSchema( const SQLiteSchema& ) = delete;
  SQLiteSchema& operator=( const SQLiteSchema& ) = delete;

  inline auto get_storage() const {
    // Creates the sqlite_orm storage.
    // make_storage defines the tables and its columns and the mapping between the object that represents a row in a table.
    // it's basically a C++ declaration of the database.
    // In this case the object represented in the database is DBUser.
    // All types are deducted by sqlite_orm, which makes all the queries strongly typed.
    return sqlite_orm::make_storage(getDBPath(),
                    sqlite_orm::make_table(std::string(USERS_TABLE),
                          sqlite_orm::make_column("user_id", &DBUser::user_id, sqlite_orm::primary_key()),
                          sqlite_orm::make_column("tenant", &DBUser::tenant),
                          sqlite_orm::make_column("ns", &DBUser::ns),
                          sqlite_orm::make_column("display_name", &DBUser::display_name),
                          sqlite_orm::make_column("user_email", &DBUser::user_email),
                          sqlite_orm::make_column("access_keys_id", &DBUser::access_keys_id),
                          sqlite_orm::make_column("access_keys_secret", &DBUser::access_keys_secret),
                          sqlite_orm::make_column("access_keys", &DBUser::access_keys),
                          sqlite_orm::make_column("swift_keys", &DBUser::swift_keys),
                          sqlite_orm::make_column("sub_users", &DBUser::sub_users),
                          sqlite_orm::make_column("suspended", &DBUser::suspended),
                          sqlite_orm::make_column("max_buckets", &DBUser::max_buckets),
                          sqlite_orm::make_column("op_mask", &DBUser::op_mask),
                          sqlite_orm::make_column("user_caps", &DBUser::user_caps),
                          sqlite_orm::make_column("admin", &DBUser::admin),
                          sqlite_orm::make_column("system", &DBUser::system),
                          sqlite_orm::make_column("placement_name", &DBUser::placement_name),
                          sqlite_orm::make_column("placement_storage_class", &DBUser::placement_storage_class),
                          sqlite_orm::make_column("placement_tags", &DBUser::placement_tags),
                          sqlite_orm::make_column("bucke_quota", &DBUser::bucke_quota),
                          sqlite_orm::make_column("temp_url_keys", &DBUser::temp_url_keys),
                          sqlite_orm::make_column("user_quota", &DBUser::user_quota),
                          sqlite_orm::make_column("type", &DBUser::type),
                          sqlite_orm::make_column("mfa_ids", &DBUser::mfa_ids),
                          sqlite_orm::make_column("assumed_role_arn", &DBUser::assumed_role_arn),
                          sqlite_orm::make_column("user_attrs", &DBUser::user_attrs),
                          sqlite_orm::make_column("user_version", &DBUser::user_version),
                          sqlite_orm::make_column("user_version_tag", &DBUser::user_version_tag)),
                    sqlite_orm::make_table(std::string(BUCKETS_TABLE),
                          sqlite_orm::make_column("bucket_name", &DBBucket::bucket_name, sqlite_orm::primary_key()),
                          sqlite_orm::make_column("tenant", &DBBucket::tenant),
                          sqlite_orm::make_column("marker", &DBBucket::marker),
                          sqlite_orm::make_column("bucket_id", &DBBucket::bucket_id),
                          sqlite_orm::make_column("owner_id", &DBBucket::owner_id),
                          sqlite_orm::make_column("creation_time", &DBBucket::creation_time),
                          sqlite_orm::make_column("placement_name", &DBBucket::placement_name),
                          sqlite_orm::make_column("placement_storage_class", &DBBucket::placement_storage_class),
                          sqlite_orm::foreign_key(&DBBucket::owner_id).references(&DBUser::user_id)),
                    sqlite_orm::make_table(std::string(OBJECTS_TABLE),
                          sqlite_orm::make_column("object_id", &DBObject::object_id, sqlite_orm::primary_key()),
                          sqlite_orm::make_column("bucket_name", &DBObject::bucket_name),
                          sqlite_orm::make_column("name", &DBObject::name),
                          sqlite_orm::make_column("size", &DBObject::size),
                          sqlite_orm::make_column("etag", &DBObject::etag),
                          sqlite_orm::make_column("mtime", &DBObject::mtime),
                          sqlite_orm::make_column("set_mtime", &DBObject::set_mtime),
                          sqlite_orm::make_column("delete_at_time", &DBObject::delete_at_time),
                          sqlite_orm::make_column("attrs", &DBObject::attrs),
                          sqlite_orm::make_column("acls", &DBObject::acls),
                          sqlite_orm::foreign_key(&DBObject::bucket_name).references(&DBBucket::bucket_name)),
                    sqlite_orm::make_table(std::string(VERSIONED_OBJECTS_TABLE),
                          sqlite_orm::make_column("id", &DBVersionedObject::id, sqlite_orm::autoincrement(), sqlite_orm::primary_key()),
                          sqlite_orm::make_column("object_id", &DBVersionedObject::object_id),
                          sqlite_orm::make_column("checksum", &DBVersionedObject::checksum),
                          sqlite_orm::make_column("deletion_time", &DBVersionedObject::deletion_time),
                          sqlite_orm::make_column("size", &DBVersionedObject::size),
                          sqlite_orm::make_column("creation_time", &DBVersionedObject::creation_time),
                          sqlite_orm::make_column("object_state", &DBVersionedObject::object_state),
                          sqlite_orm::foreign_key(&DBVersionedObject::object_id).references(&DBObject::object_id))
        );
    }

 protected:
    std::string getDBPath() const;
    void sync() const;

    CephContext *ceph_context_ = nullptr;
};

}  // namespace rgw::sal::sfs::sqlite
