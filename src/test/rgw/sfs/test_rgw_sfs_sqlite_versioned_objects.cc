// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/store/sfs/sqlite/sqlite_versioned_objects.h"
#include "rgw/store/sfs/sqlite/sqlite_objects.h"
#include "rgw/store/sfs/sqlite/sqlite_buckets.h"
#include "rgw/store/sfs/sqlite/sqlite_users.h"
#include "rgw/store/sfs/sqlite/versioned_object/versioned_object_conversions.h"

#include "rgw/rgw_sal_sfs.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

using namespace rgw::sal::sfs::sqlite;

namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_sfs_tests";

const static std::string TEST_USERNAME = "test_username";
const static std::string TEST_BUCKET = "test_bucket";
const static std::string TEST_OBJECT_NAME = "test_object";
const static std::string TEST_OBJECT_ID = "80943a6d-9f72-4001-bac0-a9a036be8c49";
const static std::string TEST_OBJECT_ID_1 = "9f06d9d3-307f-4c98-865b-cd3b087acc4f";
const static std::string TEST_OBJECT_ID_2 = "af06d9d3-307f-4c98-865b-cd3b087acc4f";
const static std::string TEST_OBJECT_ID_3 = "bf06d9d3-307f-4c98-865b-cd3b087acc4f";

class TestSFSSQLiteVersionedObjects : public ::testing::Test {
protected:
  void SetUp() override {
    fs::current_path(fs::temp_directory_path());
    fs::create_directory(TEST_DIR);
  }

  void TearDown() override {
    fs::current_path(fs::temp_directory_path());
    fs::remove_all(TEST_DIR);
  }

  std::string getTestDir() const {
    auto test_dir = fs::temp_directory_path() / TEST_DIR;
    return test_dir.string();
  }

  fs::path getDBFullPath(const std::string & base_dir) const {
    auto db_full_name = "s3gw.db";
    auto db_full_path = fs::path(base_dir) /  db_full_name;
    return db_full_path;
  }

  fs::path getDBFullPath() const {
    return getDBFullPath(getTestDir());
  }

  void createUser(const std::string & username, CephContext *context) {
    SQLiteUsers users(context);
    DBOPUserInfo user;
    user.uinfo.user_id.id = username;
    users.store_user(user);
  }

  void createBucket(const std::string & username, const std::string & bucketname, CephContext *context) {
    createUser(username, context);
    SQLiteBuckets buckets(context);
    DBOPBucketInfo bucket;
    bucket.binfo.bucket.name = bucketname;
    bucket.binfo.owner.id = username;
    buckets.store_bucket(bucket);
  }

  void createObject(const std::string & username, const std::string & bucketname, const std::string object_id, CephContext *context) {
    createBucket(username, bucketname, context);
    SQLiteObjects objects(context);

    DBOPObjectInfo object;
    object.uuid.parse(object_id.c_str());
    object.bucket_name = bucketname;
    object.name = "test_name";
    object.size = rand();
    object.etag = "test_etag";
    object.mtime = ceph::real_clock::now();
    object.set_mtime = ceph::real_clock::now();
    object.delete_at = ceph::real_clock::now();
    bufferlist buffer;
    std::string blob = "blob_test";
    buffer.append(reinterpret_cast<const char *>(blob.c_str()), blob.length());
    object.attrs["attr1"] = buffer;
    RGWAccessControlPolicy policy(context);
    rgw_user user;
    user.id = username;
    std::string name = "DEFAULT";
    policy.create_default(user, name);
    object.acls = policy;
    objects.store_object(object);
  }
};

DBOPVersionedObjectInfo createTestVersionedObject(uint id, const std::string & object_id, const std::string & suffix) {
  DBOPVersionedObjectInfo test_versioned_object;
  test_versioned_object.id = id;
  uuid_d uuid;
  uuid.parse(object_id.c_str());
  test_versioned_object.object_id = uuid;
  test_versioned_object.checksum = "test_checksum_" + suffix;
  test_versioned_object.deletion_time = ceph::real_clock::now();
  test_versioned_object.size = rand();
  test_versioned_object.creation_time = ceph::real_clock::now();
  test_versioned_object.object_state = rgw::sal::ObjectState::OPEN;
  return test_versioned_object;
}

void compareVersionedObjects(const DBOPVersionedObjectInfo & origin, const DBOPVersionedObjectInfo & dest) {
  ASSERT_EQ(origin.id, dest.id);
  ASSERT_EQ(origin.object_id, dest.object_id);
  ASSERT_EQ(origin.checksum, dest.checksum);
  ASSERT_EQ(origin.deletion_time, dest.deletion_time);
  ASSERT_EQ(origin.size, dest.size);
  ASSERT_EQ(origin.creation_time, dest.creation_time);
  ASSERT_EQ(origin.object_state, dest.object_state);
}

TEST_F(TestSFSSQLiteVersionedObjects, CreateAndGet) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  auto versioned_object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");

  db_versioned_objects->store_versioned_object(versioned_object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_ver_object = db_versioned_objects->get_versioned_object(versioned_object.id);
  ASSERT_TRUE(ret_ver_object.has_value());
  compareVersionedObjects(versioned_object, *ret_ver_object);
}

TEST_F(TestSFSSQLiteVersionedObjects, ListObjectsIDs) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());

  auto obj1 = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->store_versioned_object(obj1);
  auto obj2 = createTestVersionedObject(2, TEST_OBJECT_ID, "2");
  db_versioned_objects->store_versioned_object(obj2);
  auto obj3 = createTestVersionedObject(3, TEST_OBJECT_ID, "3");
  db_versioned_objects->store_versioned_object(obj3);

  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto object_ids = db_versioned_objects->get_versioned_object_ids();
  EXPECT_EQ(object_ids.size(), 3);

  EXPECT_EQ(1, object_ids[0]);
  EXPECT_EQ(2, object_ids[1]);
  EXPECT_EQ(3, object_ids[2]);
}

TEST_F(TestSFSSQLiteVersionedObjects, ListBucketsIDsPerObject) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));

  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_1, ceph_context.get());
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_2, ceph_context.get());
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID_3, ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());

  auto test_object_1 = createTestVersionedObject(1, TEST_OBJECT_ID_1, "1");
  db_versioned_objects->store_versioned_object(test_object_1);

  auto test_object_2 = createTestVersionedObject(2, TEST_OBJECT_ID_2, "2");
  db_versioned_objects->store_versioned_object(test_object_2);

  auto test_object_3 = createTestVersionedObject(3, TEST_OBJECT_ID_3, "3");
  db_versioned_objects->store_versioned_object(test_object_3);

  uuid_d uuid1;
  uuid1.parse(TEST_OBJECT_ID_1.c_str());
  auto objects_ids = db_versioned_objects->get_versioned_object_ids(uuid1);
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_1.id);

  uuid_d uuid2;
  uuid2.parse(TEST_OBJECT_ID_2.c_str());
  objects_ids = db_versioned_objects->get_versioned_object_ids(uuid2);
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_2.id);

  uuid_d uuid3;
  uuid3.parse(TEST_OBJECT_ID_3.c_str());
  objects_ids = db_versioned_objects->get_versioned_object_ids(uuid3);
  ASSERT_EQ(objects_ids.size(), 1);
  EXPECT_EQ(objects_ids[0], test_object_3.id);
}

TEST_F(TestSFSSQLiteVersionedObjects, RemoveObject) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());

  auto obj1 = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->store_versioned_object(obj1);
  auto obj2 = createTestVersionedObject(2, TEST_OBJECT_ID, "2");
  db_versioned_objects->store_versioned_object(obj2);
  auto obj3 = createTestVersionedObject(3, TEST_OBJECT_ID, "3");
  db_versioned_objects->store_versioned_object(obj3);

  db_versioned_objects->remove_versioned_object(obj2.id);
  auto object_ids = db_versioned_objects->get_versioned_object_ids();
  EXPECT_EQ(object_ids.size(), 2);

  EXPECT_EQ(1, object_ids[0]);
  EXPECT_EQ(3, object_ids[1]);

  auto ret_object = db_versioned_objects->get_versioned_object(obj2.id);
  ASSERT_FALSE(ret_object.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, RemoveObjectThatDoesNotExist) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());

  auto obj1 = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->store_versioned_object(obj1);
  auto obj2 = createTestVersionedObject(2, TEST_OBJECT_ID, "2");
  db_versioned_objects->store_versioned_object(obj2);
  auto obj3 = createTestVersionedObject(3, TEST_OBJECT_ID, "3");
  db_versioned_objects->store_versioned_object(obj3);

  db_versioned_objects->remove_versioned_object(10);
  auto object_ids = db_versioned_objects->get_versioned_object_ids();
  EXPECT_EQ(object_ids.size(), 3);

  EXPECT_EQ(1, object_ids[0]);
  EXPECT_EQ(2, object_ids[1]);
  EXPECT_EQ(2, object_ids[1]);

  auto ret_object = db_versioned_objects->get_versioned_object(obj2.id);
  ASSERT_TRUE(ret_object.has_value());
}

TEST_F(TestSFSSQLiteVersionedObjects, CreateAndUpdate) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));
  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  auto versioned_object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");

  db_versioned_objects->store_versioned_object(versioned_object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_ver_object = db_versioned_objects->get_versioned_object(versioned_object.id);
  ASSERT_TRUE(ret_ver_object.has_value());
  compareVersionedObjects(versioned_object, *ret_ver_object);

  // update the size
  auto original_size = versioned_object.size;
  versioned_object.size = 1999;
  //versioned_object.id = 2;
  db_versioned_objects->store_versioned_object(versioned_object);

  // get the first version
  ret_ver_object = db_versioned_objects->get_versioned_object(versioned_object.id);
  ASSERT_TRUE(ret_ver_object.has_value());
  ASSERT_EQ(original_size, ret_ver_object->size);
  versioned_object.size = original_size;
  compareVersionedObjects(versioned_object, *ret_ver_object);

  // check that there are 2 versions now
  uuid_d uuid_object;
  uuid_object.parse(TEST_OBJECT_ID.c_str());
  auto ids = db_versioned_objects->get_versioned_object_ids(uuid_object);
  ASSERT_EQ(2, ids.size());
  EXPECT_EQ(1, ids[0]);
  EXPECT_EQ(2, ids[1]);

  // get the new version
  ret_ver_object = db_versioned_objects->get_versioned_object(2);
  ASSERT_TRUE(ret_ver_object.has_value());
  ASSERT_EQ(1999, ret_ver_object->size);
  versioned_object.id = 2;
  versioned_object.size = 1999;
  compareVersionedObjects(versioned_object, *ret_ver_object);
}

TEST_F(TestSFSSQLiteVersionedObjects, GetExisting) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->store_versioned_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_versioned_objects->get_versioned_object(object.id);
  ASSERT_TRUE(ret_object.has_value());
  compareVersionedObjects(object, *ret_object);

  // create a new instance, bucket should exist
  auto db_objects_2 = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());
  ret_object = db_objects_2->get_versioned_object(object.id);
  ASSERT_TRUE(ret_object.has_value());
  compareVersionedObjects(object, *ret_object);
}

TEST_F(TestSFSSQLiteVersionedObjects, CreateObjectForNonExistingBucket) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  SQLiteVersionedObjects db_objects(ceph_context.get());
  auto storage = db_objects.get_storage();

  DBVersionedObject db_object;

  db_object.id = 1;
  db_object.object_id = "254ddc1a-06a6-11ed-b939-0242ac120002";
  db_object.checksum = "test";
  db_object.size = rand();

  EXPECT_THROW({
    try {
        storage.replace(db_object);;
    } catch( const std::system_error & e ) {
        EXPECT_STREQ( "FOREIGN KEY constraint failed: constraint failed", e.what() );
        throw;
    }
  }, std::system_error );
}

TEST_F(TestSFSSQLiteVersionedObjects, Testobject_stateConversion) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  SQLiteVersionedObjects db_objects(ceph_context.get());
  auto storage = db_objects.get_storage();

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  auto db_object = get_db_versioned_object(object);
  ASSERT_EQ(0, db_object.object_state);

  db_object.object_state = 1;
  storage.replace(db_object);

  auto ret_object = db_objects.get_versioned_object(db_object.id);
  ASSERT_TRUE(ret_object.has_value());
  ASSERT_EQ(rgw::sal::ObjectState::WRITING, ret_object->object_state);

  db_object.object_state = 2;
  storage.replace(db_object);

  ret_object = db_objects.get_versioned_object(db_object.id);
  ASSERT_TRUE(ret_object.has_value());
  ASSERT_EQ(rgw::sal::ObjectState::COMMITTED, ret_object->object_state);

  db_object.object_state = 3;
  storage.replace(db_object);

  ret_object = db_objects.get_versioned_object(db_object.id);
  ASSERT_TRUE(ret_object.has_value());
  ASSERT_EQ(rgw::sal::ObjectState::LOCKED, ret_object->object_state);

  db_object.object_state = 4;
  storage.replace(db_object);

  ret_object = db_objects.get_versioned_object(db_object.id);
  ASSERT_TRUE(ret_object.has_value());
  ASSERT_EQ(rgw::sal::ObjectState::DELETED, ret_object->object_state);
}

TEST_F(TestSFSSQLiteVersionedObjects, Testobject_stateBadValue) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  SQLiteVersionedObjects db_objects(ceph_context.get());
  auto storage = db_objects.get_storage();

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  auto db_object = get_db_versioned_object(object);
  ASSERT_EQ(0, db_object.object_state);

  db_object.object_state = 10;
  storage.replace(db_object);

  EXPECT_THROW({
    try {
        auto ret_object = db_objects.get_versioned_object(db_object.id);
    } catch( const std::system_error & e ) {
        EXPECT_STREQ( "incorrect state found (10)", e.what() );
        throw;
    }
  }, std::runtime_error );
}

TEST_F(TestSFSSQLiteVersionedObjects, StoreCreatesNewVersions) {
  auto ceph_context = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
  ceph_context->_conf.set_val("rgw_sfs_data_path", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath()));

  auto db_versioned_objects = std::make_shared<SQLiteVersionedObjects>(ceph_context.get());

  // Create the object, we need it because of foreign key constrains
  createObject(TEST_USERNAME, TEST_BUCKET, TEST_OBJECT_ID, ceph_context.get());

  auto object = createTestVersionedObject(1, TEST_OBJECT_ID, "1");
  db_versioned_objects->store_versioned_object(object);
  EXPECT_TRUE(fs::exists(getDBFullPath()));

  auto ret_object = db_versioned_objects->get_versioned_object(object.id);
  ASSERT_TRUE(ret_object.has_value());
  compareVersionedObjects(object, *ret_object);

  // just update the size
  auto original_size = object.size;
  object.size = 1;
  db_versioned_objects->store_versioned_object(object);

  // change nothing, but it should also create a new version
  db_versioned_objects->store_versioned_object(object);
  auto ids = db_versioned_objects->get_versioned_object_ids();
  ASSERT_EQ(3, ids.size());
  EXPECT_EQ(1, ids[0]);
  EXPECT_EQ(2, ids[1]);
  EXPECT_EQ(3, ids[2]);

  ret_object = db_versioned_objects->get_versioned_object(1);
  ASSERT_TRUE(ret_object.has_value());
  object.size = original_size;
  object.id = 1;
  compareVersionedObjects(object, *ret_object);

  ret_object = db_versioned_objects->get_versioned_object(2);
  ASSERT_TRUE(ret_object.has_value());
  object.size = 1;
  object.id = 2;
  compareVersionedObjects(object, *ret_object);

  ret_object = db_versioned_objects->get_versioned_object(3);
  ASSERT_TRUE(ret_object.has_value());
  object.size = 1;
  object.id = 3;
  compareVersionedObjects(object, *ret_object);
}
