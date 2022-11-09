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
#ifndef RGW_STORE_SIMPLEFILE_OBJECT_H
#define RGW_STORE_SIMPLEFILE_OBJECT_H

#include "rgw_sal.h"

namespace rgw::sal {

class SimpleFileStore;

class SimpleFileObject : public StoreObject {
 private:
  SimpleFileStore *store;
  RGWAccessControlPolicy acls;
 protected:
  SimpleFileObject(SimpleFileObject&) = default;
  void init() {
    load_meta();
  }

 public:

  struct Meta {
    size_t size;
    std::string etag;
    ceph::real_time mtime;
    ceph::real_time set_mtime;
    ceph::real_time delete_at;
    std::map<std::string, bufferlist> attrs;

    Meta() { }
    ~Meta() = default;

    void dump(Formatter *f) const {
      encode_json("size", size, f);
      encode_json("etag", etag, f);
      encode_json("mtime", mtime, f);
      encode_json("set_mtime", set_mtime, f);
      encode_json("delete_at", delete_at, f);
      encode_json("attrs", attrs, f);
    }

    void decode_json(JSONObj *obj) {
      JSONDecoder::decode_json("size", size, obj);
      JSONDecoder::decode_json("etag", etag, obj);
      JSONDecoder::decode_json("mtime", mtime, obj);
      JSONDecoder::decode_json("set_mtime", set_mtime, obj);
      JSONDecoder::decode_json("delete_at", delete_at, obj);
      JSONDecoder::decode_json("attrs", attrs, obj);
    }
  };
  SimpleFileObject::Meta meta;

  /**
   * reads an object's contents.
   */
  struct SimpleFileReadOp : public ReadOp {
   private:
    SimpleFileObject *source;
    RGWObjectCtx *rctx;

   public:
    SimpleFileReadOp(SimpleFileObject *_source, RGWObjectCtx *_rctx);

    virtual int prepare(optional_yield y,
                        const DoutPrefixProvider *dpp) override;
    virtual int read(int64_t ofs, int64_t end, bufferlist &bl, optional_yield y,
                     const DoutPrefixProvider *dpp) override;
    virtual int iterate(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end,
                        RGWGetDataCB *cb, optional_yield y) override;
    virtual int get_attr(const DoutPrefixProvider *dpp, const char *name,
                         bufferlist &dest, optional_yield y) override;

    const std::string get_cls_name() { return "object_read"; }
  };

  /**
   * deletes an object.
   */
  struct SimpleFileDeleteOp : public DeleteOp {
   private:
    SimpleFileObject *source;

   public:
    SimpleFileDeleteOp(SimpleFileObject *_source);
    virtual int delete_obj(const DoutPrefixProvider *dpp,
                           optional_yield y) override;
  };
  SimpleFileObject& operator=(const SimpleFileObject&) = delete;

  SimpleFileObject(SimpleFileStore *_st, const rgw_obj_key &_k)
      : StoreObject(_k), store(_st) {}
  SimpleFileObject(
    SimpleFileStore *_st,
    const rgw_obj_key &_k,
    Bucket *_b
  ) : StoreObject(_k, _b), store(_st) {
    init();
  }

  virtual std::unique_ptr<Object> clone() override {
    return std::unique_ptr<Object>(new SimpleFileObject{*this});
  }

  virtual int delete_object(const DoutPrefixProvider *dpp, optional_yield y,
                            bool prevent_versioning = false) override;
  virtual int delete_obj_aio(const DoutPrefixProvider *dpp, RGWObjState *astate,
                             Completions *aio, bool keep_index_consistent,
                             optional_yield y) override;
  virtual int copy_object(
      User *user, req_info *info, const rgw_zone_id &source_zone,
      rgw::sal::Object *dest_object, rgw::sal::Bucket *dest_bucket,
      rgw::sal::Bucket *src_bucket, const rgw_placement_rule &dest_placement,
      ceph::real_time *src_mtime, ceph::real_time *mtime,
      const ceph::real_time *mod_ptr, const ceph::real_time *unmod_ptr,
      bool high_precision_time, const char *if_match, const char *if_nomatch,
      AttrsMod attrs_mod, bool copy_if_newer, Attrs &attrs,
      RGWObjCategory category, uint64_t olh_epoch,
      boost::optional<ceph::real_time> delete_at, std::string *version_id,
      std::string *tag, std::string *etag, void (*progress_cb)(off_t, void *),
      void *progress_data, const DoutPrefixProvider *dpp,
      optional_yield y) override;

  virtual RGWAccessControlPolicy &get_acl(void) override { return acls; }
  virtual int set_acl(const RGWAccessControlPolicy &acl) override {
    acls = acl;
    return 0;
  }

  virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider *dpp,
                            rgw_obj *target_obj = NULL) override;
  virtual int modify_obj_attrs(const char *attr_name, bufferlist &attr_val,
                               optional_yield y,
                               const DoutPrefixProvider *dpp) override;
  virtual int delete_obj_attrs(const DoutPrefixProvider *dpp,
                               const char *attr_name,
                               optional_yield y) override;
  virtual bool is_expired() override { return false; }
  virtual void gen_rand_obj_instance_name() override;
  virtual std::unique_ptr<MPSerializer> get_serializer(
    const DoutPrefixProvider *dpp, const std::string &lock_name
  ) override;

  virtual int transition(Bucket *bucket,
                         const rgw_placement_rule &placement_rule,
                         const real_time &mtime, uint64_t olh_epoch,
                         const DoutPrefixProvider *dpp,
                         optional_yield y) override;
  /** Move an object to the cloud */
  virtual int transition_to_cloud(
    Bucket* bucket,
    rgw::sal::PlacementTier* tier,
    rgw_bucket_dir_entry& o,
    std::set<std::string>& cloud_targets,
    CephContext* cct,
    bool update_object,
    const DoutPrefixProvider* dpp,
    optional_yield y
  );
  
  virtual bool placement_rules_match(rgw_placement_rule &r1,
                                     rgw_placement_rule &r2) override;
  virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y,
                              Formatter *f) override;
  virtual int swift_versioning_restore(bool &restored, /* out */
                                       const DoutPrefixProvider *dpp) override;
  virtual int swift_versioning_copy(const DoutPrefixProvider *dpp,
                                    optional_yield y) override;
  
  /**
   * Obtain a Read Operation.
   */
  virtual std::unique_ptr<ReadOp> get_read_op() override {
    return std::make_unique<SimpleFileObject::SimpleFileReadOp>(this, nullptr);
  }
  /**
   * Obtain a Delete Operation.
   */
  virtual std::unique_ptr<DeleteOp> get_delete_op() override {
    return std::make_unique<SimpleFileObject::SimpleFileDeleteOp>(this);
  }

  virtual int omap_get_vals(const DoutPrefixProvider *dpp,
                            const std::string &marker, uint64_t count,
                            std::map<std::string, bufferlist> *m, bool *pmore,
                            optional_yield y) override;
  virtual int omap_get_all(const DoutPrefixProvider *dpp,
                           std::map<std::string, bufferlist> *m,
                           optional_yield y) override;
  virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
                                    const std::string &oid,
                                    const std::set<std::string> &keys,
                                    Attrs *vals) override;
  virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp,
                                  const std::string &key, bufferlist &val,
                                  bool must_exist, optional_yield y) override;
  // will be removed in the future..
  virtual int get_obj_state(
    const DoutPrefixProvider *dpp,
    RGWObjState **_state,
    optional_yield y,
    bool follow_olh = true
  ) override {
    *_state = &state;
    return 0;
  }
  virtual int set_obj_attrs(const DoutPrefixProvider *dpp, Attrs *setattrs,
                            Attrs *delattrs, optional_yield y) override {
                              
    return 0;
  }

  const std::string get_cls_name() { return "object"; }
  void write_meta();
  void load_meta();
  void refresh_meta() {
    load_meta();
  }
};

} // ns rgw::sal

#endif // RGW_STORE_SIMPLEFILE_OBJECT_H
