// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t; origami-fold-style: triple-braces -*-
// vim: ts=8 sw=2 smarttab ft=cpp
#include "rgw_sal_simplefile.h"

#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

#include <sstream>
#include <system_error>

#include "cls/rgw/cls_rgw_client.h"
#include "common/Clock.h"
#include "common/errno.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"
#include "rgw_bucket.h"
#include "rgw_lc.h"
#include "rgw_multi.h"
#include "rgw_rest_conn.h"
#include "rgw_sal.h"
#include "rgw_service.h"
#include "rgw_tracer.h"
#include "rgw_zone.h"
#include "services/svc_config_key.h"
#include "services/svc_quota.h"
#include "services/svc_sys_obj.h"
#include "services/svc_tier_rados.h"
#include "services/svc_zone.h"
#include "services/svc_zone_utils.h"

#include "store/simplefile/bucket_mgr.h"
#include "store/simplefile/notification.h"
#include "store/simplefile/writer.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

// Store > User {{{

std::unique_ptr<User> SimpleFileStore::get_user(const rgw_user &u) {
  return std::make_unique<SimpleFileUser>(u, this);
}
int SimpleFileStore::get_user_by_access_key(const DoutPrefixProvider *dpp,
                                            const std::string &key,
                                            optional_yield y,
                                            std::unique_ptr<User> *user) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO (returning dummy user)" << dendl;
  user->reset(new SimpleFileUser(dummy_user, this));
  return 0;
}

int SimpleFileStore::get_user_by_email(const DoutPrefixProvider *dpp,
                                       const std::string &email,
                                       optional_yield y,
                                       std::unique_ptr<User> *user) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  user->reset(new SimpleFileUser(dummy_user, this));
  return 0;
}

int SimpleFileStore::get_user_by_swift(const DoutPrefixProvider *dpp,
                                       const std::string &user_str,
                                       optional_yield y,
                                       std::unique_ptr<User> *user) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

// }}}

// Lifecycle {{{
std::unique_ptr<Lifecycle> SimpleFileStore::get_lifecycle(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}
RGWLC *SimpleFileStore::get_rgwlc(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}

// }}}

// Store > Completions {{{
std::unique_ptr<Completions> SimpleFileStore::get_completions(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}
// }}}

// Store > Notifications {{{
std::unique_ptr<Notification> SimpleFileStore::get_notification(
  rgw::sal::Object *obj,
  rgw::sal::Object *src_obj,
  struct req_state *s,
  rgw::notify::EventType event_type,
  const std::string *object_name
) {
  ldout(ctx(), 10) << __func__ << ": return stub notification" << dendl;
  return std::make_unique<SimpleFileNotification>(obj, src_obj, event_type);
}

std::unique_ptr<Notification> SimpleFileStore::get_notification(
  const DoutPrefixProvider *dpp,
  rgw::sal::Object *obj,
  rgw::sal::Object *src_obj,
  rgw::notify::EventType event_type,
  rgw::sal::Bucket *_bucket,
  std::string &_user_id,
  std::string &_user_tenant,
  std::string &_req_id,
  optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": return stub notification" << dendl;
  return std::make_unique<SimpleFileNotification>(obj, src_obj, event_type);
}

// }}}

// Store > Writer {{{
std::unique_ptr<Writer> SimpleFileStore::get_append_writer(
    const DoutPrefixProvider *dpp, optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj, const rgw_user &owner,
    const rgw_placement_rule *ptail_placement_rule,
    const std::string &unique_tag, uint64_t position,
    uint64_t *cur_accounted_size) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return nullptr;
}
/** Get a Writer that atomically writes an entire object */
std::unique_ptr<Writer> SimpleFileStore::get_atomic_writer(
    const DoutPrefixProvider *dpp,
    optional_yield y,
    std::unique_ptr<rgw::sal::Object> _head_obj,
    const rgw_user &owner,
    const rgw_placement_rule *ptail_placement_rule,
    uint64_t olh_epoch,
    const std::string &unique_tag
) {
  ldpp_dout(dpp, 10) << __func__ << ": return basic atomic writer" << dendl;
  std::string bucketname = _head_obj->get_bucket()->get_name();
  ceph_assert(buckets_map.count(bucketname) > 0);
  auto mgr = buckets_map[bucketname];
  return std::make_unique<SimpleFileAtomicWriter>(
      dpp, y, std::move(_head_obj), this, mgr,
      owner, ptail_placement_rule,
      olh_epoch, unique_tag
  );
}

// }}}

// Store: Boring Methods {{{
std::unique_ptr<RGWOIDCProvider> SimpleFileStore::get_oidc_provider() {
  RGWOIDCProvider *p = nullptr;
  return std::unique_ptr<RGWOIDCProvider>(p);
}

int SimpleFileStore::forward_request_to_master(const DoutPrefixProvider *dpp,
                                               User *user, obj_version *objv,
                                               bufferlist &in_data,
                                               JSONParser *jp, req_info &info,
                                               optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

int SimpleFileStore::forward_iam_request_to_master(
  const DoutPrefixProvider* dpp,
  const RGWAccessKey& key,
  obj_version* objv,
  bufferlist& in_data,
  RGWXMLDecoder::XMLParser* parser,
  req_info& info,
  optional_yield y
) {
  ldpp_dout(dpp, 10) << __func__ << ": not implemented" << dendl;
  return -ENOTSUP;
}

std::string SimpleFileStore::zone_unique_id(uint64_t unique_num) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}
std::string SimpleFileStore::zone_unique_trans_id(const uint64_t unique_num) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

int SimpleFileStore::get_zonegroup(
  const std::string & id, std::unique_ptr<ZoneGroup> * zonegroup
) {
  return -ENOTSUP;
}

int SimpleFileStore::list_all_zones(
  const DoutPrefixProvider *dpp, std::list<std::string> & zone_ids
) {
  return -ENOTSUP;
}

int SimpleFileStore::cluster_stat(RGWClusterStat &stats) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return -ENOTSUP;
}

void SimpleFileStore::wakeup_meta_sync_shards(std::set<int> &shard_ids) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

void SimpleFileStore::wakeup_data_sync_shards(
  const DoutPrefixProvider *dpp,
  const rgw_zone_id &source_zone,
  boost::container::flat_map<
    int,
    boost::container::flat_set<rgw_data_notify_entry>
  > &shard_ids
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return;
}

int SimpleFileStore::register_to_service_map(const DoutPrefixProvider *dpp,
                                             const string &daemon_type,
                                             const map<string, string> &meta) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

void SimpleFileStore::get_ratelimit(RGWRateLimitInfo &bucket_ratelimit,
                                    RGWRateLimitInfo &user_ratelimit,
                                    RGWRateLimitInfo &anon_ratelimit) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

void SimpleFileStore::get_quota(RGWQuota& quota) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

int SimpleFileStore::get_sync_policy_handler(
    const DoutPrefixProvider *dpp, std::optional<rgw_zone_id> zone,
    std::optional<rgw_bucket> bucket, RGWBucketSyncPolicyHandlerRef *phandler,
    optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

RGWDataSyncStatusManager *SimpleFileStore::get_data_sync_manager(
    const rgw_zone_id &source_zone) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::read_all_usage(
    const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
    uint32_t max_entries, bool *is_truncated, RGWUsageIter &usage_iter,
    map<rgw_user_bucket, rgw_usage_log_entry> &usage) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::trim_all_usage(const DoutPrefixProvider *dpp,
                                    uint64_t start_epoch, uint64_t end_epoch) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::get_config_key_val(string name, bufferlist *bl) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::meta_list_keys_init(const DoutPrefixProvider *dpp,
                                         const string &section,
                                         const string &marker, void **phandle) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::meta_list_keys_next(const DoutPrefixProvider *dpp,
                                         void *handle, int max,
                                         list<string> &keys, bool *truncated) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

void SimpleFileStore::meta_list_keys_complete(void *handle) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

std::string SimpleFileStore::meta_get_marker(void *handle) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

int SimpleFileStore::meta_remove(const DoutPrefixProvider *dpp,
                                 string &metadata_key, optional_yield y) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

const RGWSyncModuleInstanceRef &SimpleFileStore::get_sync_module() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return sync_module;
}

std::string SimpleFileStore::get_host_id() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return "";
}

std::unique_ptr<LuaManager> SimpleFileStore::get_lua_manager() {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return std::make_unique<UnsupportedLuaManager>();
}

std::unique_ptr<RGWRole> SimpleFileStore::get_role(
  std::string name,
  std::string tenant,
  std::string path,
  std::string trust_policy,
  std::string max_session_duration_str,
  std::multimap<std::string, std::string> tags
) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  RGWRole *p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> SimpleFileStore::get_role(std::string id) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  RGWRole *p = nullptr;
  return std::unique_ptr<RGWRole>(p);
}

std::unique_ptr<RGWRole> SimpleFileStore::get_role(const RGWRoleInfo& info) {
  ldout(ctx(), 10) << __func__ << ": not implemented" << dendl;
  return std::unique_ptr<RGWRole>(nullptr);
}

int SimpleFileStore::get_roles(
  const DoutPrefixProvider *dpp, optional_yield y,
  const std::string &path_prefix,
  const std::string &tenant,
  vector<std::unique_ptr<RGWRole>> &roles
) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

// }}}

void SimpleFileStore::register_admin_apis(RGWRESTMgr * mgr) {

};

// Store > Logging {{{
int SimpleFileStore::log_usage(
    const DoutPrefixProvider *dpp,
    map<rgw_user_bucket, RGWUsageBatch> &usage_info) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

int SimpleFileStore::log_op(const DoutPrefixProvider *dpp, string &oid,
                            bufferlist &bl) {
  ldpp_dout(dpp, 10) << __func__ << ": TODO" << dendl;
  return 0;
}

// }}}

// Initialization {{{

int SimpleFileStore::initialize(
  CephContext* cct,
  const DoutPrefixProvider* dpp
) {
  ldpp_dout(dpp, 10) << __func__ << dendl;
  return 0;
}

void SimpleFileStore::finalize(void) {
  ldout(ctx(), 10) << __func__ << ": TODO" << dendl;
  return;
}

void SimpleFileStore::maybe_init_store() {

  auto meta = meta_path();
  if (std::filesystem::exists(meta)) {
    // store must have been inited, so let's just return.
    return;
  }
  ldout(ctx(), 10) << __func__ << ": creating store layout." << dendl;
  // init store.
  try {
    std::filesystem::create_directories(meta);
    std::filesystem::create_directories(buckets_path());
    std::filesystem::create_directories(users_path());
  } catch (std::filesystem::filesystem_error &e) {
    lderr(ctx()) << __func__ << ": creating store layout: "
                 << e.what() << dendl;
    throw e;
  }

}

void SimpleFileStore::init_buckets() {
  ldout(cctx, 10) << "init buckets" << dendl;
  auto p = buckets_path();
  for (auto const &entry : std::filesystem::directory_iterator{p}) {
    auto name = entry.path().filename().string();
    auto mgr = get_bucket_mgr(name);
    ldout(cctx, 10) << "init buckets > bucket: " << name << ", objects: "
                    << mgr->size() << dendl;
  }
}

SimpleFileStore::SimpleFileStore(
  CephContext *c,
  const std::filesystem::path &data_path
) : dummy_user(),
    sync_module(),
    zone(this),
    data_path(data_path),
    cctx(c) {

  maybe_init_store();
  init_buckets();

  dummy_user.user_email = "simplefile@example.com";
  dummy_user.display_name = "Test User";
  dummy_user.max_buckets = 42;
  dummy_user.admin = 1;

  dummy_user.access_keys.insert({"test", RGWAccessKey("test", "test")});
  ldout(ctx(), 0) << "Simplefile store serving data from " << data_path
                  << dendl;
}

SimpleFileStore::~SimpleFileStore() { }

}  // namespace rgw::sal

extern "C" {
void *newSimpleFileStore(CephContext *cct) {
  rgw::sal::SimpleFileStore *store = new rgw::sal::SimpleFileStore(cct, "/tmp");
  return store;
}
}

// }}}
