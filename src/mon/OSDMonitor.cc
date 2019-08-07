// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <experimental/iterator>
#include <locale>
#include <sstream>

#include "mon/OSDMonitor.h"
#include "mon/Monitor.h"
#include "mon/MDSMonitor.h"
#include "mon/MgrStatMonitor.h"
#include "mon/AuthMonitor.h"
#include "mon/ConfigKeyService.h"

#include "mon/MonitorDBStore.h"
#include "mon/Session.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"
#include "crush/CrushTreeDumper.h"

#include "messages/MOSDBeacon.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDMarkMeDead.h"
#include "messages/MOSDFull.h"
#include "messages/MOSDMap.h"
#include "messages/MMonGetOSDMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGCreate2.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MOSDPGReadyToMerge.h"
#include "messages/MMonCommand.h"
#include "messages/MRemoveSnaps.h"
#include "messages/MOSDScrub.h"
#include "messages/MRoute.h"
#include "messages/MMonGetPurgedSnaps.h"
#include "messages/MMonGetPurgedSnapsReply.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "common/strtol.h"
#include "common/numa.h" // maybe remove?

#include "common/config.h"
#include "common/errno.h"

#include "erasure-code/ErasureCodePlugin.h"
#include "compressor/Compressor.h"
#include "common/Checksummer.h"

#include "include/compat.h"
#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "include/util.h"
#include "common/cmdparse.h"
#include "include/str_list.h"
#include "include/str_map.h"
#include "include/scope_guard.h"

#include "auth/cephx/CephxKeyServer.h"
#include "osd/OSDCap.h"

#include "json_spirit/json_spirit_reader.h"

#include <boost/algorithm/string/predicate.hpp>

#define dout_subsys ceph_subsys_mon
static const string OSD_PG_CREATING_PREFIX("osd_pg_creating");
static const string OSD_METADATA_PREFIX("osd_metadata");
static const string OSD_SNAP_PREFIX("osd_snap");

/*

  OSD snapshot metadata
  ---------------------

  -- starting with mimic --

  "removed_epoch_%llu_%08lx" % (pool, epoch)
   -> interval_set<snapid_t>

  "removed_snap_%llu_%016llx" % (pool, last_snap)
   -> { first_snap, end_snap, epoch }   (last_snap = end_snap - 1)

  "purged_snap_%llu_%016llx" % (pool, last_snap)
   -> { first_snap, end_snap, epoch }   (last_snap = end_snap - 1)

  - note that the {removed,purged}_snap put the last snap in they key so
    that we can use forward iteration only to search for an epoch in an
    interval.  e.g., to test if epoch N is removed/purged, we'll find a key
    >= N that either does or doesn't contain the given snap.


  -- starting with octopus --

  "purged_epoch_%08lx" % epoch
  -> map<int64_t,interval_set<snapid_t>>

  */

void LastEpochClean::Lec::report(ps_t ps, epoch_t last_epoch_clean)
{
  if (epoch_by_pg.size() <= ps) {
    epoch_by_pg.resize(ps + 1, 0);
  }
  const auto old_lec = epoch_by_pg[ps];
  if (old_lec >= last_epoch_clean) {
    // stale lec
    return;
  }
  epoch_by_pg[ps] = last_epoch_clean;
  if (last_epoch_clean < floor) {
    floor = last_epoch_clean;
  } else if (last_epoch_clean > floor) {
    if (old_lec == floor) {
      // probably should increase floor?
      auto new_floor = std::min_element(std::begin(epoch_by_pg),
					std::end(epoch_by_pg));
      floor = *new_floor;
    }
  }
  if (ps != next_missing) {
    return;
  }
  for (; next_missing < epoch_by_pg.size(); next_missing++) {
    if (epoch_by_pg[next_missing] == 0) {
      break;
    }
  }
}

void LastEpochClean::remove_pool(uint64_t pool)
{
  report_by_pool.erase(pool);
}

void LastEpochClean::report(const pg_t& pg, epoch_t last_epoch_clean)
{
  auto& lec = report_by_pool[pg.pool()];
  return lec.report(pg.ps(), last_epoch_clean);
}

epoch_t LastEpochClean::get_lower_bound(const OSDMap& latest) const
{
  auto floor = latest.get_epoch();
  for (auto& pool : latest.get_pools()) {
    auto reported = report_by_pool.find(pool.first);
    if (reported == report_by_pool.end()) {
      return 0;
    }
    if (reported->second.next_missing < pool.second.get_pg_num()) {
      return 0;
    }
    if (reported->second.floor < floor) {
      floor = reported->second.floor;
    }
  }
  return floor;
}


class C_UpdateCreatingPGs : public Context {
public:
  OSDMonitor *osdmon;
  utime_t start;
  epoch_t epoch;
  C_UpdateCreatingPGs(OSDMonitor *osdmon, epoch_t e) :
    osdmon(osdmon), start(ceph_clock_now()), epoch(e) {}
  void finish(int r) override {
    if (r >= 0) {
      utime_t end = ceph_clock_now();
      dout(10) << "osdmap epoch " << epoch << " mapping took "
	       << (end - start) << " seconds" << dendl;
      osdmon->update_creating_pgs();
      osdmon->check_pg_creates_subs();
    }
  }
};

#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, osdmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, const OSDMap& osdmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").osd e" << osdmap.get_epoch() << " ";
}

OSDMonitor::OSDMonitor(
  CephContext *cct,
  Monitor *mn,
  Paxos *p,
  const string& service_name)
 : PaxosService(mn, p, service_name),
   cct(cct),
   inc_osd_cache(g_conf()->mon_osd_cache_size),
   full_osd_cache(g_conf()->mon_osd_cache_size),
   has_osdmap_manifest(false),
   mapper(mn->cct, &mn->cpu_tp)
{}

void OSDMonitor::create_initial()
{
  dout(10) << "create_initial for " << mon->monmap->fsid << dendl;

  OSDMap newmap;

  bufferlist bl;
  mon->store->get("mkfs", "osdmap", bl);

  if (bl.length()) {
    newmap.decode(bl);
    newmap.set_fsid(mon->monmap->fsid);
  } else {
    newmap.build_simple(cct, 0, mon->monmap->fsid, 0);
  }
  newmap.set_epoch(1);
  newmap.created = newmap.modified = ceph_clock_now();

  // new clusters should sort bitwise by default.
  newmap.set_flag(CEPH_OSDMAP_SORTBITWISE);

  newmap.flags |=
    CEPH_OSDMAP_RECOVERY_DELETES |
    CEPH_OSDMAP_PURGED_SNAPDIRS |
    CEPH_OSDMAP_PGLOG_HARDLIMIT;
  newmap.full_ratio = g_conf()->mon_osd_full_ratio;
  if (newmap.full_ratio > 1.0) newmap.full_ratio /= 100;
  newmap.backfillfull_ratio = g_conf()->mon_osd_backfillfull_ratio;
  if (newmap.backfillfull_ratio > 1.0) newmap.backfillfull_ratio /= 100;
  newmap.nearfull_ratio = g_conf()->mon_osd_nearfull_ratio;
  if (newmap.nearfull_ratio > 1.0) newmap.nearfull_ratio /= 100;

  // new cluster should require latest by default
  if (g_conf().get_val<bool>("mon_debug_no_require_octopus")) {
    if (g_conf().get_val<bool>("mon_debug_no_require_nautilus")) {
      derr << __func__ << " mon_debug_no_require_octopus and nautilus=true" << dendl;
      newmap.require_osd_release = ceph_release_t::mimic;
    } else {
      derr << __func__ << " mon_debug_no_require_octopus=true" << dendl;
      newmap.require_osd_release = ceph_release_t::nautilus;
    }
  } else {
    newmap.require_osd_release = ceph_release_t::octopus;
    ceph_release_t r = ceph_release_from_name(
      g_conf()->mon_osd_initial_require_min_compat_client);
    if (!r) {
      ceph_abort_msg("mon_osd_initial_require_min_compat_client is not valid");
    }
    newmap.require_min_compat_client = r;
  }

  // encode into pending incremental
  uint64_t features = newmap.get_encoding_features();
  newmap.encode(pending_inc.fullmap,
                features | CEPH_FEATURE_RESERVED);
  pending_inc.full_crc = newmap.get_crc();
  dout(20) << " full crc " << pending_inc.full_crc << dendl;
}

void OSDMonitor::get_store_prefixes(std::set<string>& s) const
{
  s.insert(service_name);
  s.insert(OSD_PG_CREATING_PREFIX);
  s.insert(OSD_METADATA_PREFIX);
  s.insert(OSD_SNAP_PREFIX);
}

void OSDMonitor::update_from_paxos(bool *need_bootstrap)
{
  // we really don't care if the version has been updated, because we may
  // have trimmed without having increased the last committed; yet, we may
  // need to update the in-memory manifest.
  load_osdmap_manifest();

  version_t version = get_last_committed();
  if (version == osdmap.epoch)
    return;
  ceph_assert(version > osdmap.epoch);

  dout(15) << "update_from_paxos paxos e " << version
	   << ", my e " << osdmap.epoch << dendl;

  if (mapping_job) {
    if (!mapping_job->is_done()) {
      dout(1) << __func__ << " mapping job "
	      << mapping_job.get() << " did not complete, "
	      << mapping_job->shards << " left, canceling" << dendl;
      mapping_job->abort();
    }
    mapping_job.reset();
  }

  load_health();

  /*
   * We will possibly have a stashed latest that *we* wrote, and we will
   * always be sure to have the oldest full map in the first..last range
   * due to encode_trim_extra(), which includes the oldest full map in the trim
   * transaction.
   *
   * encode_trim_extra() does not however write the full map's
   * version to 'full_latest'.  This is only done when we are building the
   * full maps from the incremental versions.  But don't panic!  We make sure
   * that the following conditions find whichever full map version is newer.
   */
  version_t latest_full = get_version_latest_full();
  if (latest_full == 0 && get_first_committed() > 1)
    latest_full = get_first_committed();

  if (get_first_committed() > 1 &&
      latest_full < get_first_committed()) {
    // the monitor could be just sync'ed with its peer, and the latest_full key
    // is not encoded in the paxos commits in encode_pending(), so we need to
    // make sure we get it pointing to a proper version.
    version_t lc = get_last_committed();
    version_t fc = get_first_committed();

    dout(10) << __func__ << " looking for valid full map in interval"
	     << " [" << fc << ", " << lc << "]" << dendl;

    latest_full = 0;
    for (version_t v = lc; v >= fc; v--) {
      string full_key = "full_" + stringify(v);
      if (mon->store->exists(get_service_name(), full_key)) {
        dout(10) << __func__ << " found latest full map v " << v << dendl;
        latest_full = v;
        break;
      }
    }

    ceph_assert(latest_full > 0);
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    put_version_latest_full(t, latest_full);
    mon->store->apply_transaction(t);
    dout(10) << __func__ << " updated the on-disk full map version to "
             << latest_full << dendl;
  }

  if ((latest_full > 0) && (latest_full > osdmap.epoch)) {
    bufferlist latest_bl;
    get_version_full(latest_full, latest_bl);
    ceph_assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading latest full map e" << latest_full << dendl;
    osdmap = OSDMap();
    osdmap.decode(latest_bl);
  }

  bufferlist bl;
  if (!mon->store->get(OSD_PG_CREATING_PREFIX, "creating", bl)) {
    auto p = bl.cbegin();
    std::lock_guard<std::mutex> l(creating_pgs_lock);
    creating_pgs.decode(p);
    dout(7) << __func__ << " loading creating_pgs last_scan_epoch "
	    << creating_pgs.last_scan_epoch
	    << " with " << creating_pgs.pgs.size() << " pgs" << dendl;
  } else {
    dout(1) << __func__ << " missing creating pgs; upgrade from post-kraken?"
	    << dendl;
  }

  // walk through incrementals
  MonitorDBStore::TransactionRef t;
  size_t tx_size = 0;
  while (version > osdmap.epoch) {
    bufferlist inc_bl;
    int err = get_version(osdmap.epoch+1, inc_bl);
    ceph_assert(err == 0);
    ceph_assert(inc_bl.length());

    dout(7) << "update_from_paxos  applying incremental " << osdmap.epoch+1
	    << dendl;
    OSDMap::Incremental inc(inc_bl);
    err = osdmap.apply_incremental(inc);
    ceph_assert(err == 0);

    if (!t)
      t.reset(new MonitorDBStore::Transaction);

    // Write out the full map for all past epochs.  Encode the full
    // map with the same features as the incremental.  If we don't
    // know, use the quorum features.  If we don't know those either,
    // encode with all features.
    uint64_t f = inc.encode_features;
    if (!f)
      f = mon->get_quorum_con_features();
    if (!f)
      f = -1;
    bufferlist full_bl;
    osdmap.encode(full_bl, f | CEPH_FEATURE_RESERVED);
    tx_size += full_bl.length();

    bufferlist orig_full_bl;
    get_version_full(osdmap.epoch, orig_full_bl);
    if (orig_full_bl.length()) {
      // the primary provided the full map
      ceph_assert(inc.have_crc);
      if (inc.full_crc != osdmap.crc) {
	// This will happen if the mons were running mixed versions in
	// the past or some other circumstance made the full encoded
	// maps divergent.  Reloading here will bring us back into
	// sync with the primary for this and all future maps.  OSDs
	// will also be brought back into sync when they discover the
	// crc mismatch and request a full map from a mon.
	derr << __func__ << " full map CRC mismatch, resetting to canonical"
	     << dendl;

	dout(20) << __func__ << " my (bad) full osdmap:\n";
	JSONFormatter jf(true);
	jf.dump_object("osdmap", osdmap);
	jf.flush(*_dout);
	*_dout << "\nhexdump:\n";
	full_bl.hexdump(*_dout);
	*_dout << dendl;

	osdmap = OSDMap();
	osdmap.decode(orig_full_bl);

	dout(20) << __func__ << " canonical full osdmap:\n";
	JSONFormatter jf(true);
	jf.dump_object("osdmap", osdmap);
	jf.flush(*_dout);
	*_dout << "\nhexdump:\n";
	orig_full_bl.hexdump(*_dout);
	*_dout << dendl;
      }
    } else {
      ceph_assert(!inc.have_crc);
      put_version_full(t, osdmap.epoch, full_bl);
    }
    put_version_latest_full(t, osdmap.epoch);

    // share
    dout(1) << osdmap << dendl;

    if (osdmap.epoch == 1) {
      t->erase("mkfs", "osdmap");
    }

    if (tx_size > g_conf()->mon_sync_max_payload_size*2) {
      mon->store->apply_transaction(t);
      t = MonitorDBStore::TransactionRef();
      tx_size = 0;
    }
    for (const auto &osd_state : inc.new_state) {
      if (osd_state.second & CEPH_OSD_UP) {
	// could be marked up *or* down, but we're too lazy to check which
	last_osd_report.erase(osd_state.first);
      }
      if (osd_state.second & CEPH_OSD_EXISTS) {
	// could be created *or* destroyed, but we can safely drop it
	osd_epochs.erase(osd_state.first);
      }
    }
  }

  if (t) {
    mon->store->apply_transaction(t);
  }

  for (int o = 0; o < osdmap.get_max_osd(); o++) {
    if (osdmap.is_out(o))
      continue;
    auto found = down_pending_out.find(o);
    if (osdmap.is_down(o)) {
      // populate down -> out map
      if (found == down_pending_out.end()) {
        dout(10) << " adding osd." << o << " to down_pending_out map" << dendl;
        down_pending_out[o] = ceph_clock_now();
      }
    } else {
      if (found != down_pending_out.end()) {
        dout(10) << " removing osd." << o << " from down_pending_out map" << dendl;
        down_pending_out.erase(found);
      }
    }
  }
  // XXX: need to trim MonSession connected with a osd whose id > max_osd?

  check_osdmap_subs();
  check_pg_creates_subs();

  share_map_with_random_osd();
  update_logger();

  process_failures();

  // make sure our feature bits reflect the latest map
  update_msgr_features();

  if (!mon->is_leader()) {
    // will be called by on_active() on the leader, avoid doing so twice
    start_mapping();
  }
}

void OSDMonitor::start_mapping()
{
  // initiate mapping job
  if (mapping_job) {
    dout(10) << __func__ << " canceling previous mapping_job " << mapping_job.get()
	     << dendl;
    mapping_job->abort();
  }
  if (!osdmap.get_pools().empty()) {
    auto fin = new C_UpdateCreatingPGs(this, osdmap.get_epoch());
    mapping_job = mapping.start_update(osdmap, mapper,
				       g_conf()->mon_osd_mapping_pgs_per_chunk);
    dout(10) << __func__ << " started mapping job " << mapping_job.get()
	     << " at " << fin->start << dendl;
    mapping_job->set_finish_event(fin);
  } else {
    dout(10) << __func__ << " no pools, no mapping job" << dendl;
    mapping_job = nullptr;
  }
}

void OSDMonitor::update_msgr_features()
{
  set<int> types;
  types.insert((int)entity_name_t::TYPE_OSD);
  types.insert((int)entity_name_t::TYPE_CLIENT);
  types.insert((int)entity_name_t::TYPE_MDS);
  types.insert((int)entity_name_t::TYPE_MON);
  for (set<int>::iterator q = types.begin(); q != types.end(); ++q) {
    uint64_t mask;
    uint64_t features = osdmap.get_features(*q, &mask);
    if ((mon->messenger->get_policy(*q).features_required & mask) != features) {
      dout(0) << "crush map has features " << features << ", adjusting msgr requires" << dendl;
      ceph::net::Policy p = mon->messenger->get_policy(*q);
      p.features_required = (p.features_required & ~mask) | features;
      mon->messenger->set_policy(*q, p);
    }
  }
}

void OSDMonitor::on_active()
{
  update_logger();

  if (mon->is_leader()) {
    mon->clog->debug() << "osdmap " << osdmap;
    if (!priority_convert) {
      // Only do this once at start-up
      convert_pool_priorities();
      priority_convert = true;
    }
  } else {
    list<MonOpRequestRef> ls;
    take_all_failures(ls);
    while (!ls.empty()) {
      MonOpRequestRef op = ls.front();
      op->mark_osdmon_event(__func__);
      dispatch(op);
      ls.pop_front();
    }
  }
  start_mapping();
}

void OSDMonitor::on_restart()
{
  last_osd_report.clear();
}

void OSDMonitor::on_shutdown()
{
  dout(10) << __func__ << dendl;
  if (mapping_job) {
    dout(10) << __func__ << " canceling previous mapping_job " << mapping_job.get()
	     << dendl;
    mapping_job->abort();
  }

  // discard failure info, waiters
  list<MonOpRequestRef> ls;
  take_all_failures(ls);
  ls.clear();
}

void OSDMonitor::update_logger()
{
  dout(10) << "update_logger" << dendl;

  mon->cluster_logger->set(l_cluster_num_osd, osdmap.get_num_osds());
  mon->cluster_logger->set(l_cluster_num_osd_up, osdmap.get_num_up_osds());
  mon->cluster_logger->set(l_cluster_num_osd_in, osdmap.get_num_in_osds());
  mon->cluster_logger->set(l_cluster_osd_epoch, osdmap.get_epoch());
}

void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap.epoch+1);
  pending_inc.fsid = mon->monmap->fsid;
  pending_metadata.clear();
  pending_metadata_rm.clear();
  pending_pseudo_purged_snaps.clear();

  dout(10) << "create_pending e " << pending_inc.epoch << dendl;

  // safety checks (this shouldn't really happen)
  {
    if (osdmap.backfillfull_ratio <= 0) {
      pending_inc.new_backfillfull_ratio = g_conf()->mon_osd_backfillfull_ratio;
      if (pending_inc.new_backfillfull_ratio > 1.0)
	pending_inc.new_backfillfull_ratio /= 100;
      dout(1) << __func__ << " setting backfillfull_ratio = "
	      << pending_inc.new_backfillfull_ratio << dendl;
    }
    if (osdmap.full_ratio <= 0) {
      pending_inc.new_full_ratio = g_conf()->mon_osd_full_ratio;
      if (pending_inc.new_full_ratio > 1.0)
        pending_inc.new_full_ratio /= 100;
      dout(1) << __func__ << " setting full_ratio = "
	      << pending_inc.new_full_ratio << dendl;
    }
    if (osdmap.nearfull_ratio <= 0) {
      pending_inc.new_nearfull_ratio = g_conf()->mon_osd_nearfull_ratio;
      if (pending_inc.new_nearfull_ratio > 1.0)
        pending_inc.new_nearfull_ratio /= 100;
      dout(1) << __func__ << " setting nearfull_ratio = "
	      << pending_inc.new_nearfull_ratio << dendl;
    }
  }

  // Rewrite CRUSH rule IDs if they are using legacy "ruleset"
  // structure.
  if (osdmap.crush->has_legacy_rule_ids()) {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    // First, for all pools, work out which rule they really used
    // by resolving ruleset to rule.
    for (const auto &i : osdmap.get_pools()) {
      const auto pool_id = i.first;
      const auto &pool = i.second;
      int new_rule_id = newcrush.find_rule(pool.crush_rule,
					   pool.type, pool.size);

      dout(1) << __func__ << " rewriting pool "
	      << osdmap.get_pool_name(pool_id) << " crush ruleset "
	      << pool.crush_rule << " -> rule id " << new_rule_id << dendl;
      if (pending_inc.new_pools.count(pool_id) == 0) {
	pending_inc.new_pools[pool_id] = pool;
      }
      pending_inc.new_pools[pool_id].crush_rule = new_rule_id;
    }

    // Now, go ahead and renumber all the rules so that their
    // rule_id field corresponds to their position in the array
    auto old_to_new = newcrush.renumber_rules();
    dout(1) << __func__ << " Rewrote " << old_to_new << " crush IDs:" << dendl;
    for (const auto &i : old_to_new) {
      dout(1) << __func__ << " " << i.first << " -> " << i.second << dendl;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
  }
}

creating_pgs_t
OSDMonitor::update_pending_pgs(const OSDMap::Incremental& inc,
			       const OSDMap& nextmap)
{
  dout(10) << __func__ << dendl;
  creating_pgs_t pending_creatings;
  {
    std::lock_guard<std::mutex> l(creating_pgs_lock);
    pending_creatings = creating_pgs;
  }
  // check for new or old pools
  if (pending_creatings.last_scan_epoch < inc.epoch) {
    unsigned queued = 0;
    queued += scan_for_creating_pgs(osdmap.get_pools(),
				    inc.old_pools,
				    inc.modified,
				    &pending_creatings);
    queued += scan_for_creating_pgs(inc.new_pools,
				    inc.old_pools,
				    inc.modified,
				    &pending_creatings);
    dout(10) << __func__ << " " << queued << " pools queued" << dendl;
    for (auto deleted_pool : inc.old_pools) {
      auto removed = pending_creatings.remove_pool(deleted_pool);
      dout(10) << __func__ << " " << removed
               << " pg removed because containing pool deleted: "
               << deleted_pool << dendl;
      last_epoch_clean.remove_pool(deleted_pool);
    }
    // pgmon updates its creating_pgs in check_osd_map() which is called by
    // on_active() and check_osd_map() could be delayed if lease expires, so its
    // creating_pgs could be stale in comparison with the one of osdmon. let's
    // trim them here. otherwise, they will be added back after being erased.
    unsigned removed = 0;
    for (auto& pg : pending_created_pgs) {
      dout(20) << __func__ << " noting created pg " << pg << dendl;
      pending_creatings.created_pools.insert(pg.pool());
      removed += pending_creatings.pgs.erase(pg);
    }
    pending_created_pgs.clear();
    dout(10) << __func__ << " " << removed
	     << " pgs removed because they're created" << dendl;
    pending_creatings.last_scan_epoch = osdmap.get_epoch();
  }

  // filter out any pgs that shouldn't exist.
  {
    auto i = pending_creatings.pgs.begin();
    while (i != pending_creatings.pgs.end()) {
      if (!nextmap.pg_exists(i->first)) {
	dout(10) << __func__ << " removing pg " << i->first
		 << " which should not exist" << dendl;
	i = pending_creatings.pgs.erase(i);
      } else {
	++i;
      }
    }
  }

  // process queue
  unsigned max = std::max<int64_t>(1, g_conf()->mon_osd_max_creating_pgs);
  const auto total = pending_creatings.pgs.size();
  while (pending_creatings.pgs.size() < max &&
	 !pending_creatings.queue.empty()) {
    auto p = pending_creatings.queue.begin();
    int64_t poolid = p->first;
    dout(10) << __func__ << " pool " << poolid
	     << " created " << p->second.created
	     << " modified " << p->second.modified
	     << " [" << p->second.start << "-" << p->second.end << ")"
	     << dendl;
    int64_t n = std::min<int64_t>(max - pending_creatings.pgs.size(),
				  p->second.end - p->second.start);
    ps_t first = p->second.start;
    ps_t end = first + n;
    for (ps_t ps = first; ps < end; ++ps) {
      const pg_t pgid{ps, static_cast<uint64_t>(poolid)};
      // NOTE: use the *current* epoch as the PG creation epoch so that the
      // OSD does not have to generate a long set of PastIntervals.
      pending_creatings.pgs.emplace(
	pgid,
	creating_pgs_t::pg_create_info(inc.epoch,
				       p->second.modified));
      dout(10) << __func__ << " adding " << pgid << dendl;
    }
    p->second.start = end;
    if (p->second.done()) {
      dout(10) << __func__ << " done with queue for " << poolid << dendl;
      pending_creatings.queue.erase(p);
    } else {
      dout(10) << __func__ << " pool " << poolid
	       << " now [" << p->second.start << "-" << p->second.end << ")"
	       << dendl;
    }
  }
  dout(10) << __func__ << " queue remaining: " << pending_creatings.queue.size()
	   << " pools" << dendl;

  if (mon->monmap->min_mon_release >= ceph_release_t::octopus) {
    // walk creating pgs' history and past_intervals forward
    for (auto& i : pending_creatings.pgs) {
      // this mirrors PG::start_peering_interval()
      pg_t pgid = i.first;

      // this is a bit imprecise, but sufficient?
      struct min_size_predicate_t : public IsPGRecoverablePredicate {
	const pg_pool_t *pi;
	bool operator()(const set<pg_shard_t> &have) const {
	  return have.size() >= pi->min_size;
	}
	explicit min_size_predicate_t(const pg_pool_t *i) : pi(i) {}
      } min_size_predicate(nextmap.get_pg_pool(pgid.pool()));

      vector<int> up, acting;
      int up_primary, acting_primary;
      nextmap.pg_to_up_acting_osds(
	pgid, &up, &up_primary, &acting, &acting_primary);
      if (i.second.history.epoch_created == 0) {
	// new pg entry, set it up
	i.second.up = up;
	i.second.acting = acting;
	i.second.up_primary = up_primary;
	i.second.acting_primary = acting_primary;
	i.second.history = pg_history_t(i.second.create_epoch,
					i.second.create_stamp);
	dout(10) << __func__ << "  pg " << pgid << " just added, "
		 << " up " << i.second.up
		 << " p " << i.second.up_primary
		 << " acting " << i.second.acting
		 << " p " << i.second.acting_primary
		 << " history " << i.second.history
		 << " past_intervals " << i.second.past_intervals
		 << dendl;
     } else {
	std::stringstream debug;
	if (PastIntervals::check_new_interval(
	      i.second.acting_primary, acting_primary,
	      i.second.acting, acting,
	      i.second.up_primary, up_primary,
	      i.second.up, up,
	      i.second.history.same_interval_since,
	      i.second.history.last_epoch_clean,
	      &nextmap,
	      &osdmap,
	      pgid,
	      min_size_predicate,
	      &i.second.past_intervals,
	      &debug)) {
	  epoch_t e = inc.epoch;
	  i.second.history.same_interval_since = e;
	  if (i.second.up != up) {
	    i.second.history.same_up_since = e;
	  }
	  if (i.second.acting_primary != acting_primary) {
	    i.second.history.same_primary_since = e;
	  }
	  if (pgid.is_split(
		osdmap.get_pg_num(pgid.pool()),
		nextmap.get_pg_num(pgid.pool()),
		nullptr)) {
	    i.second.history.last_epoch_split = e;
	  }
	  dout(10) << __func__ << "  pg " << pgid << " new interval,"
		   << " up " << i.second.up << " -> " << up
		   << " p " << i.second.up_primary << " -> " << up_primary
		   << " acting " << i.second.acting << " -> " << acting
		   << " p " << i.second.acting_primary << " -> "
		   << acting_primary
		   << " history " << i.second.history
		   << " past_intervals " << i.second.past_intervals
		   << dendl;
	  dout(20) << "  debug: " << debug.str() << dendl;
	  i.second.up = up;
	  i.second.acting = acting;
	  i.second.up_primary = up_primary;
	  i.second.acting_primary = acting_primary;
	}
      }
    }
  }
  dout(10) << __func__
	   << " " << (pending_creatings.pgs.size() - total)
	   << "/" << pending_creatings.pgs.size()
	   << " pgs added from queued pools" << dendl;
  return pending_creatings;
}

void OSDMonitor::maybe_prime_pg_temp()
{
  bool all = false;
  if (pending_inc.crush.length()) {
    dout(10) << __func__ << " new crush map, all" << dendl;
    all = true;
  }

  if (!pending_inc.new_up_client.empty()) {
    dout(10) << __func__ << " new up osds, all" << dendl;
    all = true;
  }

  // check for interesting OSDs
  set<int> osds;
  for (auto p = pending_inc.new_state.begin();
       !all && p != pending_inc.new_state.end();
       ++p) {
    if ((p->second & CEPH_OSD_UP) &&
	osdmap.is_up(p->first)) {
      osds.insert(p->first);
    }
  }
  for (map<int32_t,uint32_t>::iterator p = pending_inc.new_weight.begin();
       !all && p != pending_inc.new_weight.end();
       ++p) {
    if (p->second < osdmap.get_weight(p->first)) {
      // weight reduction
      osds.insert(p->first);
    } else {
      dout(10) << __func__ << " osd." << p->first << " weight increase, all"
	       << dendl;
      all = true;
    }
  }

  if (!all && osds.empty())
    return;

  if (!all) {
    unsigned estimate =
      mapping.get_osd_acting_pgs(*osds.begin()).size() * osds.size();
    if (estimate > mapping.get_num_pgs() *
	g_conf()->mon_osd_prime_pg_temp_max_estimate) {
      dout(10) << __func__ << " estimate " << estimate << " pgs on "
	       << osds.size() << " osds >= "
	       << g_conf()->mon_osd_prime_pg_temp_max_estimate << " of total "
	       << mapping.get_num_pgs() << " pgs, all"
	       << dendl;
      all = true;
    } else {
      dout(10) << __func__ << " estimate " << estimate << " pgs on "
	       << osds.size() << " osds" << dendl;
    }
  }

  OSDMap next;
  next.deepish_copy_from(osdmap);
  next.apply_incremental(pending_inc);

  if (next.get_pools().empty()) {
    dout(10) << __func__ << " no pools, no pg_temp priming" << dendl;
  } else if (all) {
    PrimeTempJob job(next, this);
    mapper.queue(&job, g_conf()->mon_osd_mapping_pgs_per_chunk, {});
    if (job.wait_for(g_conf()->mon_osd_prime_pg_temp_max_time)) {
      dout(10) << __func__ << " done in " << job.get_duration() << dendl;
    } else {
      dout(10) << __func__ << " did not finish in "
	       << g_conf()->mon_osd_prime_pg_temp_max_time
	       << ", stopping" << dendl;
      job.abort();
    }
  } else {
    dout(10) << __func__ << " " << osds.size() << " interesting osds" << dendl;
    utime_t stop = ceph_clock_now();
    stop += g_conf()->mon_osd_prime_pg_temp_max_time;
    const int chunk = 1000;
    int n = chunk;
    std::unordered_set<pg_t> did_pgs;
    for (auto osd : osds) {
      auto& pgs = mapping.get_osd_acting_pgs(osd);
      dout(20) << __func__ << " osd." << osd << " " << pgs << dendl;
      for (auto pgid : pgs) {
	if (!did_pgs.insert(pgid).second) {
	  continue;
	}
	prime_pg_temp(next, pgid);
	if (--n <= 0) {
	  n = chunk;
	  if (ceph_clock_now() > stop) {
	    dout(10) << __func__ << " consumed more than "
		     << g_conf()->mon_osd_prime_pg_temp_max_time
		     << " seconds, stopping"
		     << dendl;
	    return;
	  }
	}
      }
    }
  }
}

void OSDMonitor::prime_pg_temp(
  const OSDMap& next,
  pg_t pgid)
{
  // TODO: remove this creating_pgs direct access?
  if (creating_pgs.pgs.count(pgid)) {
    return;
  }
  if (!osdmap.pg_exists(pgid)) {
    return;
  }

  vector<int> up, acting;
  mapping.get(pgid, &up, nullptr, &acting, nullptr);

  vector<int> next_up, next_acting;
  int next_up_primary, next_acting_primary;
  next.pg_to_up_acting_osds(pgid, &next_up, &next_up_primary,
			    &next_acting, &next_acting_primary);
  if (acting == next_acting &&
      !(up != acting && next_up == next_acting))
    return;  // no change since last epoch

  if (acting.empty())
    return;  // if previously empty now we can be no worse off
  const pg_pool_t *pool = next.get_pg_pool(pgid.pool());
  if (pool && acting.size() < pool->min_size)
    return;  // can be no worse off than before

  if (next_up == next_acting) {
    acting.clear();
    dout(20) << __func__ << " next_up == next_acting now, clear pg_temp"
	     << dendl;
  }

  dout(20) << __func__ << " " << pgid << " " << up << "/" << acting
	   << " -> " << next_up << "/" << next_acting
	   << ", priming " << acting
	   << dendl;
  {
    std::lock_guard l(prime_pg_temp_lock);
    // do not touch a mapping if a change is pending
    pending_inc.new_pg_temp.emplace(
      pgid,
      mempool::osdmap::vector<int>(acting.begin(), acting.end()));
  }
}

/**
 * @note receiving a transaction in this function gives a fair amount of
 * freedom to the service implementation if it does need it. It shouldn't.
 */
void OSDMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << "encode_pending e " << pending_inc.epoch
	   << dendl;

  if (do_prune(t)) {
    dout(1) << __func__ << " osdmap full prune encoded e"
            << pending_inc.epoch << dendl;
  }

  // finalize up pending_inc
  pending_inc.modified = ceph_clock_now();

  int r = pending_inc.propagate_snaps_to_tiers(cct, osdmap);
  ceph_assert(r == 0);

  if (mapping_job) {
    if (!mapping_job->is_done()) {
      dout(1) << __func__ << " skipping prime_pg_temp; mapping job "
	      << mapping_job.get() << " did not complete, "
	      << mapping_job->shards << " left" << dendl;
      mapping_job->abort();
    } else if (mapping.get_epoch() < osdmap.get_epoch()) {
      dout(1) << __func__ << " skipping prime_pg_temp; mapping job "
	      << mapping_job.get() << " is prior epoch "
	      << mapping.get_epoch() << dendl;
    } else {
      if (g_conf()->mon_osd_prime_pg_temp) {
	maybe_prime_pg_temp();
      }
    } 
  } else if (g_conf()->mon_osd_prime_pg_temp) {
    dout(1) << __func__ << " skipping prime_pg_temp; mapping job did not start"
	    << dendl;
  }
  mapping_job.reset();

  // ensure we don't have blank new_state updates.  these are interrpeted as
  // CEPH_OSD_UP (and almost certainly not what we want!).
  auto p = pending_inc.new_state.begin();
  while (p != pending_inc.new_state.end()) {
    if (p->second == 0) {
      dout(10) << "new_state for osd." << p->first << " is 0, removing" << dendl;
      p = pending_inc.new_state.erase(p);
    } else {
      if (p->second & CEPH_OSD_UP) {
	pending_inc.new_last_up_change = pending_inc.modified;
      }
      ++p;
    }
  }
  if (!pending_inc.new_up_client.empty()) {
    pending_inc.new_last_up_change = pending_inc.modified;
  }
  for (auto& i : pending_inc.new_weight) {
    if (i.first >= osdmap.max_osd) {
      if (i.second) {
	// new osd is already marked in
	pending_inc.new_last_in_change = pending_inc.modified;
        break;
      }
    } else if (!!i.second != !!osdmap.osd_weight[i.first]) {
      // existing osd marked in or out
      pending_inc.new_last_in_change = pending_inc.modified;
      break;
    }
  }

  {
    OSDMap tmp;
    tmp.deepish_copy_from(osdmap);
    tmp.apply_incremental(pending_inc);

    // clean pg_temp mappings
    OSDMap::clean_temps(cct, osdmap, tmp, &pending_inc);

    // clean inappropriate pg_upmap/pg_upmap_items (if any)
    {
      // check every upmapped pg for now
      // until we could reliably identify certain cases to ignore,
      // which is obviously the hard part TBD..
      vector<pg_t> pgs_to_check;
      tmp.get_upmap_pgs(&pgs_to_check);
      if (pgs_to_check.size() <
	  static_cast<uint64_t>(g_conf()->mon_clean_pg_upmaps_per_chunk * 2)) {
        // not enough pgs, do it inline
        tmp.clean_pg_upmaps(cct, &pending_inc);
      } else {
        CleanUpmapJob job(cct, tmp, pending_inc);
        mapper.queue(&job, g_conf()->mon_clean_pg_upmaps_per_chunk, pgs_to_check);
        job.wait();
      }
    }

    // update creating pgs first so that we can remove the created pgid and
    // process the pool flag removal below in the same osdmap epoch.
    auto pending_creatings = update_pending_pgs(pending_inc, tmp);
    bufferlist creatings_bl;
    uint64_t features = CEPH_FEATURES_ALL;
    if (mon->monmap->min_mon_release < ceph_release_t::octopus) {
      dout(20) << __func__ << " encoding pending pgs without octopus features"
	       << dendl;
      features &= ~CEPH_FEATURE_SERVER_OCTOPUS;
    }
    encode(pending_creatings, creatings_bl, features);
    t->put(OSD_PG_CREATING_PREFIX, "creating", creatings_bl);

    // remove any old (or incompat) POOL_CREATING flags
    for (auto& i : tmp.get_pools()) {
      if (tmp.require_osd_release < ceph_release_t::nautilus) {
	// pre-nautilus OSDMaps shouldn't get this flag.
	if (pending_inc.new_pools.count(i.first)) {
	  pending_inc.new_pools[i.first].flags &= ~pg_pool_t::FLAG_CREATING;
	}
      }
      if (i.second.has_flag(pg_pool_t::FLAG_CREATING) &&
	  !pending_creatings.still_creating_pool(i.first)) {
	dout(10) << __func__ << " done creating pool " << i.first
		 << ", clearing CREATING flag" << dendl;
	if (pending_inc.new_pools.count(i.first) == 0) {
	  pending_inc.new_pools[i.first] = i.second;
	}
	pending_inc.new_pools[i.first].flags &= ~pg_pool_t::FLAG_CREATING;
      }
    }

    // remove any legacy osdmap nearfull/full flags
    {
      if (tmp.test_flag(CEPH_OSDMAP_FULL | CEPH_OSDMAP_NEARFULL)) {
	dout(10) << __func__ << " clearing legacy osdmap nearfull/full flag"
		 << dendl;
	remove_flag(CEPH_OSDMAP_NEARFULL);
	remove_flag(CEPH_OSDMAP_FULL);
      }
    }
    // collect which pools are currently affected by
    // the near/backfill/full osd(s),
    // and set per-pool near/backfill/full flag instead
    set<int64_t> full_pool_ids;
    set<int64_t> backfillfull_pool_ids;
    set<int64_t> nearfull_pool_ids;
    tmp.get_full_pools(cct,
		       &full_pool_ids,
		       &backfillfull_pool_ids,
                         &nearfull_pool_ids);
    if (full_pool_ids.empty() ||
	backfillfull_pool_ids.empty() ||
	nearfull_pool_ids.empty()) {
      // normal case - no nearfull, backfillfull or full osds
        // try cancel any improper nearfull/backfillfull/full pool
        // flags first
      for (auto &pool: tmp.get_pools()) {
	auto p = pool.first;
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_NEARFULL) &&
	    nearfull_pool_ids.empty()) {
	  dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		   << "'s nearfull flag" << dendl;
	  if (pending_inc.new_pools.count(p) == 0) {
	    // load original pool info first!
	    pending_inc.new_pools[p] = pool.second;
	  }
	  pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_NEARFULL;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_BACKFILLFULL) &&
	    backfillfull_pool_ids.empty()) {
	  dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		   << "'s backfillfull flag" << dendl;
	  if (pending_inc.new_pools.count(p) == 0) {
	    pending_inc.new_pools[p] = pool.second;
	  }
	  pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_BACKFILLFULL;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL) &&
	    full_pool_ids.empty()) {
	  if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	    // set by EQUOTA, skipping
	    continue;
	  }
	  dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		   << "'s full flag" << dendl;
	  if (pending_inc.new_pools.count(p) == 0) {
	    pending_inc.new_pools[p] = pool.second;
	  }
	  pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_FULL;
	}
      }
    }
    if (!full_pool_ids.empty()) {
      dout(10) << __func__ << " marking pool(s) " << full_pool_ids
	       << " as full" << dendl;
      for (auto &p: full_pool_ids) {
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL)) {
	  continue;
	}
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = tmp.pools[p];
	}
	pending_inc.new_pools[p].flags |= pg_pool_t::FLAG_FULL;
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_BACKFILLFULL;
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_NEARFULL;
      }
      // cancel FLAG_FULL for pools which are no longer full too
      for (auto &pool: tmp.get_pools()) {
	auto p = pool.first;
	if (full_pool_ids.count(p)) {
	  // skip pools we have just marked as full above
	  continue;
	}
	if (!tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL) ||
	    tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	  // don't touch if currently is not full
	  // or is running out of quota (and hence considered as full)
	  continue;
	}
	dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		 << "'s full flag" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = pool.second;
	}
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_FULL;
      }
    }
    if (!backfillfull_pool_ids.empty()) {
      for (auto &p: backfillfull_pool_ids) {
	if (full_pool_ids.count(p)) {
	  // skip pools we have already considered as full above
	  continue;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	  // make sure FLAG_FULL is truly set, so we are safe not
	  // to set a extra (redundant) FLAG_BACKFILLFULL flag
	  ceph_assert(tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL));
	  continue;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_BACKFILLFULL)) {
	  // don't bother if pool is already marked as backfillfull
	  continue;
	}
	dout(10) << __func__ << " marking pool '" << tmp.pool_name[p]
		 << "'s as backfillfull" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = tmp.pools[p];
	}
	pending_inc.new_pools[p].flags |= pg_pool_t::FLAG_BACKFILLFULL;
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_NEARFULL;
      }
      // cancel FLAG_BACKFILLFULL for pools
      // which are no longer backfillfull too
      for (auto &pool: tmp.get_pools()) {
	auto p = pool.first;
	if (full_pool_ids.count(p) || backfillfull_pool_ids.count(p)) {
	  // skip pools we have just marked as backfillfull/full above
	  continue;
	}
	if (!tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_BACKFILLFULL)) {
	  // and don't touch if currently is not backfillfull
	  continue;
	}
	dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		 << "'s backfillfull flag" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = pool.second;
	}
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_BACKFILLFULL;
      }
    }
    if (!nearfull_pool_ids.empty()) {
      for (auto &p: nearfull_pool_ids) {
	if (full_pool_ids.count(p) || backfillfull_pool_ids.count(p)) {
	  continue;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	  // make sure FLAG_FULL is truly set, so we are safe not
	  // to set a extra (redundant) FLAG_NEARFULL flag
	  ceph_assert(tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL));
	  continue;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_NEARFULL)) {
	  // don't bother if pool is already marked as nearfull
	  continue;
	}
	dout(10) << __func__ << " marking pool '" << tmp.pool_name[p]
		 << "'s as nearfull" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = tmp.pools[p];
	}
	pending_inc.new_pools[p].flags |= pg_pool_t::FLAG_NEARFULL;
      }
      // cancel FLAG_NEARFULL for pools
      // which are no longer nearfull too
      for (auto &pool: tmp.get_pools()) {
	auto p = pool.first;
	if (full_pool_ids.count(p) ||
	    backfillfull_pool_ids.count(p) ||
	    nearfull_pool_ids.count(p)) {
	  // skip pools we have just marked as
	  // nearfull/backfillfull/full above
	  continue;
	}
	if (!tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_NEARFULL)) {
	  // and don't touch if currently is not nearfull
	  continue;
	}
	dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		 << "'s nearfull flag" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = pool.second;
	}
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_NEARFULL;
      }
    }

    // min_compat_client?
    if (!tmp.require_min_compat_client) {
      auto mv = tmp.get_min_compat_client();
      dout(1) << __func__ << " setting require_min_compat_client to currently "
	      << "required " << mv << dendl;
      mon->clog->info() << "setting require_min_compat_client to currently "
			<< "required " << mv;
      pending_inc.new_require_min_compat_client = mv;
    }

    if (osdmap.require_osd_release < ceph_release_t::nautilus &&
	tmp.require_osd_release >= ceph_release_t::nautilus) {
      dout(10) << __func__ << " first nautilus+ epoch" << dendl;
      // add creating flags?
      for (auto& i : tmp.get_pools()) {
	if (pending_creatings.still_creating_pool(i.first)) {
	  dout(10) << __func__ << " adding CREATING flag to pool " << i.first
		   << dendl;
	  if (pending_inc.new_pools.count(i.first) == 0) {
	    pending_inc.new_pools[i.first] = i.second;
	  }
	  pending_inc.new_pools[i.first].flags |= pg_pool_t::FLAG_CREATING;
	}
      }
      // adjust blacklist items to all be TYPE_ANY
      for (auto& i : tmp.blacklist) {
	auto a = i.first;
	a.set_type(entity_addr_t::TYPE_ANY);
	pending_inc.new_blacklist[a] = i.second;
	pending_inc.old_blacklist.push_back(i.first);
      }
    }

    if (osdmap.require_osd_release < ceph_release_t::octopus &&
	tmp.require_osd_release >= ceph_release_t::octopus) {
      dout(10) << __func__ << " first octopus+ epoch" << dendl;

      // adjust obsoleted cache modes
      for (auto& [poolid, pi] : tmp.pools) {
	if (pi.cache_mode == pg_pool_t::CACHEMODE_FORWARD) {
	  if (pending_inc.new_pools.count(poolid) == 0) {
	    pending_inc.new_pools[poolid] = pi;
	  }
	  dout(10) << __func__ << " switching pool " << poolid
		   << " cachemode from forward -> proxy" << dendl;
	  pending_inc.new_pools[poolid].cache_mode = pg_pool_t::CACHEMODE_PROXY;
	}
	if (pi.cache_mode == pg_pool_t::CACHEMODE_READFORWARD) {
	  if (pending_inc.new_pools.count(poolid) == 0) {
	    pending_inc.new_pools[poolid] = pi;
	  }
	  dout(10) << __func__ << " switching pool " << poolid
		   << " cachemode from readforward -> readproxy" << dendl;
	  pending_inc.new_pools[poolid].cache_mode =
	    pg_pool_t::CACHEMODE_READPROXY;
	}
      }

      // clear removed_snaps for every pool
      for (auto& [poolid, pi] : tmp.pools) {
	if (pi.removed_snaps.empty()) {
	  continue;
	}
	if (pending_inc.new_pools.count(poolid) == 0) {
	  pending_inc.new_pools[poolid] = pi;
	}
	dout(10) << __func__ << " clearing pool " << poolid << " removed_snaps"
		 << dendl;
	pending_inc.new_pools[poolid].removed_snaps.clear();
      }

      // create a combined purged snap epoch key for all purged snaps
      // prior to this epoch, and store it in the current epoch (i.e.,
      // the last pre-octopus epoch, just prior to the one we're
      // encoding now).
      auto it = mon->store->get_iterator(OSD_SNAP_PREFIX);
      it->lower_bound("purged_snap_");
      map<int64_t,snap_interval_set_t> combined;
      while (it->valid()) {
	if (it->key().find("purged_snap_") != 0) {
	  break;
	}
	string k = it->key();
	long long unsigned pool;
	int n = sscanf(k.c_str(), "purged_snap_%llu_", &pool);
	if (n != 1) {
	  derr << __func__ << " invalid purged_snaps key '" << k << "'" << dendl;
	} else {
	  bufferlist v = it->value();
	  auto p = v.cbegin();
	  snapid_t begin, end;
	  ceph::decode(begin, p);
	  ceph::decode(end, p);
	  combined[pool].insert(begin, end - begin);
	}
	it->next();
      }
      if (!combined.empty()) {
	string k = make_purged_snap_epoch_key(pending_inc.epoch - 1);
	bufferlist v;
	ceph::encode(combined, v);
	t->put(OSD_SNAP_PREFIX, k, v);
	dout(10) << __func__ << " recording pre-octopus purged_snaps in epoch "
		 << (pending_inc.epoch - 1) << ", " << v.length() << " bytes"
		 << dendl;
      } else {
	dout(10) << __func__ << " there were no pre-octopus purged snaps"
		 << dendl;
      }
    }
  }

  // tell me about it
  for (auto i = pending_inc.new_state.begin();
       i != pending_inc.new_state.end();
       ++i) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if (s & CEPH_OSD_UP)
      dout(2) << " osd." << i->first << " DOWN" << dendl;
    if (s & CEPH_OSD_EXISTS)
      dout(2) << " osd." << i->first << " DNE" << dendl;
  }
  for (auto i = pending_inc.new_up_client.begin();
       i != pending_inc.new_up_client.end();
       ++i) {
    //FIXME: insert cluster addresses too
    dout(2) << " osd." << i->first << " UP " << i->second << dendl;
  }
  for (map<int32_t,uint32_t>::iterator i = pending_inc.new_weight.begin();
       i != pending_inc.new_weight.end();
       ++i) {
    if (i->second == CEPH_OSD_OUT) {
      dout(2) << " osd." << i->first << " OUT" << dendl;
    } else if (i->second == CEPH_OSD_IN) {
      dout(2) << " osd." << i->first << " IN" << dendl;
    } else {
      dout(2) << " osd." << i->first << " WEIGHT " << hex << i->second << dec << dendl;
    }
  }

  // features for osdmap and its incremental
  uint64_t features;

  // encode full map and determine its crc
  OSDMap tmp;
  {
    tmp.deepish_copy_from(osdmap);
    tmp.apply_incremental(pending_inc);

    // determine appropriate features
    features = tmp.get_encoding_features();
    dout(10) << __func__ << " encoding full map with "
	     << tmp.require_osd_release
	     << " features " << features << dendl;

    // the features should be a subset of the mon quorum's features!
    ceph_assert((features & ~mon->get_quorum_con_features()) == 0);

    bufferlist fullbl;
    encode(tmp, fullbl, features | CEPH_FEATURE_RESERVED);
    pending_inc.full_crc = tmp.get_crc();

    // include full map in the txn.  note that old monitors will
    // overwrite this.  new ones will now skip the local full map
    // encode and reload from this.
    put_version_full(t, pending_inc.epoch, fullbl);
  }

  // encode
  ceph_assert(get_last_committed() + 1 == pending_inc.epoch);
  bufferlist bl;
  encode(pending_inc, bl, features | CEPH_FEATURE_RESERVED);

  dout(20) << " full_crc " << tmp.get_crc()
	   << " inc_crc " << pending_inc.inc_crc << dendl;

  /* put everything in the transaction */
  put_version(t, pending_inc.epoch, bl);
  put_last_committed(t, pending_inc.epoch);

  // metadata, too!
  for (map<int,bufferlist>::iterator p = pending_metadata.begin();
       p != pending_metadata.end();
       ++p)
    t->put(OSD_METADATA_PREFIX, stringify(p->first), p->second);
  for (set<int>::iterator p = pending_metadata_rm.begin();
       p != pending_metadata_rm.end();
       ++p)
    t->erase(OSD_METADATA_PREFIX, stringify(*p));
  pending_metadata.clear();
  pending_metadata_rm.clear();

  // removed_snaps
  for (auto& i : pending_inc.new_removed_snaps) {
    {
      // all snaps removed this epoch
      string k = make_removed_snap_epoch_key(i.first, pending_inc.epoch);
      bufferlist v;
      encode(i.second, v);
      t->put(OSD_SNAP_PREFIX, k, v);
    }
    for (auto q = i.second.begin();
	 q != i.second.end();
	 ++q) {
      insert_snap_update(false, i.first, q.get_start(), q.get_end(),
			 pending_inc.epoch,
			 t);
    }
  }
  if (tmp.require_osd_release >= ceph_release_t::octopus &&
      !pending_inc.new_purged_snaps.empty()) {
    // all snaps purged this epoch (across all pools)
    string k = make_purged_snap_epoch_key(pending_inc.epoch);
    bufferlist v;
    encode(pending_inc.new_purged_snaps, v);
    t->put(OSD_SNAP_PREFIX, k, v);
  }
  for (auto& i : pending_inc.new_purged_snaps) {
    for (auto q = i.second.begin();
	 q != i.second.end();
	 ++q) {
      insert_snap_update(true, i.first, q.get_start(), q.get_end(),
			 pending_inc.epoch,
			 t);
    }
  }
  for (auto& [pool, snaps] : pending_pseudo_purged_snaps) {
    for (auto snap : snaps) {
      insert_snap_update(true, pool, snap, snap + 1,
			 pending_inc.epoch,
			 t);
    }
  }

  // health
  health_check_map_t next;
  tmp.check_health(&next);
  encode_health(next, t);
}

int OSDMonitor::load_metadata(int osd, map<string, string>& m, ostream *err)
{
  bufferlist bl;
  int r = mon->store->get(OSD_METADATA_PREFIX, stringify(osd), bl);
  if (r < 0)
    return r;
  try {
    auto p = bl.cbegin();
    decode(m, p);
  }
  catch (buffer::error& e) {
    if (err)
      *err << "osd." << osd << " metadata is corrupt";
    return -EIO;
  }
  return 0;
}

void OSDMonitor::count_metadata(const string& field, map<string,int> *out)
{
  for (int osd = 0; osd < osdmap.get_max_osd(); ++osd) {
    if (osdmap.is_up(osd)) {
      map<string,string> meta;
      load_metadata(osd, meta, nullptr);
      auto p = meta.find(field);
      if (p == meta.end()) {
	(*out)["unknown"]++;
      } else {
	(*out)[p->second]++;
      }
    }
  }
}

void OSDMonitor::count_metadata(const string& field, Formatter *f)
{
  map<string,int> by_val;
  count_metadata(field, &by_val);
  f->open_object_section(field.c_str());
  for (auto& p : by_val) {
    f->dump_int(p.first.c_str(), p.second);
  }
  f->close_section();
}

int OSDMonitor::get_osd_objectstore_type(int osd, string *type)
{
  map<string, string> metadata;
  int r = load_metadata(osd, metadata, nullptr);
  if (r < 0)
    return r;

  auto it = metadata.find("osd_objectstore");
  if (it == metadata.end())
    return -ENOENT;
  *type = it->second;
  return 0;
}

bool OSDMonitor::is_pool_currently_all_bluestore(int64_t pool_id,
						 const pg_pool_t &pool,
						 ostream *err)
{
  // just check a few pgs for efficiency - this can't give a guarantee anyway,
  // since filestore osds could always join the pool later
  set<int> checked_osds;
  for (unsigned ps = 0; ps < std::min(8u, pool.get_pg_num()); ++ps) {
    vector<int> up, acting;
    pg_t pgid(ps, pool_id);
    osdmap.pg_to_up_acting_osds(pgid, up, acting);
    for (int osd : up) {
      if (checked_osds.find(osd) != checked_osds.end())
	continue;
      string objectstore_type;
      int r = get_osd_objectstore_type(osd, &objectstore_type);
      // allow with missing metadata, e.g. due to an osd never booting yet
      if (r < 0 || objectstore_type == "bluestore") {
	checked_osds.insert(osd);
	continue;
      }
      *err << "osd." << osd << " uses " << objectstore_type;
      return false;
    }
  }
  return true;
}

int OSDMonitor::dump_osd_metadata(int osd, Formatter *f, ostream *err)
{
  map<string,string> m;
  if (int r = load_metadata(osd, m, err))
    return r;
  for (map<string,string>::iterator p = m.begin(); p != m.end(); ++p)
    f->dump_string(p->first.c_str(), p->second);
  return 0;
}

void OSDMonitor::print_nodes(Formatter *f)
{
  // group OSDs by their hosts
  map<string, list<int> > osds; // hostname => osd
  for (int osd = 0; osd < osdmap.get_max_osd(); osd++) {
    map<string, string> m;
    if (load_metadata(osd, m, NULL)) {
      continue;
    }
    map<string, string>::iterator hostname = m.find("hostname");
    if (hostname == m.end()) {
      // not likely though
      continue;
    }
    osds[hostname->second].push_back(osd);
  }

  dump_services(f, osds, "osd");
}

void OSDMonitor::share_map_with_random_osd()
{
  if (osdmap.get_num_up_osds() == 0) {
    dout(10) << __func__ << " no up osds, don't share with anyone" << dendl;
    return;
  }

  MonSession *s = mon->session_map.get_random_osd_session(&osdmap);
  if (!s) {
    dout(10) << __func__ << " no up osd on our session map" << dendl;
    return;
  }

  dout(10) << "committed, telling random " << s->name
	   << " all about it" << dendl;

  // get feature of the peer
  // use quorum_con_features, if it's an anonymous connection.
  uint64_t features = s->con_features ? s->con_features :
                                        mon->get_quorum_con_features();
  // whatev, they'll request more if they need it
  MOSDMap *m = build_incremental(osdmap.get_epoch() - 1, osdmap.get_epoch(), features);
  s->con->send_message(m);
  // NOTE: do *not* record osd has up to this epoch (as we do
  // elsewhere) as they may still need to request older values.
}

version_t OSDMonitor::get_trim_to() const
{
  if (mon->get_quorum().empty()) {
    dout(10) << __func__ << ": quorum not formed" << dendl;
    return 0;
  }

  {
    std::lock_guard<std::mutex> l(creating_pgs_lock);
    if (!creating_pgs.pgs.empty()) {
      return 0;
    }
  }

  if (g_conf().get_val<bool>("mon_debug_block_osdmap_trim")) {
    dout(0) << __func__
            << " blocking osdmap trim"
               " ('mon_debug_block_osdmap_trim' set to 'true')"
            << dendl;
    return 0;
  }

  {
    epoch_t floor = get_min_last_epoch_clean();
    dout(10) << " min_last_epoch_clean " << floor << dendl;
    if (g_conf()->mon_osd_force_trim_to > 0 &&
	g_conf()->mon_osd_force_trim_to < (int)get_last_committed()) {
      floor = g_conf()->mon_osd_force_trim_to;
      dout(10) << " explicit mon_osd_force_trim_to = " << floor << dendl;
    }
    unsigned min = g_conf()->mon_min_osdmap_epochs;
    if (floor + min > get_last_committed()) {
      if (min < get_last_committed())
	floor = get_last_committed() - min;
      else
	floor = 0;
    }
    if (floor > get_first_committed())
      return floor;
  }
  return 0;
}

epoch_t OSDMonitor::get_min_last_epoch_clean() const
{
  auto floor = last_epoch_clean.get_lower_bound(osdmap);
  // also scan osd epochs
  // don't trim past the oldest reported osd epoch
  for (auto& osd_epoch : osd_epochs) {
    if (osd_epoch.second < floor) {
      floor = osd_epoch.second;
    }
  }
  return floor;
}

void OSDMonitor::encode_trim_extra(MonitorDBStore::TransactionRef tx,
				   version_t first)
{
  dout(10) << __func__ << " including full map for e " << first << dendl;
  bufferlist bl;
  get_version_full(first, bl);
  put_version_full(tx, first, bl);

  if (has_osdmap_manifest &&
      first > osdmap_manifest.get_first_pinned()) {
    _prune_update_trimmed(tx, first);
  }
}


/* full osdmap prune
 *
 * for more information, please refer to doc/dev/mon-osdmap-prune.rst
 */

void OSDMonitor::load_osdmap_manifest()
{
  bool store_has_manifest =
    mon->store->exists(get_service_name(), "osdmap_manifest");

  if (!store_has_manifest) {
    if (!has_osdmap_manifest) {
      return;
    }

    dout(20) << __func__
             << " dropping osdmap manifest from memory." << dendl;
    osdmap_manifest = osdmap_manifest_t();
    has_osdmap_manifest = false;
    return;
  }

  dout(20) << __func__
           << " osdmap manifest detected in store; reload." << dendl;

  bufferlist manifest_bl;
  int r = get_value("osdmap_manifest", manifest_bl);
  if (r < 0) {
    derr << __func__ << " unable to read osdmap version manifest" << dendl;
    ceph_abort_msg("error reading manifest");
  }
  osdmap_manifest.decode(manifest_bl);
  has_osdmap_manifest = true;

  dout(10) << __func__ << " store osdmap manifest pinned ("
           << osdmap_manifest.get_first_pinned()
           << " .. "
           << osdmap_manifest.get_last_pinned()
           << ")"
           << dendl;
}

bool OSDMonitor::should_prune() const
{
  version_t first = get_first_committed();
  version_t last = get_last_committed();
  version_t min_osdmap_epochs =
    g_conf().get_val<int64_t>("mon_min_osdmap_epochs");
  version_t prune_min =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_min");
  version_t prune_interval =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_interval");
  version_t last_pinned = osdmap_manifest.get_last_pinned();
  version_t last_to_pin = last - min_osdmap_epochs;

  // Make it or break it constraints.
  //
  // If any of these conditions fails, we will not prune, regardless of
  // whether we have an on-disk manifest with an on-going pruning state.
  //
  if ((last - first) <= min_osdmap_epochs) {
    // between the first and last committed epochs, we don't have
    // enough epochs to trim, much less to prune.
    dout(10) << __func__
             << " currently holding only " << (last - first)
             << " epochs (min osdmap epochs: " << min_osdmap_epochs
             << "); do not prune."
             << dendl;
    return false;

  } else if ((last_to_pin - first) < prune_min) {
    // between the first committed epoch and the last epoch we would prune,
    // we simply don't have enough versions over the minimum to prune maps.
    dout(10) << __func__
             << " could only prune " << (last_to_pin - first)
             << " epochs (" << first << ".." << last_to_pin << "), which"
                " is less than the required minimum (" << prune_min << ")"
             << dendl;
    return false;

  } else if (has_osdmap_manifest && last_pinned >= last_to_pin) {
    dout(10) << __func__
             << " we have pruned as far as we can; do not prune."
             << dendl;
    return false;

  } else if (last_pinned + prune_interval > last_to_pin) {
    dout(10) << __func__
             << " not enough epochs to form an interval (last pinned: "
             << last_pinned << ", last to pin: "
             << last_to_pin << ", interval: " << prune_interval << ")"
             << dendl;
    return false;
  }

  dout(15) << __func__
           << " should prune (" << last_pinned << ".." << last_to_pin << ")"
           << " lc (" << first << ".." << last << ")"
           << dendl;
  return true;
}

void OSDMonitor::_prune_update_trimmed(
    MonitorDBStore::TransactionRef tx,
    version_t first)
{
  dout(10) << __func__
           << " first " << first
           << " last_pinned " << osdmap_manifest.get_last_pinned()
           << " last_pinned " << osdmap_manifest.get_last_pinned()
           << dendl;

  osdmap_manifest_t manifest = osdmap_manifest;

  if (!manifest.is_pinned(first)) {
    manifest.pin(first);
  }

  set<version_t>::iterator p_end = manifest.pinned.find(first);
  set<version_t>::iterator p = manifest.pinned.begin();
  manifest.pinned.erase(p, p_end);
  ceph_assert(manifest.get_first_pinned() == first);

  if (manifest.get_last_pinned() == first+1 ||
      manifest.pinned.size() == 1) {
    // we reached the end of the line, as pinned maps go; clean up our
    // manifest, and let `should_prune()` decide whether we should prune
    // again.
    tx->erase(get_service_name(), "osdmap_manifest");
    return;
  }

  bufferlist bl;
  manifest.encode(bl);
  tx->put(get_service_name(), "osdmap_manifest", bl);
}

void OSDMonitor::prune_init(osdmap_manifest_t& manifest)
{
  dout(1) << __func__ << dendl;

  version_t pin_first;

  // verify constrainsts on stable in-memory state
  if (!has_osdmap_manifest) {
    // we must have never pruned, OR if we pruned the state must no longer
    // be relevant (i.e., the state must have been removed alongside with
    // the trim that *must* have removed past the last pinned map in a
    // previous prune).
    ceph_assert(osdmap_manifest.pinned.empty());
    ceph_assert(!mon->store->exists(get_service_name(), "osdmap_manifest"));
    pin_first = get_first_committed();

  } else {
    // we must have pruned in the past AND its state is still relevant
    // (i.e., even if we trimmed, we still hold pinned maps in the manifest,
    // and thus we still hold a manifest in the store).
    ceph_assert(!osdmap_manifest.pinned.empty());
    ceph_assert(osdmap_manifest.get_first_pinned() == get_first_committed());
    ceph_assert(osdmap_manifest.get_last_pinned() < get_last_committed());

    dout(10) << __func__
             << " first_pinned " << osdmap_manifest.get_first_pinned()
             << " last_pinned " << osdmap_manifest.get_last_pinned()
             << dendl;

    pin_first = osdmap_manifest.get_last_pinned();
  }

  manifest.pin(pin_first);
}

bool OSDMonitor::_prune_sanitize_options() const
{
  uint64_t prune_interval =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_interval");
  uint64_t prune_min =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_min");
  uint64_t txsize =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_txsize");

  bool r = true;

  if (prune_interval == 0) {
    derr << __func__
         << " prune is enabled BUT prune interval is zero; abort."
         << dendl;
    r = false;
  } else if (prune_interval == 1) {
    derr << __func__
         << " prune interval is equal to one, which essentially means"
            " no pruning; abort."
         << dendl;
    r = false;
  }
  if (prune_min == 0) {
    derr << __func__
         << " prune is enabled BUT prune min is zero; abort."
         << dendl;
    r = false;
  }
  if (prune_interval > prune_min) {
    derr << __func__
         << " impossible to ascertain proper prune interval because"
         << " it is greater than the minimum prune epochs"
         << " (min: " << prune_min << ", interval: " << prune_interval << ")"
         << dendl;
    r = false;
  }

  if (txsize < prune_interval - 1) {
    derr << __func__
         << "'mon_osdmap_full_prune_txsize' (" << txsize
         << ") < 'mon_osdmap_full_prune_interval-1' (" << prune_interval - 1
         << "); abort." << dendl;
    r = false;
  }
  return r;
}

bool OSDMonitor::is_prune_enabled() const {
  return g_conf().get_val<bool>("mon_osdmap_full_prune_enabled");
}

bool OSDMonitor::is_prune_supported() const {
  return mon->get_required_mon_features().contains_any(
      ceph::features::mon::FEATURE_OSDMAP_PRUNE);
}

/** do_prune
 *
 * @returns true if has side-effects; false otherwise.
 */
bool OSDMonitor::do_prune(MonitorDBStore::TransactionRef tx)
{
  bool enabled = is_prune_enabled();

  dout(1) << __func__ << " osdmap full prune "
          << ( enabled ? "enabled" : "disabled")
          << dendl;

  if (!enabled || !_prune_sanitize_options() || !should_prune()) {
    return false;
  }

  // we are beyond the minimum prune versions, we need to remove maps because
  // otherwise the store will grow unbounded and we may end up having issues
  // with available disk space or store hangs.

  // we will not pin all versions. We will leave a buffer number of versions.
  // this allows us the monitor to trim maps without caring too much about
  // pinned maps, and then allow us to use another ceph-mon without these
  // capabilities, without having to repair the store.

  osdmap_manifest_t manifest = osdmap_manifest;

  version_t first = get_first_committed();
  version_t last = get_last_committed();

  version_t last_to_pin = last - g_conf()->mon_min_osdmap_epochs;
  version_t last_pinned = manifest.get_last_pinned();
  uint64_t prune_interval =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_interval");
  uint64_t txsize =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_txsize");

  prune_init(manifest);

  // we need to get rid of some osdmaps

  dout(5) << __func__
          << " lc (" << first << " .. " << last << ")"
          << " last_pinned " << last_pinned
          << " interval " << prune_interval
          << " last_to_pin " << last_to_pin
          << dendl;

  // We will be erasing maps as we go.
  //
  // We will erase all maps between `last_pinned` and the `next_to_pin`.
  //
  // If `next_to_pin` happens to be greater than `last_to_pin`, then
  // we stop pruning. We could prune the maps between `next_to_pin` and
  // `last_to_pin`, but by not doing it we end up with neater pruned
  // intervals, aligned with `prune_interval`. Besides, this should not be a
  // problem as long as `prune_interval` is set to a sane value, instead of
  // hundreds or thousands of maps.

  auto map_exists = [this](version_t v) {
    string k = mon->store->combine_strings("full", v);
    return mon->store->exists(get_service_name(), k);
  };

  // 'interval' represents the number of maps from the last pinned
  // i.e., if we pinned version 1 and have an interval of 10, we're pinning
  // version 11 next; all intermediate versions will be removed.
  //
  // 'txsize' represents the maximum number of versions we'll be removing in
  // this iteration. If 'txsize' is large enough to perform multiple passes
  // pinning and removing maps, we will do so; if not, we'll do at least one
  // pass. We are quite relaxed about honouring 'txsize', but we'll always
  // ensure that we never go *over* the maximum.

  // e.g., if we pin 1 and 11, we're removing versions [2..10]; i.e., 9 maps.
  uint64_t removal_interval = prune_interval - 1;

  if (txsize < removal_interval) {
    dout(5) << __func__
	    << " setting txsize to removal interval size ("
	    << removal_interval << " versions"
	    << dendl;
    txsize = removal_interval;
  }
  ceph_assert(removal_interval > 0);

  uint64_t num_pruned = 0;
  while (num_pruned + removal_interval <= txsize) { 
    last_pinned = manifest.get_last_pinned();

    if (last_pinned + prune_interval > last_to_pin) {
      break;
    }
    ceph_assert(last_pinned < last_to_pin);

    version_t next_pinned = last_pinned + prune_interval;
    ceph_assert(next_pinned <= last_to_pin);
    manifest.pin(next_pinned);

    dout(20) << __func__
	     << " last_pinned " << last_pinned
	     << " next_pinned " << next_pinned
	     << " num_pruned " << num_pruned
	     << " removal interval (" << (last_pinned+1)
	     << ".." << (next_pinned-1) << ")"
	     << " txsize " << txsize << dendl;

    ceph_assert(map_exists(last_pinned));
    ceph_assert(map_exists(next_pinned));

    for (version_t v = last_pinned+1; v < next_pinned; ++v) {
      ceph_assert(!manifest.is_pinned(v));

      dout(20) << __func__ << "   pruning full osdmap e" << v << dendl;
      string full_key = mon->store->combine_strings("full", v);
      tx->erase(get_service_name(), full_key);
      ++num_pruned;
    }
  }

  ceph_assert(num_pruned > 0);

  bufferlist bl;
  manifest.encode(bl);
  tx->put(get_service_name(), "osdmap_manifest", bl);

  return true;
}


// -------------

bool OSDMonitor::preprocess_query(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  Message *m = op->get_req();
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    try {
      return preprocess_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon->reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }
  case CEPH_MSG_MON_GET_OSDMAP:
    return preprocess_get_osdmap(op);

    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return preprocess_mark_me_down(op);
  case MSG_OSD_MARK_ME_DEAD:
    return preprocess_mark_me_dead(op);
  case MSG_OSD_FULL:
    return preprocess_full(op);
  case MSG_OSD_FAILURE:
    return preprocess_failure(op);
  case MSG_OSD_BOOT:
    return preprocess_boot(op);
  case MSG_OSD_ALIVE:
    return preprocess_alive(op);
  case MSG_OSD_PG_CREATED:
    return preprocess_pg_created(op);
  case MSG_OSD_PG_READY_TO_MERGE:
    return preprocess_pg_ready_to_merge(op);
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp(op);
  case MSG_OSD_BEACON:
    return preprocess_beacon(op);

  case CEPH_MSG_POOLOP:
    return preprocess_pool_op(op);

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps(op);

  case MSG_MON_GET_PURGED_SNAPS:
    return preprocess_get_purged_snaps(op);

  default:
    ceph_abort();
    return true;
  }
}

bool OSDMonitor::prepare_update(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  Message *m = op->get_req();
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return prepare_mark_me_down(op);
  case MSG_OSD_MARK_ME_DEAD:
    return prepare_mark_me_dead(op);
  case MSG_OSD_FULL:
    return prepare_full(op);
  case MSG_OSD_FAILURE:
    return prepare_failure(op);
  case MSG_OSD_BOOT:
    return prepare_boot(op);
  case MSG_OSD_ALIVE:
    return prepare_alive(op);
  case MSG_OSD_PG_CREATED:
    return prepare_pg_created(op);
  case MSG_OSD_PGTEMP:
    return prepare_pgtemp(op);
  case MSG_OSD_PG_READY_TO_MERGE:
    return prepare_pg_ready_to_merge(op);
  case MSG_OSD_BEACON:
    return prepare_beacon(op);

  case MSG_MON_COMMAND:
    try {
      return prepare_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon->reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }

  case CEPH_MSG_POOLOP:
    return prepare_pool_op(op);

  case MSG_REMOVE_SNAPS:
    return prepare_remove_snaps(op);


  default:
    ceph_abort();
  }

  return false;
}

bool OSDMonitor::should_propose(double& delay)
{
  dout(10) << "should_propose" << dendl;

  // if full map, propose immediately!  any subsequent changes will be clobbered.
  if (pending_inc.fullmap.length())
    return true;

  // adjust osd weights?
  if (!osd_weight.empty() &&
      osd_weight.size() == (unsigned)osdmap.get_max_osd()) {
    dout(0) << " adjusting osd weights based on " << osd_weight << dendl;
    osdmap.adjust_osd_weights(osd_weight, pending_inc);
    delay = 0.0;
    osd_weight.clear();
    return true;
  }

  return PaxosService::should_propose(delay);
}



// ---------------------------
// READs

bool OSDMonitor::preprocess_get_osdmap(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MMonGetOSDMap *m = static_cast<MMonGetOSDMap*>(op->get_req());

  uint64_t features = mon->get_quorum_con_features();
  if (op->get_session() && op->get_session()->con_features)
    features = op->get_session()->con_features;

  dout(10) << __func__ << " " << *m << dendl;
  MOSDMap *reply = new MOSDMap(mon->monmap->fsid, features);
  epoch_t first = get_first_committed();
  epoch_t last = osdmap.get_epoch();
  int max = g_conf()->osd_map_message_max;
  ssize_t max_bytes = g_conf()->osd_map_message_max_bytes;
  for (epoch_t e = std::max(first, m->get_full_first());
       e <= std::min(last, m->get_full_last()) && max > 0 && max_bytes > 0;
       ++e, --max) {
    bufferlist& bl = reply->maps[e];
    int r = get_version_full(e, features, bl);
    ceph_assert(r >= 0);
    max_bytes -= bl.length();
  }
  for (epoch_t e = std::max(first, m->get_inc_first());
       e <= std::min(last, m->get_inc_last()) && max > 0 && max_bytes > 0;
       ++e, --max) {
    bufferlist& bl = reply->incremental_maps[e];
    int r = get_version(e, features, bl);
    ceph_assert(r >= 0);
    max_bytes -= bl.length();
  }
  reply->oldest_map = first;
  reply->newest_map = last;
  mon->send_reply(op, reply);
  return true;
}


// ---------------------------
// UPDATEs

// failure --

bool OSDMonitor::check_source(MonOpRequestRef op, uuid_d fsid) {
  // check permissions
  MonSession *session = op->get_session();
  if (!session)
    return true;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "got MOSDFailure from entity with insufficient caps "
	    << session->caps << dendl;
    return true;
  }
  if (fsid != mon->monmap->fsid) {
    dout(0) << "check_source: on fsid " << fsid
	    << " != " << mon->monmap->fsid << dendl;
    return true;
  }
  return false;
}


bool OSDMonitor::preprocess_failure(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDFailure *m = static_cast<MOSDFailure*>(op->get_req());
  // who is target_osd
  int badboy = m->get_target_osd();

  // check permissions
  if (check_source(op, m->fsid))
    goto didit;

  // first, verify the reporting host is valid
  if (m->get_orig_source().is_osd()) {
    int from = m->get_orig_source().num();
    if (!osdmap.exists(from) ||
	!osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs()) ||
	(osdmap.is_down(from) && m->if_osd_failed())) {
      dout(5) << "preprocess_failure from dead osd." << from
	      << ", ignoring" << dendl;
      send_incremental(op, m->get_epoch()+1);
      goto didit;
    }
  }


  // weird?
  if (osdmap.is_down(badboy)) {
    dout(5) << "preprocess_failure dne(/dup?): osd." << m->get_target_osd()
	    << " " << m->get_target_addrs()
	    << ", from " << m->get_orig_source() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }
  if (osdmap.get_addrs(badboy) != m->get_target_addrs()) {
    dout(5) << "preprocess_failure wrong osd: report osd." << m->get_target_osd()
	    << " " << m->get_target_addrs()
	    << " != map's " << osdmap.get_addrs(badboy)
	    << ", from " << m->get_orig_source() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }

  // already reported?
  if (osdmap.is_down(badboy) ||
      osdmap.get_up_from(badboy) > m->get_epoch()) {
    dout(5) << "preprocess_failure dup/old: osd." << m->get_target_osd()
	    << " " << m->get_target_addrs()
	    << ", from " << m->get_orig_source() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }

  if (!can_mark_down(badboy)) {
    dout(5) << "preprocess_failure ignoring report of osd."
	    << m->get_target_osd() << " " << m->get_target_addrs()
	    << " from " << m->get_orig_source() << dendl;
    goto didit;
  }

  dout(10) << "preprocess_failure new: osd." << m->get_target_osd()
	   << " " << m->get_target_addrs()
	   << ", from " << m->get_orig_source() << dendl;
  return false;

 didit:
  mon->no_reply(op);
  return true;
}

class C_AckMarkedDown : public C_MonOp {
  OSDMonitor *osdmon;
public:
  C_AckMarkedDown(
    OSDMonitor *osdmon,
    MonOpRequestRef op)
    : C_MonOp(op), osdmon(osdmon) {}

  void _finish(int) override {
    MOSDMarkMeDown *m = static_cast<MOSDMarkMeDown*>(op->get_req());
    osdmon->mon->send_reply(
      op,
      new MOSDMarkMeDown(
	m->fsid,
	m->target_osd,
	m->target_addrs,
	m->get_epoch(),
	false));   // ACK itself does not request an ack
  }
  ~C_AckMarkedDown() override {
  }
};

bool OSDMonitor::preprocess_mark_me_down(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDMarkMeDown *m = static_cast<MOSDMarkMeDown*>(op->get_req());
  int from = m->target_osd;

  // check permissions
  if (check_source(op, m->fsid))
    goto reply;

  // first, verify the reporting host is valid
  if (!m->get_orig_source().is_osd())
    goto reply;

  if (!osdmap.exists(from) ||
      osdmap.is_down(from) ||
      osdmap.get_addrs(from) != m->target_addrs) {
    dout(5) << "preprocess_mark_me_down from dead osd."
	    << from << ", ignoring" << dendl;
    send_incremental(op, m->get_epoch()+1);
    goto reply;
  }

  // no down might be set
  if (!can_mark_down(from))
    goto reply;

  dout(10) << "MOSDMarkMeDown for: " << m->get_orig_source()
	   << " " << m->target_addrs << dendl;
  return false;

 reply:
  if (m->request_ack) {
    Context *c(new C_AckMarkedDown(this, op));
    c->complete(0);
  }
  return true;
}

bool OSDMonitor::prepare_mark_me_down(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDMarkMeDown *m = static_cast<MOSDMarkMeDown*>(op->get_req());
  int target_osd = m->target_osd;

  ceph_assert(osdmap.is_up(target_osd));
  ceph_assert(osdmap.get_addrs(target_osd) == m->target_addrs);

  mon->clog->info() << "osd." << target_osd << " marked itself down";
  pending_inc.new_state[target_osd] = CEPH_OSD_UP;
  if (m->request_ack)
    wait_for_finished_proposal(op, new C_AckMarkedDown(this, op));
  return true;
}

bool OSDMonitor::preprocess_mark_me_dead(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDMarkMeDead *m = static_cast<MOSDMarkMeDead*>(op->get_req());
  int from = m->target_osd;

  // check permissions
  if (check_source(op, m->fsid)) {
    mon->no_reply(op);
    return true;
  }

  // first, verify the reporting host is valid
  if (!m->get_orig_source().is_osd()) {
    mon->no_reply(op);
    return true;
  }

  if (!osdmap.exists(from) ||
      !osdmap.is_down(from)) {
    dout(5) << __func__ << " from nonexistent or up osd." << from
	    << ", ignoring" << dendl;
    send_incremental(op, m->get_epoch()+1);
    mon->no_reply(op);
    return true;
  }

  return false;
}

bool OSDMonitor::prepare_mark_me_dead(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDMarkMeDead *m = static_cast<MOSDMarkMeDead*>(op->get_req());
  int target_osd = m->target_osd;

  ceph_assert(osdmap.is_down(target_osd));

  mon->clog->info() << "osd." << target_osd << " marked itself dead as of e"
		    << m->get_epoch();
  if (!pending_inc.new_xinfo.count(target_osd)) {
    pending_inc.new_xinfo[target_osd] = osdmap.osd_xinfo[target_osd];
  }
  pending_inc.new_xinfo[target_osd].dead_epoch = m->get_epoch();
  wait_for_finished_proposal(
    op,
    new FunctionContext(
      [op, this] (int r) {
	if (r >= 0) {
	  mon->no_reply(op);	  // ignore on success
	}
      }
      ));
  return true;
}

bool OSDMonitor::can_mark_down(int i)
{
  if (osdmap.is_nodown(i)) {
    dout(5) << __func__ << " osd." << i << " is marked as nodown, "
            << "will not mark it down" << dendl;
    return false;
  }

  int num_osds = osdmap.get_num_osds();
  if (num_osds == 0) {
    dout(5) << __func__ << " no osds" << dendl;
    return false;
  }
  int up = osdmap.get_num_up_osds() - pending_inc.get_net_marked_down(&osdmap);
  float up_ratio = (float)up / (float)num_osds;
  if (up_ratio < g_conf()->mon_osd_min_up_ratio) {
    dout(2) << __func__ << " current up_ratio " << up_ratio << " < min "
	    << g_conf()->mon_osd_min_up_ratio
	    << ", will not mark osd." << i << " down" << dendl;
    return false;
  }
  return true;
}

bool OSDMonitor::can_mark_up(int i)
{
  if (osdmap.is_noup(i)) {
    dout(5) << __func__ << " osd." << i << " is marked as noup, "
            << "will not mark it up" << dendl;
    return false;
  }

  return true;
}

/**
 * @note the parameter @p i apparently only exists here so we can output the
 *	 osd's id on messages.
 */
bool OSDMonitor::can_mark_out(int i)
{
  if (osdmap.is_noout(i)) {
    dout(5) << __func__ << " osd." << i << " is marked as noout, "
            << "will not mark it out" << dendl;
    return false;
  }

  int num_osds = osdmap.get_num_osds();
  if (num_osds == 0) {
    dout(5) << __func__ << " no osds" << dendl;
    return false;
  }
  int in = osdmap.get_num_in_osds() - pending_inc.get_net_marked_out(&osdmap);
  float in_ratio = (float)in / (float)num_osds;
  if (in_ratio < g_conf()->mon_osd_min_in_ratio) {
    if (i >= 0)
      dout(5) << __func__ << " current in_ratio " << in_ratio << " < min "
	      << g_conf()->mon_osd_min_in_ratio
	      << ", will not mark osd." << i << " out" << dendl;
    else
      dout(5) << __func__ << " current in_ratio " << in_ratio << " < min "
	      << g_conf()->mon_osd_min_in_ratio
	      << ", will not mark osds out" << dendl;
    return false;
  }

  return true;
}

bool OSDMonitor::can_mark_in(int i)
{
  if (osdmap.is_noin(i)) {
    dout(5) << __func__ << " osd." << i << " is marked as noin, "
            << "will not mark it in" << dendl;
    return false;
  }

  return true;
}

bool OSDMonitor::check_failures(utime_t now)
{
  bool found_failure = false;
  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    if (can_mark_down(p->first)) {
      found_failure |= check_failure(now, p->first, p->second);
    }
  }
  return found_failure;
}

bool OSDMonitor::check_failure(utime_t now, int target_osd, failure_info_t& fi)
{
  // already pending failure?
  if (pending_inc.new_state.count(target_osd) &&
      pending_inc.new_state[target_osd] & CEPH_OSD_UP) {
    dout(10) << " already pending failure" << dendl;
    return true;
  }

  set<string> reporters_by_subtree;
  auto reporter_subtree_level = g_conf().get_val<string>("mon_osd_reporter_subtree_level");
  utime_t orig_grace(g_conf()->osd_heartbeat_grace, 0);
  utime_t max_failed_since = fi.get_failed_since();
  utime_t failed_for = now - max_failed_since;

  utime_t grace = orig_grace;
  double my_grace = 0, peer_grace = 0;
  double decay_k = 0;
  if (g_conf()->mon_osd_adjust_heartbeat_grace) {
    double halflife = (double)g_conf()->mon_osd_laggy_halflife;
    decay_k = ::log(.5) / halflife;

    // scale grace period based on historical probability of 'lagginess'
    // (false positive failures due to slowness).
    const osd_xinfo_t& xi = osdmap.get_xinfo(target_osd);
    double decay = exp((double)failed_for * decay_k);
    dout(20) << " halflife " << halflife << " decay_k " << decay_k
	     << " failed_for " << failed_for << " decay " << decay << dendl;
    my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
    grace += my_grace;
  }

  // consider the peers reporting a failure a proxy for a potential
  // 'subcluster' over the overall cluster that is similarly
  // laggy.  this is clearly not true in all cases, but will sometimes
  // help us localize the grace correction to a subset of the system
  // (say, a rack with a bad switch) that is unhappy.
  ceph_assert(fi.reporters.size());
  for (map<int,failure_reporter_t>::iterator p = fi.reporters.begin();
	p != fi.reporters.end();
	++p) {
    // get the parent bucket whose type matches with "reporter_subtree_level".
    // fall back to OSD if the level doesn't exist.
    map<string, string> reporter_loc = osdmap.crush->get_full_location(p->first);
    map<string, string>::iterator iter = reporter_loc.find(reporter_subtree_level);
    if (iter == reporter_loc.end()) {
      reporters_by_subtree.insert("osd." + to_string(p->first));
    } else {
      reporters_by_subtree.insert(iter->second);
    }
    if (g_conf()->mon_osd_adjust_heartbeat_grace) {
      const osd_xinfo_t& xi = osdmap.get_xinfo(p->first);
      utime_t elapsed = now - xi.down_stamp;
      double decay = exp((double)elapsed * decay_k);
      peer_grace += decay * (double)xi.laggy_interval * xi.laggy_probability;
    }
  }
  
  if (g_conf()->mon_osd_adjust_heartbeat_grace) {
    peer_grace /= (double)fi.reporters.size();
    grace += peer_grace;
  }

  dout(10) << " osd." << target_osd << " has "
	   << fi.reporters.size() << " reporters, "
	   << grace << " grace (" << orig_grace << " + " << my_grace
	   << " + " << peer_grace << "), max_failed_since " << max_failed_since
	   << dendl;

  if (failed_for >= grace &&
      reporters_by_subtree.size() >= g_conf().get_val<uint64_t>("mon_osd_min_down_reporters")) {
    dout(1) << " we have enough reporters to mark osd." << target_osd
	    << " down" << dendl;
    pending_inc.new_state[target_osd] = CEPH_OSD_UP;

    mon->clog->info() << "osd." << target_osd << " failed ("
		      << osdmap.crush->get_full_location_ordered_string(
			target_osd)
		      << ") ("
		      << (int)reporters_by_subtree.size()
		      << " reporters from different "
		      << reporter_subtree_level << " after "
		      << failed_for << " >= grace " << grace << ")";
    return true;
  }
  return false;
}

void OSDMonitor::force_failure(int target_osd, int by)
{
  // already pending failure?
  if (pending_inc.new_state.count(target_osd) &&
      pending_inc.new_state[target_osd] & CEPH_OSD_UP) {
    dout(10) << " already pending failure" << dendl;
    return;
  }

  dout(1) << " we're forcing failure of osd." << target_osd << dendl;
  pending_inc.new_state[target_osd] = CEPH_OSD_UP;
  if (!pending_inc.new_xinfo.count(target_osd)) {
    pending_inc.new_xinfo[target_osd] = osdmap.osd_xinfo[target_osd];
  }
  pending_inc.new_xinfo[target_osd].dead_epoch = pending_inc.epoch;

  mon->clog->info() << "osd." << target_osd << " failed ("
		    << osdmap.crush->get_full_location_ordered_string(target_osd)
		    << ") (connection refused reported by osd." << by << ")";
  return;
}

bool OSDMonitor::prepare_failure(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDFailure *m = static_cast<MOSDFailure*>(op->get_req());
  dout(1) << "prepare_failure osd." << m->get_target_osd()
	  << " " << m->get_target_addrs()
	  << " from " << m->get_orig_source()
          << " is reporting failure:" << m->if_osd_failed() << dendl;

  int target_osd = m->get_target_osd();
  int reporter = m->get_orig_source().num();
  ceph_assert(osdmap.is_up(target_osd));
  ceph_assert(osdmap.get_addrs(target_osd) == m->get_target_addrs());

  if (m->if_osd_failed()) {
    // calculate failure time
    utime_t now = ceph_clock_now();
    utime_t failed_since =
      m->get_recv_stamp() - utime_t(m->failed_for, 0);

    // add a report
    if (m->is_immediate()) {
      mon->clog->debug() << "osd." << m->get_target_osd()
			 << " reported immediately failed by "
			 << m->get_orig_source();
      force_failure(target_osd, reporter);
      mon->no_reply(op);
      return true;
    }
    mon->clog->debug() << "osd." << m->get_target_osd() << " reported failed by "
		      << m->get_orig_source();

    failure_info_t& fi = failure_info[target_osd];
    MonOpRequestRef old_op = fi.add_report(reporter, failed_since, op);
    if (old_op) {
      mon->no_reply(old_op);
    }

    return check_failure(now, target_osd, fi);
  } else {
    // remove the report
    mon->clog->debug() << "osd." << m->get_target_osd()
		       << " failure report canceled by "
		       << m->get_orig_source();
    if (failure_info.count(target_osd)) {
      failure_info_t& fi = failure_info[target_osd];
      MonOpRequestRef report_op = fi.cancel_report(reporter);
      if (report_op) {
        mon->no_reply(report_op);
      }
      if (fi.reporters.empty()) {
	dout(10) << " removing last failure_info for osd." << target_osd
		 << dendl;
	failure_info.erase(target_osd);
      } else {
	dout(10) << " failure_info for osd." << target_osd << " now "
		 << fi.reporters.size() << " reporters" << dendl;
      }
    } else {
      dout(10) << " no failure_info for osd." << target_osd << dendl;
    }
    mon->no_reply(op);
  }

  return false;
}

void OSDMonitor::process_failures()
{
  map<int,failure_info_t>::iterator p = failure_info.begin();
  while (p != failure_info.end()) {
    if (osdmap.is_up(p->first)) {
      ++p;
    } else {
      dout(10) << "process_failures osd." << p->first << dendl;
      list<MonOpRequestRef> ls;
      p->second.take_report_messages(ls);
      failure_info.erase(p++);

      while (!ls.empty()) {
        MonOpRequestRef o = ls.front();
        if (o) {
          o->mark_event(__func__);
          MOSDFailure *m = o->get_req<MOSDFailure>();
          send_latest(o, m->get_epoch());
	  mon->no_reply(o);
        }
	ls.pop_front();
      }
    }
  }
}

void OSDMonitor::take_all_failures(list<MonOpRequestRef>& ls)
{
  dout(10) << __func__ << " on " << failure_info.size() << " osds" << dendl;

  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    p->second.take_report_messages(ls);
  }
  failure_info.clear();
}


// boot --

bool OSDMonitor::preprocess_boot(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDBoot *m = static_cast<MOSDBoot*>(op->get_req());
  int from = m->get_orig_source_inst().name.num();

  // check permissions, ignore if failed (no response expected)
  MonSession *session = op->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "got preprocess_boot message from entity with insufficient caps"
	    << session->caps << dendl;
    goto ignore;
  }

  if (m->sb.cluster_fsid != mon->monmap->fsid) {
    dout(0) << "preprocess_boot on fsid " << m->sb.cluster_fsid
	    << " != " << mon->monmap->fsid << dendl;
    goto ignore;
  }

  if (m->get_orig_source_inst().addr.is_blank_ip()) {
    dout(0) << "preprocess_boot got blank addr for " << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  ceph_assert(m->get_orig_source_inst().name.is_osd());

  // force all osds to have gone through luminous prior to upgrade to nautilus
  {
    vector<string> missing;
    if (!HAVE_FEATURE(m->osd_features, SERVER_LUMINOUS)) {
      missing.push_back("CEPH_FEATURE_SERVER_LUMINOUS");
    }
    if (!HAVE_FEATURE(m->osd_features, SERVER_JEWEL)) {
      missing.push_back("CEPH_FEATURE_SERVER_JEWEL");
    }
    if (!HAVE_FEATURE(m->osd_features, SERVER_KRAKEN)) {
      missing.push_back("CEPH_FEATURE_SERVER_KRAKEN");
    }
    if (!HAVE_FEATURE(m->osd_features, OSD_RECOVERY_DELETES)) {
      missing.push_back("CEPH_FEATURE_OSD_RECOVERY_DELETES");
    }

    if (!missing.empty()) {
      using std::experimental::make_ostream_joiner;

      stringstream ss;
      copy(begin(missing), end(missing), make_ostream_joiner(ss, ";"));

      mon->clog->info() << "disallowing boot of OSD "
			<< m->get_orig_source_inst()
			<< " because the osd lacks " << ss.str();
      goto ignore;
    }
  }

  // make sure osd versions do not span more than 3 releases
  if (HAVE_FEATURE(m->osd_features, SERVER_OCTOPUS) &&
      osdmap.require_osd_release < ceph_release_t::mimic) {
    mon->clog->info() << "disallowing boot of octopus+ OSD "
		      << m->get_orig_source_inst()
		      << " because require_osd_release < mimic";
    goto ignore;
  }

  // The release check here is required because for OSD_PGLOG_HARDLIMIT,
  // we are reusing a jewel feature bit that was retired in luminous.
  if (osdmap.require_osd_release >= ceph_release_t::luminous &&
      osdmap.test_flag(CEPH_OSDMAP_PGLOG_HARDLIMIT) &&
      !(m->osd_features & CEPH_FEATURE_OSD_PGLOG_HARDLIMIT)) {
    mon->clog->info() << "disallowing boot of OSD "
		      << m->get_orig_source_inst()
		      << " because 'pglog_hardlimit' osdmap flag is set and OSD lacks the OSD_PGLOG_HARDLIMIT feature";
    goto ignore;
  }

  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs()) &&
      osdmap.get_cluster_addrs(from).legacy_equals(m->cluster_addrs)) {
    // yup.
    dout(7) << "preprocess_boot dup from " << m->get_orig_source()
	    << " " << m->get_orig_source_addrs()
	    << " =~ " << osdmap.get_addrs(from) << dendl;
    _booted(op, false);
    return true;
  }

  if (osdmap.exists(from) &&
      !osdmap.get_uuid(from).is_zero() &&
      osdmap.get_uuid(from) != m->sb.osd_fsid) {
    dout(7) << __func__ << " from " << m->get_orig_source_inst()
            << " clashes with existing osd: different fsid"
            << " (ours: " << osdmap.get_uuid(from)
            << " ; theirs: " << m->sb.osd_fsid << ")" << dendl;
    goto ignore;
  }

  if (osdmap.exists(from) &&
      osdmap.get_info(from).up_from > m->version &&
      osdmap.get_most_recent_addrs(from).legacy_equals(
	m->get_orig_source_addrs())) {
    dout(7) << "prepare_boot msg from before last up_from, ignoring" << dendl;
    send_latest(op, m->sb.current_epoch+1);
    return true;
  }

  // noup?
  if (!can_mark_up(from)) {
    dout(7) << "preprocess_boot ignoring boot from " << m->get_orig_source_inst() << dendl;
    send_latest(op, m->sb.current_epoch+1);
    return true;
  }

  dout(10) << "preprocess_boot from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  return true;
}

bool OSDMonitor::prepare_boot(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDBoot *m = static_cast<MOSDBoot*>(op->get_req());
  dout(7) << __func__ << " from " << m->get_source()
	  << " sb " << m->sb
	  << " client_addrs" << m->get_connection()->get_peer_addrs()
	  << " cluster_addrs " << m->cluster_addrs
	  << " hb_back_addrs " << m->hb_back_addrs
	  << " hb_front_addrs " << m->hb_front_addrs
	  << dendl;

  ceph_assert(m->get_orig_source().is_osd());
  int from = m->get_orig_source().num();

  // does this osd exist?
  if (from >= osdmap.get_max_osd()) {
    dout(1) << "boot from osd." << from << " >= max_osd "
	    << osdmap.get_max_osd() << dendl;
    return false;
  }

  int oldstate = osdmap.exists(from) ? osdmap.get_state(from) : CEPH_OSD_NEW;
  if (pending_inc.new_state.count(from))
    oldstate ^= pending_inc.new_state[from];

  // already up?  mark down first?
  if (osdmap.is_up(from)) {
    dout(7) << __func__ << " was up, first marking down osd." << from << " "
	    << osdmap.get_addrs(from) << dendl;
    // preprocess should have caught these;  if not, assert.
    ceph_assert(!osdmap.get_addrs(from).legacy_equals(
		  m->get_orig_source_addrs()) ||
		!osdmap.get_cluster_addrs(from).legacy_equals(m->cluster_addrs));
    ceph_assert(osdmap.get_uuid(from) == m->sb.osd_fsid);

    if (pending_inc.new_state.count(from) == 0 ||
	(pending_inc.new_state[from] & CEPH_OSD_UP) == 0) {
      // mark previous guy down
      pending_inc.new_state[from] = CEPH_OSD_UP;
    }
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  } else if (pending_inc.new_up_client.count(from)) {
    // already prepared, just wait
    dout(7) << __func__ << " already prepared, waiting on "
	    << m->get_orig_source_addr() << dendl;
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  } else {
    // mark new guy up.
    pending_inc.new_up_client[from] = m->get_orig_source_addrs();
    pending_inc.new_up_cluster[from] = m->cluster_addrs;
    pending_inc.new_hb_back_up[from] = m->hb_back_addrs;
    pending_inc.new_hb_front_up[from] = m->hb_front_addrs;

    down_pending_out.erase(from);  // if any

    if (m->sb.weight)
      osd_weight[from] = m->sb.weight;

    // set uuid?
    dout(10) << " setting osd." << from << " uuid to " << m->sb.osd_fsid
	     << dendl;
    if (!osdmap.exists(from) || osdmap.get_uuid(from) != m->sb.osd_fsid) {
      // preprocess should have caught this;  if not, assert.
      ceph_assert(!osdmap.exists(from) || osdmap.get_uuid(from).is_zero());
      pending_inc.new_uuid[from] = m->sb.osd_fsid;
    }

    // fresh osd?
    if (m->sb.newest_map == 0 && osdmap.exists(from)) {
      const osd_info_t& i = osdmap.get_info(from);
      if (i.up_from > i.lost_at) {
	dout(10) << " fresh osd; marking lost_at too" << dendl;
	pending_inc.new_lost[from] = osdmap.get_epoch();
      }
    }

    // metadata
    bufferlist osd_metadata;
    encode(m->metadata, osd_metadata);
    pending_metadata[from] = osd_metadata;
    pending_metadata_rm.erase(from);

    // adjust last clean unmount epoch?
    const osd_info_t& info = osdmap.get_info(from);
    dout(10) << " old osd_info: " << info << dendl;
    if (m->sb.mounted > info.last_clean_begin ||
	(m->sb.mounted == info.last_clean_begin &&
	 m->sb.clean_thru > info.last_clean_end)) {
      epoch_t begin = m->sb.mounted;
      epoch_t end = m->sb.clean_thru;

      dout(10) << __func__ << " osd." << from << " last_clean_interval "
	       << "[" << info.last_clean_begin << "," << info.last_clean_end
	       << ") -> [" << begin << "-" << end << ")"
	       << dendl;
      pending_inc.new_last_clean_interval[from] =
	pair<epoch_t,epoch_t>(begin, end);
    }

    if (pending_inc.new_xinfo.count(from) == 0)
      pending_inc.new_xinfo[from] = osdmap.osd_xinfo[from];
    osd_xinfo_t& xi = pending_inc.new_xinfo[from];
    if (m->boot_epoch == 0) {
      xi.laggy_probability *= (1.0 - g_conf()->mon_osd_laggy_weight);
      xi.laggy_interval *= (1.0 - g_conf()->mon_osd_laggy_weight);
      dout(10) << " not laggy, new xi " << xi << dendl;
    } else {
      if (xi.down_stamp.sec()) {
        int interval = ceph_clock_now().sec() -
	  xi.down_stamp.sec();
        if (g_conf()->mon_osd_laggy_max_interval &&
	    (interval > g_conf()->mon_osd_laggy_max_interval)) {
          interval =  g_conf()->mon_osd_laggy_max_interval;
        }
        xi.laggy_interval =
	  interval * g_conf()->mon_osd_laggy_weight +
	  xi.laggy_interval * (1.0 - g_conf()->mon_osd_laggy_weight);
      }
      xi.laggy_probability =
	g_conf()->mon_osd_laggy_weight +
	xi.laggy_probability * (1.0 - g_conf()->mon_osd_laggy_weight);
      dout(10) << " laggy, now xi " << xi << dendl;
    }

    // set features shared by the osd
    if (m->osd_features)
      xi.features = m->osd_features;
    else
      xi.features = m->get_connection()->get_features();

    // mark in?
    if ((g_conf()->mon_osd_auto_mark_auto_out_in &&
	 (oldstate & CEPH_OSD_AUTOOUT)) ||
	(g_conf()->mon_osd_auto_mark_new_in && (oldstate & CEPH_OSD_NEW)) ||
	(g_conf()->mon_osd_auto_mark_in)) {
      if (can_mark_in(from)) {
	if (xi.old_weight > 0) {
	  pending_inc.new_weight[from] = xi.old_weight;
	  xi.old_weight = 0;
	} else {
	  pending_inc.new_weight[from] = CEPH_OSD_IN;
	}
      } else {
	dout(7) << __func__ << " NOIN set, will not mark in "
		<< m->get_orig_source_addr() << dendl;
      }
    }

    // wait
    wait_for_finished_proposal(op, new C_Booted(this, op));
  }
  return true;
}

void OSDMonitor::_booted(MonOpRequestRef op, bool logit)
{
  op->mark_osdmon_event(__func__);
  MOSDBoot *m = static_cast<MOSDBoot*>(op->get_req());
  dout(7) << "_booted " << m->get_orig_source_inst() 
	  << " w " << m->sb.weight << " from " << m->sb.current_epoch << dendl;

  if (logit) {
    mon->clog->info() << m->get_source() << " " << m->get_orig_source_addrs()
		      << " boot";
  }

  send_latest(op, m->sb.current_epoch+1);
}


// -------------
// full

bool OSDMonitor::preprocess_full(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDFull *m = static_cast<MOSDFull*>(op->get_req());
  int from = m->get_orig_source().num();
  set<string> state;
  unsigned mask = CEPH_OSD_NEARFULL | CEPH_OSD_BACKFILLFULL | CEPH_OSD_FULL;

  // check permissions, ignore if failed
  MonSession *session = op->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "MOSDFull from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  // ignore a full message from the osd instance that already went down
  if (!osdmap.exists(from)) {
    dout(7) << __func__ << " ignoring full message from nonexistent "
	    << m->get_orig_source_inst() << dendl;
    goto ignore;
  }
  if ((!osdmap.is_up(from) &&
       osdmap.get_most_recent_addrs(from).legacy_equals(
	 m->get_orig_source_addrs())) ||
      (osdmap.is_up(from) &&
       !osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs()))) {
    dout(7) << __func__ << " ignoring full message from down "
	    << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  OSDMap::calc_state_set(osdmap.get_state(from), state);

  if ((osdmap.get_state(from) & mask) == m->state) {
    dout(7) << __func__ << " state already " << state << " for osd." << from
	    << " " << m->get_orig_source_inst() << dendl;
    _reply_map(op, m->version);
    goto ignore;
  }

  dout(10) << __func__ << " want state " << state << " for osd." << from
	   << " " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  return true;
}

bool OSDMonitor::prepare_full(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  const MOSDFull *m = static_cast<MOSDFull*>(op->get_req());
  const int from = m->get_orig_source().num();

  const unsigned mask = CEPH_OSD_NEARFULL | CEPH_OSD_BACKFILLFULL | CEPH_OSD_FULL;
  const unsigned want_state = m->state & mask;  // safety first

  unsigned cur_state = osdmap.get_state(from);
  auto p = pending_inc.new_state.find(from);
  if (p != pending_inc.new_state.end()) {
    cur_state ^= p->second;
  }
  cur_state &= mask;

  set<string> want_state_set, cur_state_set;
  OSDMap::calc_state_set(want_state, want_state_set);
  OSDMap::calc_state_set(cur_state, cur_state_set);

  if (cur_state != want_state) {
    if (p != pending_inc.new_state.end()) {
      p->second &= ~mask;
    } else {
      pending_inc.new_state[from] = 0;
    }
    pending_inc.new_state[from] |= (osdmap.get_state(from) & mask) ^ want_state;
    dout(7) << __func__ << " osd." << from << " " << cur_state_set
	    << " -> " << want_state_set << dendl;
  } else {
    dout(7) << __func__ << " osd." << from << " " << cur_state_set
	    << " = wanted " << want_state_set << ", just waiting" << dendl;
  }

  wait_for_finished_proposal(op, new C_ReplyMap(this, op, m->version));
  return true;
}

// -------------
// alive

bool OSDMonitor::preprocess_alive(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDAlive *m = static_cast<MOSDAlive*>(op->get_req());
  int from = m->get_orig_source().num();

  // check permissions, ignore if failed
  MonSession *session = op->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "attempt to send MOSDAlive from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap.is_up(from) ||
      !osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs())) {
    dout(7) << "preprocess_alive ignoring alive message from down "
	    << m->get_orig_source() << " " << m->get_orig_source_addrs()
	    << dendl;
    goto ignore;
  }

  if (osdmap.get_up_thru(from) >= m->want) {
    // yup.
    dout(7) << "preprocess_alive want up_thru " << m->want << " dup from " << m->get_orig_source_inst() << dendl;
    _reply_map(op, m->version);
    return true;
  }

  dout(10) << "preprocess_alive want up_thru " << m->want
	   << " from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  return true;
}

bool OSDMonitor::prepare_alive(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDAlive *m = static_cast<MOSDAlive*>(op->get_req());
  int from = m->get_orig_source().num();

  if (0) {  // we probably don't care much about these
    mon->clog->debug() << m->get_orig_source_inst() << " alive";
  }

  dout(7) << "prepare_alive want up_thru " << m->want << " have " << m->version
	  << " from " << m->get_orig_source_inst() << dendl;

  update_up_thru(from, m->version); // set to the latest map the OSD has
  wait_for_finished_proposal(op, new C_ReplyMap(this, op, m->version));
  return true;
}

void OSDMonitor::_reply_map(MonOpRequestRef op, epoch_t e)
{
  op->mark_osdmon_event(__func__);
  dout(7) << "_reply_map " << e
	  << " from " << op->get_req()->get_orig_source_inst()
	  << dendl;
  send_latest(op, e);
}

// pg_created
bool OSDMonitor::preprocess_pg_created(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = static_cast<MOSDPGCreated*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  auto session = op->get_session();
  mon->no_reply(op);
  if (!session) {
    dout(10) << __func__ << ": no monitor session!" << dendl;
    return true;
  }
  if (!session->is_capable("osd", MON_CAP_X)) {
    derr << __func__ << " received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    return true;
  }
  // always forward the "created!" to the leader
  return false;
}

bool OSDMonitor::prepare_pg_created(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = static_cast<MOSDPGCreated*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  auto src = m->get_orig_source();
  auto from = src.num();
  if (!src.is_osd() ||
      !mon->osdmon()->osdmap.is_up(from) ||
      !mon->osdmon()->osdmap.get_addrs(from).legacy_equals(
	m->get_orig_source_addrs())) {
    dout(1) << __func__ << " ignoring stats from non-active osd." << dendl;
    return false;
  }
  pending_created_pgs.push_back(m->pgid);
  return true;
}

bool OSDMonitor::preprocess_pg_ready_to_merge(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = static_cast<MOSDPGReadyToMerge*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  const pg_pool_t *pi;
  auto session = op->get_session();
  if (!session) {
    dout(10) << __func__ << ": no monitor session!" << dendl;
    goto ignore;
  }
  if (!session->is_capable("osd", MON_CAP_X)) {
    derr << __func__ << " received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    goto ignore;
  }
  pi = osdmap.get_pg_pool(m->pgid.pool());
  if (!pi) {
    derr << __func__ << " pool for " << m->pgid << " dne" << dendl;
    goto ignore;
  }
  if (pi->get_pg_num() <= m->pgid.ps()) {
    dout(20) << " pg_num " << pi->get_pg_num() << " already < " << m->pgid << dendl;
    goto ignore;
  }
  if (pi->get_pg_num() != m->pgid.ps() + 1) {
    derr << " OSD trying to merge wrong pgid " << m->pgid << dendl;
    goto ignore;
  }
  if (pi->get_pg_num_pending() > m->pgid.ps()) {
    dout(20) << " pg_num_pending " << pi->get_pg_num_pending() << " > " << m->pgid << dendl;
    goto ignore;
  }
  return false;

 ignore:
  mon->no_reply(op);
  return true;
}

bool OSDMonitor::prepare_pg_ready_to_merge(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = static_cast<MOSDPGReadyToMerge*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  pg_pool_t p;
  if (pending_inc.new_pools.count(m->pgid.pool()))
    p = pending_inc.new_pools[m->pgid.pool()];
  else
    p = *osdmap.get_pg_pool(m->pgid.pool());
  if (p.get_pg_num() != m->pgid.ps() + 1 ||
      p.get_pg_num_pending() > m->pgid.ps()) {
    dout(10) << __func__
	     << " race with concurrent pg_num[_pending] update, will retry"
	     << dendl;
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
    return true;
  }

  if (m->ready) {
    p.dec_pg_num(m->pgid,
		 pending_inc.epoch,
		 m->source_version,
		 m->target_version,
		 m->last_epoch_started,
		 m->last_epoch_clean);
    p.last_change = pending_inc.epoch;
  } else {
    // back off the merge attempt!
    p.set_pg_num_pending(p.get_pg_num());
  }

  // force pre-nautilus clients to resend their ops, since they
  // don't understand pg_num_pending changes form a new interval
  p.last_force_op_resend_prenautilus = pending_inc.epoch;

  pending_inc.new_pools[m->pgid.pool()] = p;

  auto prob = g_conf().get_val<double>("mon_inject_pg_merge_bounce_probability");
  if (m->ready &&
      prob > 0 &&
      prob > (double)(rand() % 1000)/1000.0) {
    derr << __func__ << " injecting pg merge pg_num bounce" << dendl;
    auto n = new MMonCommand(mon->monmap->get_fsid());
    n->set_connection(m->get_connection());
    n->cmd = { "{\"prefix\":\"osd pool set\", \"pool\": \"" +
	       osdmap.get_pool_name(m->pgid.pool()) +
	       "\", \"var\": \"pg_num_actual\", \"val\": \"" +
	       stringify(m->pgid.ps() + 1) + "\"}" };
    MonOpRequestRef nop = mon->op_tracker.create_request<MonOpRequest>(n);
    nop->set_type_service();
    wait_for_finished_proposal(op, new C_RetryMessage(this, nop));
  } else {
    wait_for_finished_proposal(op, new C_ReplyMap(this, op, m->version));
  }
  return true;
}


// -------------
// pg_temp changes

bool OSDMonitor::preprocess_pgtemp(MonOpRequestRef op)
{
  MOSDPGTemp *m = static_cast<MOSDPGTemp*>(op->get_req());
  dout(10) << "preprocess_pgtemp " << *m << dendl;
  mempool::osdmap::vector<int> empty;
  int from = m->get_orig_source().num();
  size_t ignore_cnt = 0;

  // check caps
  MonSession *session = op->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "attempt to send MOSDPGTemp from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap.is_up(from) ||
      !osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs())) {
    dout(7) << "ignoring pgtemp message from down "
	    << m->get_orig_source() << " " << m->get_orig_source_addrs()
	    << dendl;
    goto ignore;
  }

  if (m->forced) {
    return false;
  }

  for (auto p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p) {
    dout(20) << " " << p->first
	     << (osdmap.pg_temp->count(p->first) ? osdmap.pg_temp->get(p->first) : empty)
             << " -> " << p->second << dendl;

    // does the pool exist?
    if (!osdmap.have_pg_pool(p->first.pool())) {
      /*
       * 1. If the osdmap does not have the pool, it means the pool has been
       *    removed in-between the osd sending this message and us handling it.
       * 2. If osdmap doesn't have the pool, it is safe to assume the pool does
       *    not exist in the pending either, as the osds would not send a
       *    message about a pool they know nothing about (yet).
       * 3. However, if the pool does exist in the pending, then it must be a
       *    new pool, and not relevant to this message (see 1).
       */
      dout(10) << __func__ << " ignore " << p->first << " -> " << p->second
               << ": pool has been removed" << dendl;
      ignore_cnt++;
      continue;
    }

    int acting_primary = -1;
    osdmap.pg_to_up_acting_osds(
      p->first, nullptr, nullptr, nullptr, &acting_primary);
    if (acting_primary != from) {
      /* If the source isn't the primary based on the current osdmap, we know
       * that the interval changed and that we can discard this message.
       * Indeed, we must do so to avoid 16127 since we can't otherwise determine
       * which of two pg temp mappings on the same pg is more recent.
       */
      dout(10) << __func__ << " ignore " << p->first << " -> " << p->second
	       << ": primary has changed" << dendl;
      ignore_cnt++;
      continue;
    }

    // removal?
    if (p->second.empty() && (osdmap.pg_temp->count(p->first) ||
			      osdmap.primary_temp->count(p->first)))
      return false;
    // change?
    //  NOTE: we assume that this will clear pg_primary, so consider
    //        an existing pg_primary field to imply a change
    if (p->second.size() &&
	(osdmap.pg_temp->count(p->first) == 0 ||
	 osdmap.pg_temp->get(p->first) != p->second ||
	 osdmap.primary_temp->count(p->first)))
      return false;
  }

  // should we ignore all the pgs?
  if (ignore_cnt == m->pg_temp.size())
    goto ignore;

  dout(7) << "preprocess_pgtemp e" << m->map_epoch << " no changes from " << m->get_orig_source_inst() << dendl;
  _reply_map(op, m->map_epoch);
  return true;

 ignore:
  return true;
}

void OSDMonitor::update_up_thru(int from, epoch_t up_thru)
{
  epoch_t old_up_thru = osdmap.get_up_thru(from);
  auto ut = pending_inc.new_up_thru.find(from);
  if (ut != pending_inc.new_up_thru.end()) {
    old_up_thru = ut->second;
  }
  if (up_thru > old_up_thru) {
    // set up_thru too, so the osd doesn't have to ask again
    pending_inc.new_up_thru[from] = up_thru;
  }
}

bool OSDMonitor::prepare_pgtemp(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDPGTemp *m = static_cast<MOSDPGTemp*>(op->get_req());
  int from = m->get_orig_source().num();
  dout(7) << "prepare_pgtemp e" << m->map_epoch << " from " << m->get_orig_source_inst() << dendl;
  for (map<pg_t,vector<int32_t> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p) {
    uint64_t pool = p->first.pool();
    if (pending_inc.old_pools.count(pool)) {
      dout(10) << __func__ << " ignore " << p->first << " -> " << p->second
               << ": pool pending removal" << dendl;
      continue;
    }
    if (!osdmap.have_pg_pool(pool)) {
      dout(10) << __func__ << " ignore " << p->first << " -> " << p->second
               << ": pool has been removed" << dendl;
      continue;
    }
    pending_inc.new_pg_temp[p->first] =
      mempool::osdmap::vector<int>(p->second.begin(), p->second.end());

    // unconditionally clear pg_primary (until this message can encode
    // a change for that, too.. at which point we need to also fix
    // preprocess_pg_temp)
    if (osdmap.primary_temp->count(p->first) ||
	pending_inc.new_primary_temp.count(p->first))
      pending_inc.new_primary_temp[p->first] = -1;
  }

  // set up_thru too, so the osd doesn't have to ask again
  update_up_thru(from, m->map_epoch);

  wait_for_finished_proposal(op, new C_ReplyMap(this, op, m->map_epoch));
  return true;
}


// ---

bool OSDMonitor::preprocess_remove_snaps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MRemoveSnaps *m = static_cast<MRemoveSnaps*>(op->get_req());
  dout(7) << "preprocess_remove_snaps " << *m << dendl;

  // check privilege, ignore if failed
  MonSession *session = op->get_session();
  mon->no_reply(op);
  if (!session)
    goto ignore;
  if (!session->caps.is_capable(
	cct,
	CEPH_ENTITY_TYPE_MON,
	session->entity_name,
        "osd", "osd pool rmsnap", {}, true, true, false,
	session->get_peer_socket_addr())) {
    dout(0) << "got preprocess_remove_snaps from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  for (map<int, vector<snapid_t> >::iterator q = m->snaps.begin();
       q != m->snaps.end();
       ++q) {
    if (!osdmap.have_pg_pool(q->first)) {
      dout(10) << " ignoring removed_snaps " << q->second
	       << " on non-existent pool " << q->first << dendl;
      continue;
    }
    const pg_pool_t *pi = osdmap.get_pg_pool(q->first);
    for (vector<snapid_t>::iterator p = q->second.begin();
	 p != q->second.end();
	 ++p) {
      if (*p > pi->get_snap_seq() ||
	  !_is_removed_snap(q->first, *p)) {
	return false;
      }
    }
  }

  if (HAVE_FEATURE(m->get_connection()->get_features(), SERVER_OCTOPUS)) {
    auto reply = make_message<MRemoveSnaps>();
    reply->snaps = m->snaps;
    mon->send_reply(op, reply.detach());
  }

 ignore:
  return true;
}

bool OSDMonitor::prepare_remove_snaps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MRemoveSnaps *m = static_cast<MRemoveSnaps*>(op->get_req());
  dout(7) << "prepare_remove_snaps " << *m << dendl;

  for (auto& [pool, snaps] : m->snaps) {
    if (!osdmap.have_pg_pool(pool)) {
      dout(10) << " ignoring removed_snaps " << snaps
	       << " on non-existent pool " << pool << dendl;
      continue;
    }

    pg_pool_t& pi = osdmap.pools[pool];
    for (auto s : snaps) {
      if (!_is_removed_snap(pool, s) &&
	  (!pending_inc.new_pools.count(pool) ||
	   !pending_inc.new_pools[pool].removed_snaps.contains(s)) &&
	  (!pending_inc.new_removed_snaps.count(pool) ||
	   !pending_inc.new_removed_snaps[pool].contains(s))) {
	pg_pool_t *newpi = pending_inc.get_new_pool(pool, &pi);
	if (osdmap.require_osd_release < ceph_release_t::octopus) {
	  newpi->removed_snaps.insert(s);
	  dout(10) << " pool " << pool << " removed_snaps added " << s
		   << " (now " << newpi->removed_snaps << ")" << dendl;
	}
	newpi->flags |= pg_pool_t::FLAG_SELFMANAGED_SNAPS;
	if (s > newpi->get_snap_seq()) {
	  dout(10) << " pool " << pool << " snap_seq "
		   << newpi->get_snap_seq() << " -> " << s << dendl;
	  newpi->set_snap_seq(s);
	}
	newpi->set_snap_epoch(pending_inc.epoch);
	dout(10) << " added pool " << pool << " snap " << s
		 << " to removed_snaps queue" << dendl;
	pending_inc.new_removed_snaps[pool].insert(s);
      }
    }
  }

  if (HAVE_FEATURE(m->get_connection()->get_features(), SERVER_OCTOPUS)) {
    auto reply = make_message<MRemoveSnaps>();
    reply->snaps = m->snaps;
    wait_for_finished_proposal(op, new C_ReplyOp(this, op, reply));
  }

  return true;
}

bool OSDMonitor::preprocess_get_purged_snaps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  const MMonGetPurgedSnaps *m = static_cast<MMonGetPurgedSnaps*>(op->get_req());
  dout(7) << __func__ << " " << *m << dendl;

  map<epoch_t,mempool::osdmap::map<int64_t,snap_interval_set_t>> r;

  string k = make_purged_snap_epoch_key(m->start);
  auto it = mon->store->get_iterator(OSD_SNAP_PREFIX);
  it->upper_bound(k);
  unsigned long epoch = m->last;
  while (it->valid()) {
    if (it->key().find("purged_epoch_") != 0) {
      break;
    }
    string k = it->key();
    int n = sscanf(k.c_str(), "purged_epoch_%lx", &epoch);
    if (n != 1) {
      derr << __func__ << " unable to parse key '" << it->key() << "'" << dendl;
    } else if (epoch > m->last) {
      break;
    } else {
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      auto &v = r[epoch];
      try {
	ceph::decode(v, p);
      } catch (buffer::error& e) {
	derr << __func__ << " unable to parse value for key '" << it->key()
	     << "': \n";
	bl.hexdump(*_dout);
	*_dout << dendl;
      }
      n += 4 + v.size() * 16;
    }
    if (n > 1048576) {
      // impose a semi-arbitrary limit to message size
      break;
    }
    it->next();
  }

  auto reply = make_message<MMonGetPurgedSnapsReply>(m->start, epoch);
  reply->purged_snaps.swap(r);
  mon->send_reply(op, reply.detach());

  return true;
}

// osd beacon
bool OSDMonitor::preprocess_beacon(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  // check caps
  auto session = op->get_session();
  mon->no_reply(op);
  if (!session) {
    dout(10) << __func__ << " no monitor session!" << dendl;
    return true;
  }
  if (!session->is_capable("osd", MON_CAP_X)) {
    derr << __func__ << " received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    return true;
  }
  // Always forward the beacon to the leader, even if they are the same as
  // the old one. The leader will mark as down osds that haven't sent
  // beacon for a few minutes.
  return false;
}

bool OSDMonitor::prepare_beacon(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  const auto beacon = static_cast<MOSDBeacon*>(op->get_req());
  const auto src = beacon->get_orig_source();
  dout(10) << __func__ << " " << *beacon
	   << " from " << src << dendl;
  int from = src.num();

  if (!src.is_osd() ||
      !osdmap.is_up(from) ||
      !osdmap.get_addrs(from).legacy_equals(beacon->get_orig_source_addrs())) {
    if (src.is_osd() && !osdmap.is_up(from)) {
      // share some new maps with this guy in case it may not be
      // aware of its own deadness...
      send_latest(op, beacon->version+1);
    }
    dout(1) << " ignoring beacon from non-active osd." << from << dendl;
    return false;
  }

  last_osd_report[from] = ceph_clock_now();
  osd_epochs[from] = beacon->version;

  for (const auto& pg : beacon->pgs) {
    last_epoch_clean.report(pg, beacon->min_last_epoch_clean);
  }

  if (osdmap.osd_xinfo[from].last_purged_snaps_scrub <
      beacon->last_purged_snaps_scrub) {
    if (pending_inc.new_xinfo.count(from) == 0) {
      pending_inc.new_xinfo[from] = osdmap.osd_xinfo[from];
    }
    pending_inc.new_xinfo[from].last_purged_snaps_scrub =
      beacon->last_purged_snaps_scrub;
    return true;
  } else {
    return false;
  }
}

// ---------------
// map helpers

void OSDMonitor::send_latest(MonOpRequestRef op, epoch_t start)
{
  op->mark_osdmon_event(__func__);
  dout(5) << "send_latest to " << op->get_req()->get_orig_source_inst()
	  << " start " << start << dendl;
  if (start == 0)
    send_full(op);
  else
    send_incremental(op, start);
}


MOSDMap *OSDMonitor::build_latest_full(uint64_t features)
{
  MOSDMap *r = new MOSDMap(mon->monmap->fsid, features);
  get_version_full(osdmap.get_epoch(), features, r->maps[osdmap.get_epoch()]);
  r->oldest_map = get_first_committed();
  r->newest_map = osdmap.get_epoch();
  return r;
}

MOSDMap *OSDMonitor::build_incremental(epoch_t from, epoch_t to, uint64_t features)
{
  dout(10) << "build_incremental [" << from << ".." << to << "] with features "
	   << std::hex << features << std::dec << dendl;
  MOSDMap *m = new MOSDMap(mon->monmap->fsid, features);
  m->oldest_map = get_first_committed();
  m->newest_map = osdmap.get_epoch();

  for (epoch_t e = to; e >= from && e > 0; e--) {
    bufferlist bl;
    int err = get_version(e, features, bl);
    if (err == 0) {
      ceph_assert(bl.length());
      // if (get_version(e, bl) > 0) {
      dout(20) << "build_incremental    inc " << e << " "
	       << bl.length() << " bytes" << dendl;
      m->incremental_maps[e] = bl;
    } else {
      ceph_assert(err == -ENOENT);
      ceph_assert(!bl.length());
      get_version_full(e, features, bl);
      if (bl.length() > 0) {
      //else if (get_version("full", e, bl) > 0) {
      dout(20) << "build_incremental   full " << e << " "
	       << bl.length() << " bytes" << dendl;
      m->maps[e] = bl;
      } else {
	ceph_abort();  // we should have all maps.
      }
    }
  }
  return m;
}

void OSDMonitor::send_full(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  dout(5) << "send_full to " << op->get_req()->get_orig_source_inst() << dendl;
  mon->send_reply(op, build_latest_full(op->get_session()->con_features));
}

void OSDMonitor::send_incremental(MonOpRequestRef op, epoch_t first)
{
  op->mark_osdmon_event(__func__);

  MonSession *s = op->get_session();
  ceph_assert(s);

  if (s->proxy_con) {
    // oh, we can tell the other mon to do it
    dout(10) << __func__ << " asking proxying mon to send_incremental from "
	     << first << dendl;
    MRoute *r = new MRoute(s->proxy_tid, NULL);
    r->send_osdmap_first = first;
    s->proxy_con->send_message(r);
    op->mark_event("reply: send routed send_osdmap_first reply");
  } else {
    // do it ourselves
    send_incremental(first, s, false, op);
  }
}

void OSDMonitor::send_incremental(epoch_t first,
				  MonSession *session,
				  bool onetime,
				  MonOpRequestRef req)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << session->name << dendl;

  // get feature of the peer
  // use quorum_con_features, if it's an anonymous connection.
  uint64_t features = session->con_features ? session->con_features :
    mon->get_quorum_con_features();

  if (first <= session->osd_epoch) {
    dout(10) << __func__ << " " << session->name << " should already have epoch "
	     << session->osd_epoch << dendl;
    first = session->osd_epoch + 1;
  }

  if (first < get_first_committed()) {
    MOSDMap *m = new MOSDMap(osdmap.get_fsid(), features);
    m->oldest_map = get_first_committed();
    m->newest_map = osdmap.get_epoch();

    first = get_first_committed();
    bufferlist bl;
    int err = get_version_full(first, features, bl);
    ceph_assert(err == 0);
    ceph_assert(bl.length());
    dout(20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;
    m->maps[first] = bl;

    if (req) {
      mon->send_reply(req, m);
      session->osd_epoch = first;
      return;
    } else {
      session->con->send_message(m);
      session->osd_epoch = first;
    }
    first++;
  }

  while (first <= osdmap.get_epoch()) {
    epoch_t last = std::min<epoch_t>(first + g_conf()->osd_map_message_max - 1,
				     osdmap.get_epoch());
    MOSDMap *m = build_incremental(first, last, features);

    if (req) {
      // send some maps.  it may not be all of them, but it will get them
      // started.
      mon->send_reply(req, m);
    } else {
      session->con->send_message(m);
      first = last + 1;
    }
    session->osd_epoch = last;
    if (onetime || req)
      break;
  }
}

int OSDMonitor::get_version(version_t ver, bufferlist& bl)
{
  return get_version(ver, mon->get_quorum_con_features(), bl);
}

void OSDMonitor::reencode_incremental_map(bufferlist& bl, uint64_t features)
{
  OSDMap::Incremental inc;
  auto q = bl.cbegin();
  inc.decode(q);
  // always encode with subset of osdmap's canonical features
  uint64_t f = features & inc.encode_features;
  dout(20) << __func__ << " " << inc.epoch << " with features " << f
	   << dendl;
  bl.clear();
  if (inc.fullmap.length()) {
    // embedded full map?
    OSDMap m;
    m.decode(inc.fullmap);
    inc.fullmap.clear();
    m.encode(inc.fullmap, f | CEPH_FEATURE_RESERVED);
  }
  if (inc.crush.length()) {
    // embedded crush map
    CrushWrapper c;
    auto p = inc.crush.cbegin();
    c.decode(p);
    inc.crush.clear();
    c.encode(inc.crush, f);
  }
  inc.encode(bl, f | CEPH_FEATURE_RESERVED);
}

void OSDMonitor::reencode_full_map(bufferlist& bl, uint64_t features)
{
  OSDMap m;
  auto q = bl.cbegin();
  m.decode(q);
  // always encode with subset of osdmap's canonical features
  uint64_t f = features & m.get_encoding_features();
  dout(20) << __func__ << " " << m.get_epoch() << " with features " << f
	   << dendl;
  bl.clear();
  m.encode(bl, f | CEPH_FEATURE_RESERVED);
}

int OSDMonitor::get_version(version_t ver, uint64_t features, bufferlist& bl)
{
  uint64_t significant_features = OSDMap::get_significant_features(features);
  if (inc_osd_cache.lookup({ver, significant_features}, &bl)) {
    return 0;
  }
  int ret = PaxosService::get_version(ver, bl);
  if (ret < 0) {
    return ret;
  }
  // NOTE: this check is imprecise; the OSDMap encoding features may
  // be a subset of the latest mon quorum features, but worst case we
  // reencode once and then cache the (identical) result under both
  // feature masks.
  if (significant_features !=
      OSDMap::get_significant_features(mon->get_quorum_con_features())) {
    reencode_incremental_map(bl, features);
  }
  inc_osd_cache.add({ver, significant_features}, bl);
  return 0;
}

int OSDMonitor::get_inc(version_t ver, OSDMap::Incremental& inc)
{
  bufferlist inc_bl;
  int err = get_version(ver, inc_bl);
  ceph_assert(err == 0);
  ceph_assert(inc_bl.length());

  auto p = inc_bl.cbegin();
  inc.decode(p);
  dout(10) << __func__ << "     "
           << " epoch " << inc.epoch
           << " inc_crc " << inc.inc_crc
           << " full_crc " << inc.full_crc
           << " encode_features " << inc.encode_features << dendl;
  return 0;
}

int OSDMonitor::get_full_from_pinned_map(version_t ver, bufferlist& bl)
{
  dout(10) << __func__ << " ver " << ver << dendl;

  version_t closest_pinned = osdmap_manifest.get_lower_closest_pinned(ver);
  if (closest_pinned == 0) {
    return -ENOENT;
  }
  if (closest_pinned > ver) {
    dout(0) << __func__ << " pinned: " << osdmap_manifest.pinned << dendl;
  }
  ceph_assert(closest_pinned <= ver);

  dout(10) << __func__ << " closest pinned ver " << closest_pinned << dendl;

  // get osdmap incremental maps and apply on top of this one.
  bufferlist osdm_bl;
  bool has_cached_osdmap = false;
  for (version_t v = ver-1; v >= closest_pinned; --v) {
    if (full_osd_cache.lookup({v, mon->get_quorum_con_features()},
                                &osdm_bl)) {
      dout(10) << __func__ << " found map in cache ver " << v << dendl;
      closest_pinned = v;
      has_cached_osdmap = true;
      break;
    }
  }

  if (!has_cached_osdmap) {
    int err = PaxosService::get_version_full(closest_pinned, osdm_bl);
    if (err != 0) {
      derr << __func__ << " closest pinned map ver " << closest_pinned
           << " not available! error: " << cpp_strerror(err) << dendl;
    }
    ceph_assert(err == 0);
  }

  ceph_assert(osdm_bl.length());

  OSDMap osdm;
  osdm.decode(osdm_bl);

  dout(10) << __func__ << " loaded osdmap epoch " << closest_pinned
           << " e" << osdm.epoch
           << " crc " << osdm.get_crc()
           << " -- applying incremental maps." << dendl;

  uint64_t encode_features = 0;
  for (version_t v = closest_pinned + 1; v <= ver; ++v) {
    dout(20) << __func__ << "    applying inc epoch " << v << dendl;

    OSDMap::Incremental inc;
    int err = get_inc(v, inc);
    ceph_assert(err == 0);

    encode_features = inc.encode_features;

    err = osdm.apply_incremental(inc);
    ceph_assert(err == 0);

    // this block performs paranoid checks on map retrieval
    if (g_conf().get_val<bool>("mon_debug_extra_checks") &&
        inc.full_crc != 0) {

      uint64_t f = encode_features;
      if (!f) {
        f = (mon->quorum_con_features ? mon->quorum_con_features : -1);
      }

      // encode osdmap to force calculating crcs
      bufferlist tbl;
      osdm.encode(tbl, f | CEPH_FEATURE_RESERVED);
      // decode osdmap to compare crcs with what's expected by incremental
      OSDMap tosdm;
      tosdm.decode(tbl);

      if (tosdm.get_crc() != inc.full_crc) {
        derr << __func__
             << "    osdmap crc mismatch! (osdmap crc " << tosdm.get_crc()
             << ", expected " << inc.full_crc << ")" << dendl;
        ceph_abort_msg("osdmap crc mismatch");
      }
    }

    // note: we cannot add the recently computed map to the cache, as is,
    // because we have not encoded the map into a bl.
  }

  if (!encode_features) {
    dout(10) << __func__
             << " last incremental map didn't have features;"
             << " defaulting to quorum's or all" << dendl;
    encode_features =
      (mon->quorum_con_features ? mon->quorum_con_features : -1);
  }
  osdm.encode(bl, encode_features | CEPH_FEATURE_RESERVED);

  return 0;
}

int OSDMonitor::get_version_full(version_t ver, bufferlist& bl)
{
  return get_version_full(ver, mon->get_quorum_con_features(), bl);
}

int OSDMonitor::get_version_full(version_t ver, uint64_t features,
				 bufferlist& bl)
{
  uint64_t significant_features = OSDMap::get_significant_features(features);
  if (full_osd_cache.lookup({ver, significant_features}, &bl)) {
    return 0;
  }
  int ret = PaxosService::get_version_full(ver, bl);
  if (ret == -ENOENT) {
    // build map?
    ret = get_full_from_pinned_map(ver, bl);
  }
  if (ret < 0) {
    return ret;
  }
  // NOTE: this check is imprecise; the OSDMap encoding features may
  // be a subset of the latest mon quorum features, but worst case we
  // reencode once and then cache the (identical) result under both
  // feature masks.
  if (significant_features !=
      OSDMap::get_significant_features(mon->get_quorum_con_features())) {
    reencode_full_map(bl, features);
  }
  full_osd_cache.add({ver, significant_features}, bl);
  return 0;
}

epoch_t OSDMonitor::blacklist(const entity_addrvec_t& av, utime_t until)
{
  dout(10) << "blacklist " << av << " until " << until << dendl;
  for (auto a : av.v) {
    if (osdmap.require_osd_release >= ceph_release_t::nautilus) {
      a.set_type(entity_addr_t::TYPE_ANY);
    } else {
      a.set_type(entity_addr_t::TYPE_LEGACY);
    }
    pending_inc.new_blacklist[a] = until;
  }
  return pending_inc.epoch;
}

epoch_t OSDMonitor::blacklist(entity_addr_t a, utime_t until)
{
  if (osdmap.require_osd_release >= ceph_release_t::nautilus) {
    a.set_type(entity_addr_t::TYPE_ANY);
  } else {
    a.set_type(entity_addr_t::TYPE_LEGACY);
  }
  dout(10) << "blacklist " << a << " until " << until << dendl;
  pending_inc.new_blacklist[a] = until;
  return pending_inc.epoch;
}


void OSDMonitor::check_osdmap_subs()
{
  dout(10) << __func__ << dendl;
  if (!osdmap.get_epoch()) {
    return;
  }
  auto osdmap_subs = mon->session_map.subs.find("osdmap");
  if (osdmap_subs == mon->session_map.subs.end()) {
    return;
  }
  auto p = osdmap_subs->second->begin();
  while (!p.end()) {
    auto sub = *p;
    ++p;
    check_osdmap_sub(sub);
  }
}

void OSDMonitor::check_osdmap_sub(Subscription *sub)
{
  dout(10) << __func__ << " " << sub << " next " << sub->next
	   << (sub->onetime ? " (onetime)":" (ongoing)") << dendl;
  if (sub->next <= osdmap.get_epoch()) {
    if (sub->next >= 1)
      send_incremental(sub->next, sub->session, sub->incremental_onetime);
    else
      sub->session->con->send_message(build_latest_full(sub->session->con_features));
    if (sub->onetime)
      mon->session_map.remove_sub(sub);
    else
      sub->next = osdmap.get_epoch() + 1;
  }
}

void OSDMonitor::check_pg_creates_subs()
{
  if (!osdmap.get_num_up_osds()) {
    return;
  }
  ceph_assert(osdmap.get_up_osd_features() & CEPH_FEATURE_MON_STATEFUL_SUB);
  mon->with_session_map([this](const MonSessionMap& session_map) {
      auto pg_creates_subs = session_map.subs.find("osd_pg_creates");
      if (pg_creates_subs == session_map.subs.end()) {
	return;
      }
      for (auto sub : *pg_creates_subs->second) {
	check_pg_creates_sub(sub);
      }
    });
}

void OSDMonitor::check_pg_creates_sub(Subscription *sub)
{
  dout(20) << __func__ << " .. " << sub->session->name << dendl;
  ceph_assert(sub->type == "osd_pg_creates");
  // only send these if the OSD is up.  we will check_subs() when they do
  // come up so they will get the creates then.
  if (sub->session->name.is_osd() &&
      mon->osdmon()->osdmap.is_up(sub->session->name.num())) {
    sub->next = send_pg_creates(sub->session->name.num(),
				sub->session->con.get(),
				sub->next);
  }
}

void OSDMonitor::do_application_enable(int64_t pool_id,
                                       const std::string &app_name,
				       const std::string &app_key,
				       const std::string &app_value)
{
  ceph_assert(paxos->is_plugged() && is_writeable());

  dout(20) << __func__ << ": pool_id=" << pool_id << ", app_name=" << app_name
           << dendl;

  ceph_assert(osdmap.require_osd_release >= ceph_release_t::luminous);

  auto pp = osdmap.get_pg_pool(pool_id);
  ceph_assert(pp != nullptr);

  pg_pool_t p = *pp;
  if (pending_inc.new_pools.count(pool_id)) {
    p = pending_inc.new_pools[pool_id];
  }

  if (app_key.empty()) {
    p.application_metadata.insert({app_name, {}});
  } else {
    p.application_metadata.insert({app_name, {{app_key, app_value}}});
  }
  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool_id] = p;
}

void OSDMonitor::do_set_pool_opt(int64_t pool_id,
				 pool_opts_t::key_t opt,
				 pool_opts_t::value_t val)
{
  auto p = pending_inc.new_pools.try_emplace(
    pool_id, *osdmap.get_pg_pool(pool_id));
  p.first->second.opts.set(opt, val);
}

unsigned OSDMonitor::scan_for_creating_pgs(
  const mempool::osdmap::map<int64_t,pg_pool_t>& pools,
  const mempool::osdmap::set<int64_t>& removed_pools,
  utime_t modified,
  creating_pgs_t* creating_pgs) const
{
  unsigned queued = 0;
  for (auto& p : pools) {
    int64_t poolid = p.first;
    if (creating_pgs->created_pools.count(poolid)) {
      dout(10) << __func__ << " already created " << poolid << dendl;
      continue;
    }
    const pg_pool_t& pool = p.second;
    int ruleno = osdmap.crush->find_rule(pool.get_crush_rule(),
					 pool.get_type(), pool.get_size());
    if (ruleno < 0 || !osdmap.crush->rule_exists(ruleno))
      continue;

    const auto last_scan_epoch = creating_pgs->last_scan_epoch;
    const auto created = pool.get_last_change();
    if (last_scan_epoch && created <= last_scan_epoch) {
      dout(10) << __func__ << " no change in pool " << poolid
	       << " " << pool << dendl;
      continue;
    }
    if (removed_pools.count(poolid)) {
      dout(10) << __func__ << " pool is being removed: " << poolid
	       << " " << pool << dendl;
      continue;
    }
    dout(10) << __func__ << " queueing pool create for " << poolid
	     << " " << pool << dendl;
    creating_pgs->create_pool(poolid, pool.get_pg_num(),
			      created, modified);
    queued++;
  }
  return queued;
}

void OSDMonitor::update_creating_pgs()
{
  dout(10) << __func__ << " " << creating_pgs.pgs.size() << " pgs creating, "
	   << creating_pgs.queue.size() << " pools in queue" << dendl;
  decltype(creating_pgs_by_osd_epoch) new_pgs_by_osd_epoch;
  std::lock_guard<std::mutex> l(creating_pgs_lock);
  for (const auto& pg : creating_pgs.pgs) {
    int acting_primary = -1;
    auto pgid = pg.first;
    if (!osdmap.pg_exists(pgid)) {
      dout(20) << __func__ << " ignoring " << pgid << " which should not exist"
	       << dendl;
      continue;
    }
    auto mapped = pg.second.create_epoch;
    dout(20) << __func__ << " looking up " << pgid << "@" << mapped << dendl;
    spg_t spgid(pgid);
    mapping.get_primary_and_shard(pgid, &acting_primary, &spgid);
    // check the previous creating_pgs, look for the target to whom the pg was
    // previously mapped
    for (const auto& pgs_by_epoch : creating_pgs_by_osd_epoch) {
      const auto last_acting_primary = pgs_by_epoch.first;
      for (auto& pgs: pgs_by_epoch.second) {
	if (pgs.second.count(spgid)) {
	  if (last_acting_primary == acting_primary) {
	    mapped = pgs.first;
	  } else {
	    dout(20) << __func__ << " " << pgid << " "
		     << " acting_primary:" << last_acting_primary
		     << " -> " << acting_primary << dendl;
	    // note epoch if the target of the create message changed.
	    mapped = mapping.get_epoch();
          }
          break;
        } else {
	  // newly creating
	  mapped = mapping.get_epoch();
	}
      }
    }
    dout(10) << __func__ << " will instruct osd." << acting_primary
	     << " to create " << pgid << "@" << mapped << dendl;
    new_pgs_by_osd_epoch[acting_primary][mapped].insert(spgid);
  }
  creating_pgs_by_osd_epoch = std::move(new_pgs_by_osd_epoch);
  creating_pgs_epoch = mapping.get_epoch();
}

epoch_t OSDMonitor::send_pg_creates(int osd, Connection *con, epoch_t next) const
{
  dout(30) << __func__ << " osd." << osd << " next=" << next
	   << " " << creating_pgs_by_osd_epoch << dendl;
  std::lock_guard<std::mutex> l(creating_pgs_lock);
  if (creating_pgs_epoch <= creating_pgs.last_scan_epoch) {
    dout(20) << __func__
	     << " not using stale creating_pgs@" << creating_pgs_epoch << dendl;
    // the subscribers will be updated when the mapping is completed anyway
    return next;
  }
  auto creating_pgs_by_epoch = creating_pgs_by_osd_epoch.find(osd);
  if (creating_pgs_by_epoch == creating_pgs_by_osd_epoch.end())
    return next;
  ceph_assert(!creating_pgs_by_epoch->second.empty());

  MOSDPGCreate *oldm = nullptr; // for pre-mimic OSD compat
  MOSDPGCreate2 *m = nullptr;

  bool old = osdmap.require_osd_release < ceph_release_t::nautilus;

  epoch_t last = 0;
  for (auto epoch_pgs = creating_pgs_by_epoch->second.lower_bound(next);
       epoch_pgs != creating_pgs_by_epoch->second.end(); ++epoch_pgs) {
    auto epoch = epoch_pgs->first;
    auto& pgs = epoch_pgs->second;
    dout(20) << __func__ << " osd." << osd << " from " << next
             << " : epoch " << epoch << " " << pgs.size() << " pgs" << dendl;
    last = epoch;
    for (auto& pg : pgs) {
      // Need the create time from the monitor using its clock to set
      // last_scrub_stamp upon pg creation.
      auto create = creating_pgs.pgs.find(pg.pgid);
      ceph_assert(create != creating_pgs.pgs.end());
      if (old) {
	if (!oldm) {
	  oldm = new MOSDPGCreate(creating_pgs_epoch);
	}
	oldm->mkpg.emplace(pg.pgid,
			   pg_create_t{create->second.create_epoch, pg.pgid, 0});
	oldm->ctimes.emplace(pg.pgid, create->second.create_stamp);
      } else {
	if (!m) {
	  m = new MOSDPGCreate2(creating_pgs_epoch);
	}
	m->pgs.emplace(pg, make_pair(create->second.create_epoch,
				     create->second.create_stamp));
	if (create->second.history.epoch_created) {
	  dout(20) << __func__ << "   " << pg << " " << create->second.history
		   << " " << create->second.past_intervals << dendl;
	  m->pg_extra.emplace(pg, make_pair(create->second.history,
					    create->second.past_intervals));
	}
      }
      dout(20) << __func__ << " will create " << pg
	       << " at " << create->second.create_epoch << dendl;
    }
  }
  if (m) {
    con->send_message(m);
  } else if (oldm) {
    con->send_message(oldm);
  } else {
    dout(20) << __func__ << " osd." << osd << " from " << next
             << " has nothing to send" << dendl;
    return next;
  }

  // sub is current through last + 1
  return last + 1;
}

// TICK


void OSDMonitor::tick()
{
  if (!is_active()) return;

  dout(10) << osdmap << dendl;

  // always update osdmap manifest, regardless of being the leader.
  load_osdmap_manifest();

  if (!mon->is_leader()) return;

  bool do_propose = false;
  utime_t now = ceph_clock_now();

  if (handle_osd_timeouts(now, last_osd_report)) {
    do_propose = true;
  }

  // mark osds down?
  if (check_failures(now)) {
    do_propose = true;
  }

  // Force a proposal if we need to prune; pruning is performed on
  // ``encode_pending()``, hence why we need to regularly trigger a proposal
  // even if there's nothing going on.
  if (is_prune_enabled() && should_prune()) {
    do_propose = true;
  }

  // mark down osds out?

  /* can_mark_out() checks if we can mark osds as being out. The -1 has no
   * influence at all. The decision is made based on the ratio of "in" osds,
   * and the function returns false if this ratio is lower that the minimum
   * ratio set by g_conf()->mon_osd_min_in_ratio. So it's not really up to us.
   */
  if (can_mark_out(-1)) {
    string down_out_subtree_limit = g_conf().get_val<string>(
      "mon_osd_down_out_subtree_limit");
    set<int> down_cache;  // quick cache of down subtrees

    map<int,utime_t>::iterator i = down_pending_out.begin();
    while (i != down_pending_out.end()) {
      int o = i->first;
      utime_t down = now;
      down -= i->second;
      ++i;

      if (osdmap.is_down(o) &&
	  osdmap.is_in(o) &&
	  can_mark_out(o)) {
	utime_t orig_grace(g_conf()->mon_osd_down_out_interval, 0);
	utime_t grace = orig_grace;
	double my_grace = 0.0;

	if (g_conf()->mon_osd_adjust_down_out_interval) {
	  // scale grace period the same way we do the heartbeat grace.
	  const osd_xinfo_t& xi = osdmap.get_xinfo(o);
	  double halflife = (double)g_conf()->mon_osd_laggy_halflife;
	  double decay_k = ::log(.5) / halflife;
	  double decay = exp((double)down * decay_k);
	  dout(20) << "osd." << o << " laggy halflife " << halflife << " decay_k " << decay_k
		   << " down for " << down << " decay " << decay << dendl;
	  my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
	  grace += my_grace;
	}

	// is this an entire large subtree down?
	if (down_out_subtree_limit.length()) {
	  int type = osdmap.crush->get_type_id(down_out_subtree_limit);
	  if (type > 0) {
	    if (osdmap.containing_subtree_is_down(cct, o, type, &down_cache)) {
	      dout(10) << "tick entire containing " << down_out_subtree_limit
		       << " subtree for osd." << o
		       << " is down; resetting timer" << dendl;
	      // reset timer, too.
	      down_pending_out[o] = now;
	      continue;
	    }
	  }
	}

        bool down_out = !osdmap.is_destroyed(o) &&
          g_conf()->mon_osd_down_out_interval > 0 && down.sec() >= grace;
        bool destroyed_out = osdmap.is_destroyed(o) &&
          g_conf()->mon_osd_destroyed_out_interval > 0 &&
        // this is not precise enough as we did not make a note when this osd
        // was marked as destroyed, but let's not bother with that
        // complexity for now.
          down.sec() >= g_conf()->mon_osd_destroyed_out_interval;
        if (down_out || destroyed_out) {
	  dout(10) << "tick marking osd." << o << " OUT after " << down
		   << " sec (target " << grace << " = " << orig_grace << " + " << my_grace << ")" << dendl;
	  pending_inc.new_weight[o] = CEPH_OSD_OUT;

	  // set the AUTOOUT bit.
	  if (pending_inc.new_state.count(o) == 0)
	    pending_inc.new_state[o] = 0;
	  pending_inc.new_state[o] |= CEPH_OSD_AUTOOUT;

	  // remember previous weight
	  if (pending_inc.new_xinfo.count(o) == 0)
	    pending_inc.new_xinfo[o] = osdmap.osd_xinfo[o];
	  pending_inc.new_xinfo[o].old_weight = osdmap.osd_weight[o];

	  do_propose = true;

	  mon->clog->info() << "Marking osd." << o << " out (has been down for "
                            << int(down.sec()) << " seconds)";
	} else
	  continue;
      }

      down_pending_out.erase(o);
    }
  } else {
    dout(10) << "tick NOOUT flag set, not checking down osds" << dendl;
  }

  // expire blacklisted items?
  for (ceph::unordered_map<entity_addr_t,utime_t>::iterator p = osdmap.blacklist.begin();
       p != osdmap.blacklist.end();
       ++p) {
    if (p->second < now) {
      dout(10) << "expiring blacklist item " << p->first << " expired " << p->second << " < now " << now << dendl;
      pending_inc.old_blacklist.push_back(p->first);
      do_propose = true;
    }
  }

  if (try_prune_purged_snaps()) {
    do_propose = true;
  }

  if (update_pools_status())
    do_propose = true;

  if (do_propose ||
      !pending_inc.new_pg_temp.empty())  // also propose if we adjusted pg_temp
    propose_pending();
}

bool OSDMonitor::handle_osd_timeouts(const utime_t &now,
				     std::map<int,utime_t> &last_osd_report)
{
  utime_t timeo(g_conf()->mon_osd_report_timeout, 0);
  if (now - mon->get_leader_since() < timeo) {
    // We haven't been the leader for long enough to consider OSD timeouts
    return false;
  }

  int max_osd = osdmap.get_max_osd();
  bool new_down = false;

  for (int i=0; i < max_osd; ++i) {
    dout(30) << __func__ << ": checking up on osd " << i << dendl;
    if (!osdmap.exists(i)) {
      last_osd_report.erase(i); // if any
      continue;
    }
    if (!osdmap.is_up(i))
      continue;
    const std::map<int,utime_t>::const_iterator t = last_osd_report.find(i);
    if (t == last_osd_report.end()) {
      // it wasn't in the map; start the timer.
      last_osd_report[i] = now;
    } else if (can_mark_down(i)) {
      utime_t diff = now - t->second;
      if (diff > timeo) {
	mon->clog->info() << "osd." << i << " marked down after no beacon for "
			  << diff << " seconds";
	derr << "no beacon from osd." << i << " since " << t->second
	     << ", " << diff << " seconds ago.  marking down" << dendl;
	pending_inc.new_state[i] = CEPH_OSD_UP;
	new_down = true;
      }
    }
  }
  return new_down;
}

void OSDMonitor::dump_info(Formatter *f)
{
  f->open_object_section("osdmap");
  osdmap.dump(f);
  f->close_section();

  f->open_array_section("osd_metadata");
  for (int i=0; i<osdmap.get_max_osd(); ++i) {
    if (osdmap.exists(i)) {
      f->open_object_section("osd");
      f->dump_unsigned("id", i);
      dump_osd_metadata(i, f, NULL);
      f->close_section();
    }
  }
  f->close_section();

  f->dump_unsigned("osdmap_first_committed", get_first_committed());
  f->dump_unsigned("osdmap_last_committed", get_last_committed());

  f->open_object_section("crushmap");
  osdmap.crush->dump(f);
  f->close_section();

  if (has_osdmap_manifest) {
    f->open_object_section("osdmap_manifest");
    osdmap_manifest.dump(f);
    f->close_section();
  }
}


bool OSDMonitor::preprocess_command(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = 0;
  bufferlist rdata;
  stringstream ss, ds;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  MonSession *session = op->get_session();
  if (!session) {
    derr << __func__ << " no session" << dendl;
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cct, cmdmap, "prefix", prefix);

  string format;
  cmd_getval(cct, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "pg map") {
    pg_t pgid;
    string pgidstr;
    cmd_getval(cct, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      r = -EINVAL;
      goto reply;
    }
    vector<int> up, acting;
    if (!osdmap.have_pg_pool(pgid.pool())) {
      ss << "pg '" << pgidstr << "' does not exist";
      r = -ENOENT;
      goto reply;
    }
    pg_t mpgid = osdmap.raw_pg_to_pg(pgid);
    osdmap.pg_to_up_acting_osds(pgid, up, acting);
    if (f) {
      f->open_object_section("pg_map");
      f->dump_unsigned("epoch", osdmap.get_epoch());
      f->dump_stream("raw_pgid") << pgid;
      f->dump_stream("pgid") << mpgid;
      f->open_array_section("up");
      for (auto osd : up) {
	f->dump_int("up_osd", osd);
      }
      f->close_section();
      f->open_array_section("acting");
      for (auto osd : acting) {
	f->dump_int("acting_osd", osd);
      }
      f->close_section();
      f->close_section();
      f->flush(rdata);
    } else {
      ds << "osdmap e" << osdmap.get_epoch()
         << " pg " << pgid << " (" << mpgid << ")"
         << " -> up " << up << " acting " << acting;
      rdata.append(ds);
    }
    goto reply;

  } else if (prefix == "osd blacklist ls") {
    if (f)
      f->open_array_section("blacklist");

    for (ceph::unordered_map<entity_addr_t,utime_t>::iterator p = osdmap.blacklist.begin();
	 p != osdmap.blacklist.end();
	 ++p) {
      if (f) {
	f->open_object_section("entry");
	f->dump_string("addr", p->first.get_legacy_str());
	f->dump_stream("until") << p->second;
	f->close_section();
      } else {
	stringstream ss;
	string s;
	ss << p->first << " " << p->second;
	getline(ss, s);
	s += "\n";
	rdata.append(s);
      }
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    }
    ss << "listed " << osdmap.blacklist.size() << " entries";

  } else if (prefix == "osd erasure-code-profile ls") {
    const auto &profiles = osdmap.get_erasure_code_profiles();
    if (f)
      f->open_array_section("erasure-code-profiles");
    for (auto i = profiles.begin(); i != profiles.end(); ++i) {
      if (f)
        f->dump_string("profile", i->first.c_str());
      else
	rdata.append(i->first + "\n");
    }
    if (f) {
      f->close_section();
      ostringstream rs;
      f->flush(rs);
      rs << "\n";
      rdata.append(rs.str());
    }
  } else if (prefix == "osd erasure-code-profile get") {
    string name;
    cmd_getval(cct, cmdmap, "name", name);
    if (!osdmap.has_erasure_code_profile(name)) {
      ss << "unknown erasure code profile '" << name << "'";
      r = -ENOENT;
      goto reply;
    }
    const map<string,string> &profile = osdmap.get_erasure_code_profile(name);
    if (f)
      f->open_object_section("profile");
    for (map<string,string>::const_iterator i = profile.begin();
	 i != profile.end();
	 ++i) {
      if (f)
        f->dump_string(i->first.c_str(), i->second.c_str());
      else
	rdata.append(i->first + "=" + i->second + "\n");
    }
    if (f) {
      f->close_section();
      ostringstream rs;
      f->flush(rs);
      rs << "\n";
      rdata.append(rs.str());
    }
  } else if (boost::starts_with(prefix, "osd pool")) {
    return preprocess_pool_command(op);

  } else if (boost::starts_with(prefix, "osd crush")) {
    return preprocess_crush_command(op);

  } else if (boost::starts_with(prefix, "osd")) {
    return preprocess_osd_command(op);

  } else {
    // try prepare update
    return false;
  }

 reply:
  string rs;
  getline(ss, rs);
  mon->reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

string OSDMonitor::make_removed_snap_epoch_key(int64_t pool, epoch_t epoch)
{
  char k[80];
  snprintf(k, sizeof(k), "removed_epoch_%llu_%08lx",
	   (unsigned long long)pool, (unsigned long)epoch);
  return k;
}

string OSDMonitor::make_purged_snap_epoch_key(epoch_t epoch)
{
  char k[80];
  snprintf(k, sizeof(k), "purged_epoch_%08lx", (unsigned long)epoch);
  return k;
}

string OSDMonitor::_make_snap_key(bool purged, int64_t pool, snapid_t snap)
{
  char k[80];
  snprintf(k, sizeof(k), "%s_snap_%llu_%016llx",
	   purged ? "purged" : "removed",
	   (unsigned long long)pool, (unsigned long long)snap);
  return k;
}

string OSDMonitor::_make_snap_key_value(
  bool purged, int64_t pool, snapid_t snap, snapid_t num,
  epoch_t epoch, bufferlist *v)
{
  // encode the *last* epoch in the key so that we can use forward
  // iteration only to search for an epoch in an interval.
  encode(snap, *v);
  encode(snap + num, *v);
  encode(epoch, *v);
  return _make_snap_key(purged, pool, snap + num - 1);
}


int OSDMonitor::_lookup_snap(bool purged,
			     int64_t pool, snapid_t snap,
			     snapid_t *begin, snapid_t *end)
{
  string k = _make_snap_key(purged, pool, snap);
  auto it = mon->store->get_iterator(OSD_SNAP_PREFIX);
  it->lower_bound(k);
  if (!it->valid()) {
    dout(20) << __func__ << (purged ? " (purged)" : " (removed)")
	     << " pool " << pool << " snap " << snap
	     << " - key '" << k << "' not found" << dendl;
    return -ENOENT;
  }
  if ((purged && it->key().find("purged_snap_") != 0) ||
      (!purged && it->key().find("removed_snap_") != 0)) {
    dout(20) << __func__ << (purged ? " (purged)" : " (removed)")
	     << " pool " << pool << " snap " << snap
	     << " - key '" << k << "' got '" << it->key()
	     << "', wrong prefix" << dendl;
    return -ENOENT;
  }
  string gotk = it->key();
  const char *format;
  if (purged) {
    format = "purged_snap_%llu_";
  } else {
    format = "removed_snap_%llu_";
  }
  long long int keypool;
  int n = sscanf(gotk.c_str(), format, &keypool);
  if (n != 1) {
    derr << __func__ << " invalid k '" << gotk << "'" << dendl;
    return -ENOENT;
  }
  if (pool != keypool) {
    dout(20) << __func__ << (purged ? " (purged)" : " (removed)")
	     << " pool " << pool << " snap " << snap
	     << " - key '" << k << "' got '" << gotk
	     << "', wrong pool " << keypool
	     << dendl;
    return -ENOENT;
  }
  bufferlist v = it->value();
  auto p = v.cbegin();
  decode(*begin, p);
  decode(*end, p);
  if (snap < *begin || snap >= *end) {
    dout(20) << __func__ << (purged ? " (purged)" : " (removed)")
	     << " pool " << pool << " snap " << snap
	     << " - found [" << *begin << "," << *end << "), no overlap"
	     << dendl;
    return -ENOENT;
  }
  return 0;
}

void OSDMonitor::insert_snap_update(
  bool purged,
  int64_t pool,
  snapid_t start, snapid_t end,
  epoch_t epoch,
  MonitorDBStore::TransactionRef t)
{
  snapid_t before_begin, before_end;
  snapid_t after_begin, after_end;
  int b = _lookup_snap(purged, pool, start - 1,
		      &before_begin, &before_end);
  int a = _lookup_snap(purged, pool, end,
		       &after_begin, &after_end);
  if (!b && !a) {
    dout(10) << __func__ << (purged ? " (purged)" : " (removed)")
	     << " [" << start << "," << end << ") - joins ["
	     << before_begin << "," << before_end << ") and ["
	     << after_begin << "," << after_end << ")" << dendl;
    // erase only the begin record; we'll overwrite the end one.
    t->erase(OSD_SNAP_PREFIX, _make_snap_key(purged, pool, before_end - 1));
    bufferlist v;
    string k = _make_snap_key_value(purged, pool,
				    before_begin, after_end - before_begin,
				    pending_inc.epoch, &v);
    t->put(OSD_SNAP_PREFIX, k, v);
  } else if (!b) {
    dout(10) << __func__ << (purged ? " (purged)" : " (removed)")
	     << " [" << start << "," << end << ") - join with earlier ["
	     << before_begin << "," << before_end << ")" << dendl;
    t->erase(OSD_SNAP_PREFIX, _make_snap_key(purged, pool, before_end - 1));
    bufferlist v;
    string k = _make_snap_key_value(purged, pool,
				    before_begin, end - before_begin,
				    pending_inc.epoch, &v);
    t->put(OSD_SNAP_PREFIX, k, v);
  } else if (!a) {
    dout(10) << __func__ << (purged ? " (purged)" : " (removed)")
	     << " [" << start << "," << end << ") - join with later ["
	     << after_begin << "," << after_end << ")" << dendl;
    // overwrite after record
    bufferlist v;
    string k = _make_snap_key_value(purged, pool,
				    start, after_end - start,
				    pending_inc.epoch, &v);
    t->put(OSD_SNAP_PREFIX, k, v);
  } else {
    dout(10) << __func__ << (purged ? " (purged)" : " (removed)")
	     << " [" << start << "," << end << ") - new"
	     << dendl;
    bufferlist v;
    string k = _make_snap_key_value(purged, pool,
				    start, end - start,
				    pending_inc.epoch, &v);
    t->put(OSD_SNAP_PREFIX, k, v);
  }
}

bool OSDMonitor::try_prune_purged_snaps()
{
  if (!mon->mgrstatmon()->is_readable()) {
    return false;
  }
  if (!pending_inc.new_purged_snaps.empty()) {
    return false;  // we already pruned for this epoch
  }

  unsigned max_prune = cct->_conf.get_val<uint64_t>(
    "mon_max_snap_prune_per_epoch");
  if (!max_prune) {
    max_prune = 100000;
  }
  dout(10) << __func__ << " max_prune " << max_prune << dendl;

  unsigned actually_pruned = 0;
  auto& purged_snaps = mon->mgrstatmon()->get_digest().purged_snaps;
  for (auto& p : osdmap.get_pools()) {
    auto q = purged_snaps.find(p.first);
    if (q == purged_snaps.end()) {
      continue;
    }
    auto& purged = q->second;
    if (purged.empty()) {
      dout(20) << __func__ << " " << p.first << " nothing purged" << dendl;
      continue;
    }
    dout(20) << __func__ << " pool " << p.first << " purged " << purged << dendl;
    snap_interval_set_t to_prune;
    unsigned maybe_pruned = actually_pruned;
    for (auto i = purged.begin(); i != purged.end(); ++i) {
      snapid_t begin = i.get_start();
      auto end = i.get_start() + i.get_len();
      snapid_t pbegin = 0, pend = 0;
      int r = lookup_purged_snap(p.first, begin, &pbegin, &pend);
      if (r == 0) {
	// already purged.
	// be a bit aggressive about backing off here, because the mon may
	// do a lot of work going through this set, and if we know the
	// purged set from the OSDs is at least *partly* stale we may as
	// well wait for it to be fresh.
	dout(20) << __func__ << "  we've already purged " << pbegin
		 << "~" << (pend - pbegin) << dendl;
	break;  // next pool
      }
      if (pbegin && pbegin > begin && pbegin < end) {
	// the tail of [begin,end) is purged; shorten the range
	end = pbegin;
      }
      to_prune.insert(begin, end - begin);
      maybe_pruned += end - begin;
      if (maybe_pruned >= max_prune) {
	break;
      }
    }
    if (!to_prune.empty()) {
      // PGs may still be reporting things as purged that we have already
      // pruned from removed_snaps_queue.
      snap_interval_set_t actual;
      auto r = osdmap.removed_snaps_queue.find(p.first);
      if (r != osdmap.removed_snaps_queue.end()) {
	actual.intersection_of(to_prune, r->second);
      }
      actually_pruned += actual.size();
      dout(10) << __func__ << " pool " << p.first << " reports pruned " << to_prune
	       << ", actual pruned " << actual << dendl;
      if (!actual.empty()) {
	pending_inc.new_purged_snaps[p.first].swap(actual);
      }
    }
    if (actually_pruned >= max_prune) {
      break;
    }
  }
  dout(10) << __func__ << " actually pruned " << actually_pruned << dendl;
  return !!actually_pruned;
}

void OSDMonitor::check_legacy_ec_plugin(const string& plugin, const string& profile) const
{
  string replacement = "";

  if (plugin == "jerasure_generic" || 
      plugin == "jerasure_sse3" ||
      plugin == "jerasure_sse4" ||
      plugin == "jerasure_neon") {
    replacement = "jerasure";
  } else if (plugin == "shec_generic" ||
	     plugin == "shec_sse3" ||
	     plugin == "shec_sse4" ||
             plugin == "shec_neon") {
    replacement = "shec";
  }

  if (replacement != "") {
    dout(0) << "WARNING: erasure coding profile " << profile << " uses plugin "
	    << plugin << " that has been deprecated. Please use " 
	    << replacement << " instead." << dendl;
  }
}

int OSDMonitor::normalize_profile(const string& profilename,
				  ErasureCodeProfile &profile,
				  bool force,
				  ostream *ss)
{
  ErasureCodeInterfaceRef erasure_code;
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeProfile::const_iterator plugin = profile.find("plugin");
  check_legacy_ec_plugin(plugin->second, profilename);
  int err = instance.factory(plugin->second,
			     g_conf().get_val<std::string>("erasure_code_dir"),
			     profile, &erasure_code, ss);
  if (err) {
    return err;
  }

  err = erasure_code->init(profile, ss);
  if (err) {
    return err;
  }

  auto it = profile.find("stripe_unit");
  if (it != profile.end()) {
    string err_str;
    uint32_t stripe_unit = strict_iecstrtoll(it->second.c_str(), &err_str);
    if (!err_str.empty()) {
      *ss << "could not parse stripe_unit '" << it->second
	  << "': " << err_str << std::endl;
      return -EINVAL;
    }
    uint32_t data_chunks = erasure_code->get_data_chunk_count();
    uint32_t chunk_size = erasure_code->get_chunk_size(stripe_unit * data_chunks);
    if (chunk_size != stripe_unit) {
      *ss << "stripe_unit " << stripe_unit << " does not match ec profile "
	  << "alignment. Would be padded to " << chunk_size
	  << std::endl;
      return -EINVAL;
    }
    if ((stripe_unit % 4096) != 0 && !force) {
      *ss << "stripe_unit should be a multiple of 4096 bytes for best performance."
	  << "use --force to override this check" << std::endl;
      return -EINVAL;
    }
  }
  return 0;
}

int OSDMonitor::get_erasure_code(const string &erasure_code_profile,
				 ErasureCodeInterfaceRef *erasure_code,
				 ostream *ss) const
{
  if (pending_inc.has_erasure_code_profile(erasure_code_profile))
    return -EAGAIN;
  ErasureCodeProfile profile =
    osdmap.get_erasure_code_profile(erasure_code_profile);
  ErasureCodeProfile::const_iterator plugin =
    profile.find("plugin");
  if (plugin == profile.end()) {
    *ss << "cannot determine the erasure code plugin"
	<< " because there is no 'plugin' entry in the erasure_code_profile "
	<< profile << std::endl;
    return -EINVAL;
  }
  check_legacy_ec_plugin(plugin->second, erasure_code_profile);
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.factory(plugin->second,
			  g_conf().get_val<std::string>("erasure_code_dir"),
			  profile, erasure_code, ss);
}

int OSDMonitor::check_cluster_features(uint64_t features,
				       stringstream &ss)
{
  stringstream unsupported_ss;
  int unsupported_count = 0;
  if ((mon->get_quorum_con_features() & features) != features) {
    unsupported_ss << "the monitor cluster";
    ++unsupported_count;
  }

  set<int32_t> up_osds;
  osdmap.get_up_osds(up_osds);
  for (set<int32_t>::iterator it = up_osds.begin();
       it != up_osds.end(); ++it) {
    const osd_xinfo_t &xi = osdmap.get_xinfo(*it);
    if ((xi.features & features) != features) {
      if (unsupported_count > 0)
	unsupported_ss << ", ";
      unsupported_ss << "osd." << *it;
      unsupported_count ++;
    }
  }

  if (unsupported_count > 0) {
    ss << "features " << features << " unsupported by: "
       << unsupported_ss.str();
    return -ENOTSUP;
  }

  // check pending osd state, too!
  for (map<int32_t,osd_xinfo_t>::const_iterator p =
	 pending_inc.new_xinfo.begin();
       p != pending_inc.new_xinfo.end(); ++p) {
    const osd_xinfo_t &xi = p->second;
    if ((xi.features & features) != features) {
      dout(10) << __func__ << " pending osd." << p->first
	       << " features are insufficient; retry" << dendl;
      return -EAGAIN;
    }
  }

  return 0;
}

bool OSDMonitor::erasure_code_profile_in_use(
  const mempool::osdmap::map<int64_t, pg_pool_t> &pools,
  const string &profile,
  ostream *ss)
{
  bool found = false;
  for (map<int64_t, pg_pool_t>::const_iterator p = pools.begin();
       p != pools.end();
       ++p) {
    if (p->second.erasure_code_profile == profile && p->second.is_erasure()) {
      *ss << osdmap.pool_name[p->first] << " ";
      found = true;
    }
  }
  if (found) {
    *ss << "pool(s) are using the erasure code profile '" << profile << "'";
  }
  return found;
}

int OSDMonitor::parse_erasure_code_profile(const vector<string> &erasure_code_profile,
					   map<string,string> *erasure_code_profile_map,
					   ostream *ss)
{
  int r = g_conf().with_val<string>("osd_pool_default_erasure_code_profile",
				   get_json_str_map,
				   *ss,
				   erasure_code_profile_map,
				   true);
  if (r)
    return r;
  ceph_assert((*erasure_code_profile_map).count("plugin"));
  string default_plugin = (*erasure_code_profile_map)["plugin"];
  map<string,string> user_map;
  for (vector<string>::const_iterator i = erasure_code_profile.begin();
       i != erasure_code_profile.end();
       ++i) {
    size_t equal = i->find('=');
    if (equal == string::npos) {
      user_map[*i] = string();
      (*erasure_code_profile_map)[*i] = string();
    } else {
      const string key = i->substr(0, equal);
      equal++;
      const string value = i->substr(equal);
      if (key.find("ruleset-") == 0) {
	*ss << "property '" << key << "' is no longer supported; try "
	    << "'crush-" << key.substr(8) << "' instead";
	return -EINVAL;
      }
      user_map[key] = value;
      (*erasure_code_profile_map)[key] = value;
    }
  }

  if (user_map.count("plugin") && user_map["plugin"] != default_plugin)
    (*erasure_code_profile_map) = user_map;

  return 0;
}

int OSDMonitor::check_pg_num(int64_t pool, int pg_num, int size, ostream *ss)
{
  auto max_pgs_per_osd = g_conf().get_val<uint64_t>("mon_max_pg_per_osd");
  auto num_osds = std::max(osdmap.get_num_in_osds(), 3u);   // assume min cluster size 3
  auto max_pgs = max_pgs_per_osd * num_osds;
  uint64_t projected = 0;
  if (pool < 0) {
    projected += pg_num * size;
  }
  for (const auto& i : osdmap.get_pools()) {
    if (i.first == pool) {
      projected += pg_num * size;
    } else {
      projected += i.second.get_pg_num_target() * i.second.get_size();
    }
  }
  if (projected > max_pgs) {
    if (pool >= 0) {
      *ss << "pool id " << pool;
    }
    *ss << " pg_num " << pg_num << " size " << size
	<< " would mean " << projected
	<< " total pgs, which exceeds max " << max_pgs
	<< " (mon_max_pg_per_osd " << max_pgs_per_osd
	<< " * num_in_osds " << num_osds << ")";
    return -ERANGE;
  }
  return 0;
}

bool OSDMonitor::prepare_set_flag(MonOpRequestRef op, int flag)
{
  op->mark_osdmon_event(__func__);
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags |= flag;
  ss << OSDMap::get_flag_string(flag) << " is set";
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
						    get_last_committed() + 1));
  return true;
}

bool OSDMonitor::prepare_unset_flag(MonOpRequestRef op, int flag)
{
  op->mark_osdmon_event(__func__);
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags &= ~flag;
  ss << OSDMap::get_flag_string(flag) << " is unset";
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
						    get_last_committed() + 1));
  return true;
}

int32_t OSDMonitor::_allocate_osd_id(int32_t* existing_id)
{
  ceph_assert(existing_id);
  *existing_id = -1;

  for (int32_t i = 0; i < osdmap.get_max_osd(); ++i) {
    if (!osdmap.exists(i) &&
        pending_inc.new_up_client.count(i) == 0 &&
        (pending_inc.new_state.count(i) == 0 ||
         (pending_inc.new_state[i] & CEPH_OSD_EXISTS) == 0)) {
      *existing_id = i;
      return -1;
    }
  }

  if (pending_inc.new_max_osd < 0) {
    return osdmap.get_max_osd();
  }
  return pending_inc.new_max_osd;
}

bool OSDMonitor::prepare_command(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  stringstream ss;
  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  MonSession *session = op->get_session();
  if (!session) {
    derr << __func__ << " no session" << dendl;
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  return prepare_command_impl(op, cmdmap);
}


bool OSDMonitor::prepare_command_impl(MonOpRequestRef op,
				      const cmdmap_t& cmdmap)
{
  op->mark_osdmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  bool ret = false;
  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;

  string format;
  cmd_getval(cct, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(cct, cmdmap, "prefix", prefix);

  int64_t osdid;
  string osd_name;
  bool osdid_present = false;
  if (prefix != "osd pg-temp" &&
      prefix != "osd pg-upmap" &&
      prefix != "osd pg-upmap-items") {  // avoid commands with non-int id arg
    osdid_present = cmd_getval(cct, cmdmap, "id", osdid);
  }
  if (osdid_present) {
    ostringstream oss;
    oss << "osd." << osdid;
    osd_name = oss.str();
  }

  // Even if there's a pending state with changes that could affect
  // a command, considering that said state isn't yet committed, we
  // just don't care about those changes if the command currently being
  // handled acts as a no-op against the current committed state.
  // In a nutshell, we assume this command  happens *before*.
  //
  // Let me make this clearer:
  //
  //   - If we have only one client, and that client issues some
  //     operation that would conflict with this operation  but is
  //     still on the pending state, then we would be sure that said
  //     operation wouldn't have returned yet, so the client wouldn't
  //     issue this operation (unless the client didn't wait for the
  //     operation to finish, and that would be the client's own fault).
  //
  //   - If we have more than one client, each client will observe
  //     whatever is the state at the moment of the commit.  So, if we
  //     have two clients, one issuing an unlink and another issuing a
  //     link, and if the link happens while the unlink is still on the
  //     pending state, from the link's point-of-view this is a no-op.
  //     If different clients are issuing conflicting operations and
  //     they care about that, then the clients should make sure they
  //     enforce some kind of concurrency mechanism -- from our
  //     perspective that's what Douglas Adams would call an SEP.
  //
  // This should be used as a general guideline for most commands handled
  // in this function.  Adapt as you see fit, but please bear in mind that
  // this is the expected behavior.
   

  if (prefix == "osd erasure-code-profile rm") {
    string name;
    cmd_getval(cct, cmdmap, "name", name);

    if (erasure_code_profile_in_use(pending_inc.new_pools, name, &ss))
      goto wait;

    if (erasure_code_profile_in_use(osdmap.pools, name, &ss)) {
      err = -EBUSY;
      goto reply;
    }

    if (osdmap.has_erasure_code_profile(name) ||
	pending_inc.new_erasure_code_profiles.count(name)) {
      if (osdmap.has_erasure_code_profile(name)) {
	pending_inc.old_erasure_code_profiles.push_back(name);
      } else {
	dout(20) << "erasure code profile rm " << name << ": creation canceled" << dendl;
	pending_inc.new_erasure_code_profiles.erase(name);
      }

      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
							get_last_committed() + 1));
      return true;
    } else {
      ss << "erasure-code-profile " << name << " does not exist";
      err = 0;
      goto reply;
    }

  } else if (prefix == "osd erasure-code-profile set") {
    string name;
    cmd_getval(cct, cmdmap, "name", name);
    vector<string> profile;
    cmd_getval(cct, cmdmap, "profile", profile);

    bool force = false;
    cmd_getval(cct, cmdmap, "force", force);

    map<string,string> profile_map;
    err = parse_erasure_code_profile(profile, &profile_map, &ss);
    if (err)
      goto reply;
    if (profile_map.find("plugin") == profile_map.end()) {
      ss << "erasure-code-profile " << profile_map
	 << " must contain a plugin entry" << std::endl;
      err = -EINVAL;
      goto reply;
    }
    string plugin = profile_map["plugin"];

    if (pending_inc.has_erasure_code_profile(name)) {
      dout(20) << "erasure code profile " << name << " try again" << dendl;
      goto wait;
    } else {
      err = normalize_profile(name, profile_map, force, &ss);
      if (err)
	goto reply;

      if (osdmap.has_erasure_code_profile(name)) {
	ErasureCodeProfile existing_profile_map =
	  osdmap.get_erasure_code_profile(name);
	err = normalize_profile(name, existing_profile_map, force, &ss);
	if (err)
	  goto reply;

	if (existing_profile_map == profile_map) {
	  err = 0;
	  goto reply;
	}
	if (!force) {
	  err = -EPERM;
	  ss << "will not override erasure code profile " << name
	     << " because the existing profile "
	     << existing_profile_map
	     << " is different from the proposed profile "
	     << profile_map;
	  goto reply;
	}
      }

      dout(20) << "erasure code profile set " << name << "="
	       << profile_map << dendl;
      pending_inc.set_erasure_code_profile(name, profile_map);
    }

    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                                      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd pg-temp") {
    string pgidstr;
    if (!cmd_getval(cct, cmdmap, "pgid", pgidstr)) {
      ss << "unable to parse 'pgid' value '"
         << cmd_vartype_stringify(cmdmap.at("pgid")) << "'";
      err = -EINVAL;
      goto reply;
    }
    pg_t pgid;
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.pg_exists(pgid)) {
      ss << "pg " << pgid << " does not exist";
      err = -ENOENT;
      goto reply;
    }

    if (pending_inc.new_pg_temp.count(pgid)) {
      dout(10) << __func__ << " waiting for pending update on " << pgid << dendl;
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }

    vector<int64_t> id_vec;
    vector<int32_t> new_pg_temp;
    cmd_getval(cct, cmdmap, "id", id_vec);
    if (id_vec.empty())  {
      pending_inc.new_pg_temp[pgid] = mempool::osdmap::vector<int>();
      ss << "done cleaning up pg_temp of " << pgid;
      goto update;
    }
    for (auto osd : id_vec) {
      if (!osdmap.exists(osd)) {
        ss << "osd." << osd << " does not exist";
        err = -ENOENT;
        goto reply;
      }
      new_pg_temp.push_back(osd);
    }

    int pool_min_size = osdmap.get_pg_pool_min_size(pgid);
    if ((int)new_pg_temp.size() < pool_min_size) {
      ss << "num of osds (" << new_pg_temp.size() <<") < pool min size ("
         << pool_min_size << ")";
      err = -EINVAL;
      goto reply;
    }

    int pool_size = osdmap.get_pg_pool_size(pgid);
    if ((int)new_pg_temp.size() > pool_size) {
      ss << "num of osds (" << new_pg_temp.size() <<") > pool size ("
         << pool_size << ")";
      err = -EINVAL;
      goto reply;
    }

    pending_inc.new_pg_temp[pgid] = mempool::osdmap::vector<int>(
      new_pg_temp.begin(), new_pg_temp.end());
    ss << "set " << pgid << " pg_temp mapping to " << new_pg_temp;
    goto update;
  } else if (prefix == "osd primary-temp") {
    string pgidstr;
    if (!cmd_getval(cct, cmdmap, "pgid", pgidstr)) {
      ss << "unable to parse 'pgid' value '"
         << cmd_vartype_stringify(cmdmap.at("pgid")) << "'";
      err = -EINVAL;
      goto reply;
    }
    pg_t pgid;
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.pg_exists(pgid)) {
      ss << "pg " << pgid << " does not exist";
      err = -ENOENT;
      goto reply;
    }

    int64_t osd;
    if (!cmd_getval(cct, cmdmap, "id", osd)) {
      ss << "unable to parse 'id' value '"
         << cmd_vartype_stringify(cmdmap.at("id")) << "'";
      err = -EINVAL;
      goto reply;
    }
    if (osd != -1 && !osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      err = -ENOENT;
      goto reply;
    }

    if (osdmap.require_min_compat_client != ceph_release_t::unknown &&
	osdmap.require_min_compat_client < ceph_release_t::firefly) {
      ss << "require_min_compat_client "
	 << osdmap.require_min_compat_client
	 << " < firefly, which is required for primary-temp";
      err = -EPERM;
      goto reply;
    }

    pending_inc.new_primary_temp[pgid] = osd;
    ss << "set " << pgid << " primary_temp mapping to " << osd;
    goto update;
  } else if (prefix == "pg repeer") {
    pg_t pgid;
    string pgidstr;
    cmd_getval(cct, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.pg_exists(pgid)) {
      ss << "pg '" << pgidstr << "' does not exist";
      err = -ENOENT;
      goto reply;
    }
    vector<int> acting;
    int primary;
    osdmap.pg_to_acting_osds(pgid, &acting, &primary);
    if (primary < 0) {
      err = -EAGAIN;
      ss << "pg currently has no primary";
      goto reply;
    }
    if (acting.size() > 1) {
      // map to just primary; it will map back to what it wants
      pending_inc.new_pg_temp[pgid] = { primary };
    } else {
      // hmm, pick another arbitrary osd to induce a change.  Note
      // that this won't work if there is only one suitable OSD in the cluster.
      int i;
      bool done = false;
      for (i = 0; i < osdmap.get_max_osd(); ++i) {
	if (i == primary || !osdmap.is_up(i) || !osdmap.exists(i)) {
	  continue;
	}
	pending_inc.new_pg_temp[pgid] = { primary, i };
	done = true;
	break;
      }
      if (!done) {
	err = -EAGAIN;
	ss << "not enough up OSDs in the cluster to force repeer";
	goto reply;
      }
    }
    goto update;
  } else if (prefix == "osd pg-upmap" ||
             prefix == "osd rm-pg-upmap" ||
             prefix == "osd pg-upmap-items" ||
             prefix == "osd rm-pg-upmap-items") {
    if (osdmap.require_min_compat_client < ceph_release_t::luminous) {
      ss << "min_compat_client "
	 << osdmap.require_min_compat_client
	 << " < luminous, which is required for pg-upmap. "
         << "Try 'ceph osd set-require-min-compat-client luminous' "
         << "before using the new interface";
      err = -EPERM;
      goto reply;
    }
    err = check_cluster_features(CEPH_FEATUREMASK_OSDMAP_PG_UPMAP, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err < 0)
      goto reply;
    string pgidstr;
    if (!cmd_getval(cct, cmdmap, "pgid", pgidstr)) {
      ss << "unable to parse 'pgid' value '"
         << cmd_vartype_stringify(cmdmap.at("pgid")) << "'";
      err = -EINVAL;
      goto reply;
    }
    pg_t pgid;
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.pg_exists(pgid)) {
      ss << "pg " << pgid << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    if (pending_inc.old_pools.count(pgid.pool())) {
      ss << "pool of " << pgid << " is pending removal";
      err = -ENOENT;
      getline(ss, rs);
      wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, err, rs, get_last_committed() + 1));
      return true;
    }

    enum {
      OP_PG_UPMAP,
      OP_RM_PG_UPMAP,
      OP_PG_UPMAP_ITEMS,
      OP_RM_PG_UPMAP_ITEMS,
    } option;

    if (prefix == "osd pg-upmap") {
      option = OP_PG_UPMAP;
    } else if (prefix == "osd rm-pg-upmap") {
      option = OP_RM_PG_UPMAP;
    } else if (prefix == "osd pg-upmap-items") {
      option = OP_PG_UPMAP_ITEMS;
    } else {
      option = OP_RM_PG_UPMAP_ITEMS;
    }

    // check pending upmap changes
    switch (option) {
    case OP_PG_UPMAP: // fall through
    case OP_RM_PG_UPMAP:
      if (pending_inc.new_pg_upmap.count(pgid) ||
          pending_inc.old_pg_upmap.count(pgid)) {
        dout(10) << __func__ << " waiting for pending update on "
                 << pgid << dendl;
        wait_for_finished_proposal(op, new C_RetryMessage(this, op));
        return true;
      }
      break;

    case OP_PG_UPMAP_ITEMS: // fall through
    case OP_RM_PG_UPMAP_ITEMS:
      if (pending_inc.new_pg_upmap_items.count(pgid) ||
          pending_inc.old_pg_upmap_items.count(pgid)) {
        dout(10) << __func__ << " waiting for pending update on "
                 << pgid << dendl;
        wait_for_finished_proposal(op, new C_RetryMessage(this, op));
        return true;
      }
      break;

    default:
      ceph_abort_msg("invalid option");
    }

    switch (option) {
    case OP_PG_UPMAP:
      {
        vector<int64_t> id_vec;
        if (!cmd_getval(cct, cmdmap, "id", id_vec)) {
          ss << "unable to parse 'id' value(s) '"
             << cmd_vartype_stringify(cmdmap.at("id")) << "'";
          err = -EINVAL;
          goto reply;
        }

        int pool_min_size = osdmap.get_pg_pool_min_size(pgid);
        if ((int)id_vec.size() < pool_min_size) {
          ss << "num of osds (" << id_vec.size() <<") < pool min size ("
             << pool_min_size << ")";
          err = -EINVAL;
          goto reply;
        }

        int pool_size = osdmap.get_pg_pool_size(pgid);
        if ((int)id_vec.size() > pool_size) {
          ss << "num of osds (" << id_vec.size() <<") > pool size ("
             << pool_size << ")";
          err = -EINVAL;
          goto reply;
        }

        vector<int32_t> new_pg_upmap;
        for (auto osd : id_vec) {
          if (osd != CRUSH_ITEM_NONE && !osdmap.exists(osd)) {
            ss << "osd." << osd << " does not exist";
            err = -ENOENT;
            goto reply;
          }
          auto it = std::find(new_pg_upmap.begin(), new_pg_upmap.end(), osd);
          if (it != new_pg_upmap.end()) {
            ss << "osd." << osd << " already exists, ";
            continue;
          }
          new_pg_upmap.push_back(osd);
        }

        if (new_pg_upmap.empty()) {
          ss << "no valid upmap items(pairs) is specified";
          err = -EINVAL;
          goto reply;
        }

        pending_inc.new_pg_upmap[pgid] = mempool::osdmap::vector<int32_t>(
          new_pg_upmap.begin(), new_pg_upmap.end());
        ss << "set " << pgid << " pg_upmap mapping to " << new_pg_upmap;
      }
      break;

    case OP_RM_PG_UPMAP:
      {
        pending_inc.old_pg_upmap.insert(pgid);
        ss << "clear " << pgid << " pg_upmap mapping";
      }
      break;

    case OP_PG_UPMAP_ITEMS:
      {
        vector<int64_t> id_vec;
        if (!cmd_getval(cct, cmdmap, "id", id_vec)) {
          ss << "unable to parse 'id' value(s) '"
             << cmd_vartype_stringify(cmdmap.at("id")) << "'";
          err = -EINVAL;
          goto reply;
        }

        if (id_vec.size() % 2) {
          ss << "you must specify pairs of osd ids to be remapped";
          err = -EINVAL;
          goto reply;
        }

        int pool_size = osdmap.get_pg_pool_size(pgid);
        if ((int)(id_vec.size() / 2) > pool_size) {
          ss << "num of osd pairs (" << id_vec.size() / 2 <<") > pool size ("
             << pool_size << ")";
          err = -EINVAL;
          goto reply;
        }

        vector<pair<int32_t,int32_t>> new_pg_upmap_items;
        ostringstream items;
        items << "[";
        for (auto p = id_vec.begin(); p != id_vec.end(); ++p) {
          int from = *p++;
          int to = *p;
          if (from == to) {
            ss << "from osd." << from << " == to osd." << to << ", ";
            continue;
          }
          if (!osdmap.exists(from)) {
            ss << "osd." << from << " does not exist";
            err = -ENOENT;
            goto reply;
          }
          if (to != CRUSH_ITEM_NONE && !osdmap.exists(to)) {
            ss << "osd." << to << " does not exist";
            err = -ENOENT;
            goto reply;
          }
          pair<int32_t,int32_t> entry = make_pair(from, to);
          auto it = std::find(new_pg_upmap_items.begin(),
            new_pg_upmap_items.end(), entry);
          if (it != new_pg_upmap_items.end()) {
            ss << "osd." << from << " -> osd." << to << " already exists, ";
            continue;
          }
          new_pg_upmap_items.push_back(entry);
          items << from << "->" << to << ",";
        }
        string out(items.str());
        out.resize(out.size() - 1); // drop last ','
        out += "]";

        if (new_pg_upmap_items.empty()) {
          ss << "no valid upmap items(pairs) is specified";
          err = -EINVAL;
          goto reply;
        }

        pending_inc.new_pg_upmap_items[pgid] =
          mempool::osdmap::vector<pair<int32_t,int32_t>>(
          new_pg_upmap_items.begin(), new_pg_upmap_items.end());
        ss << "set " << pgid << " pg_upmap_items mapping to " << out;
      }
      break;

    case OP_RM_PG_UPMAP_ITEMS:
      {
        pending_inc.old_pg_upmap_items.insert(pgid);
        ss << "clear " << pgid << " pg_upmap_items mapping";
      }
      break;

    default:
      ceph_abort_msg("invalid option");
    }

    goto update;
  } else if (prefix == "osd primary-affinity") {
    int64_t id;
    if (!cmd_getval(cct, cmdmap, "id", id)) {
      ss << "invalid osd id value '"
         << cmd_vartype_stringify(cmdmap.at("id")) << "'";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(cct, cmdmap, "weight", w)) {
      ss << "unable to parse 'weight' value '"
	 << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply;
    }
    long ww = (int)((double)CEPH_OSD_MAX_PRIMARY_AFFINITY*w);
    if (ww < 0L) {
      ss << "weight must be >= 0";
      err = -EINVAL;
      goto reply;
    }
    if (osdmap.require_min_compat_client != ceph_release_t::unknown &&
	osdmap.require_min_compat_client < ceph_release_t::firefly) {
      ss << "require_min_compat_client "
	 << osdmap.require_min_compat_client
	 << " < firefly, which is required for primary-affinity";
      err = -EPERM;
      goto reply;
    }
    if (osdmap.exists(id)) {
      pending_inc.new_primary_affinity[id] = ww;
      ss << "set osd." << id << " primary-affinity to " << w << " (" << ios::hex << ww << ios::dec << ")";
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                                get_last_committed() + 1));
      return true;
    } else {
      ss << "osd." << id << " does not exist";
      err = -ENOENT;
      goto reply;
    }
  } else if (prefix == "osd blacklist clear") {
    pending_inc.new_blacklist.clear();
    std::list<std::pair<entity_addr_t,utime_t > > blacklist;
    osdmap.get_blacklist(&blacklist);
    for (const auto &entry : blacklist) {
      pending_inc.old_blacklist.push_back(entry.first);
    }
    ss << " removed all blacklist entries";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                              get_last_committed() + 1));
    return true;
  } else if (prefix == "osd blacklist") {
    string addrstr;
    cmd_getval(cct, cmdmap, "addr", addrstr);
    entity_addr_t addr;
    if (!addr.parse(addrstr.c_str(), 0)) {
      ss << "unable to parse address " << addrstr;
      err = -EINVAL;
      goto reply;
    }
    else {
      if (osdmap.require_osd_release >= ceph_release_t::nautilus) {
	// always blacklist type ANY
	addr.set_type(entity_addr_t::TYPE_ANY);
      } else {
	addr.set_type(entity_addr_t::TYPE_LEGACY);
      }

      string blacklistop;
      cmd_getval(cct, cmdmap, "blacklistop", blacklistop);
      if (blacklistop == "add") {
	utime_t expires = ceph_clock_now();
	double d;
	// default one hour
	cmd_getval(cct, cmdmap, "expire", d,
          g_conf()->mon_osd_blacklist_default_expire);
	expires += d;

	pending_inc.new_blacklist[addr] = expires;

        {
          // cancel any pending un-blacklisting request too
          auto it = std::find(pending_inc.old_blacklist.begin(),
            pending_inc.old_blacklist.end(), addr);
          if (it != pending_inc.old_blacklist.end()) {
            pending_inc.old_blacklist.erase(it);
          }
        }

	ss << "blacklisting " << addr << " until " << expires << " (" << d << " sec)";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      } else if (blacklistop == "rm") {
	if (osdmap.is_blacklisted(addr) ||
	    pending_inc.new_blacklist.count(addr)) {
	  if (osdmap.is_blacklisted(addr))
	    pending_inc.old_blacklist.push_back(addr);
	  else
	    pending_inc.new_blacklist.erase(addr);
	  ss << "un-blacklisting " << addr;
	  getline(ss, rs);
	  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						    get_last_committed() + 1));
	  return true;
	}
	ss << addr << " isn't blacklisted";
	err = 0;
	goto reply;
      }
    }
  } else if (prefix == "osd tier add") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply;
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string tierpoolstr;
    cmd_getval(cct, cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    ceph_assert(tp);

    if (!_check_become_tier(tierpool_id, tp, pool_id, p, &err, &ss)) {
      goto reply;
    }

    // make sure new tier is empty
    string force_nonempty;
    cmd_getval(cct, cmdmap, "force_nonempty", force_nonempty);
    const pool_stat_t *pstats = mon->mgrstatmon()->get_pool_stat(tierpool_id);
    if (pstats && pstats->stats.sum.num_objects != 0 &&
	force_nonempty != "--force-nonempty") {
      ss << "tier pool '" << tierpoolstr << "' is not empty; --force-nonempty to force";
      err = -ENOTEMPTY;
      goto reply;
    }
    if (tp->is_erasure()) {
      ss << "tier pool '" << tierpoolstr
	 << "' is an ec pool, which cannot be a tier";
      err = -ENOTSUP;
      goto reply;
    }
    if ((!tp->removed_snaps.empty() || !tp->snaps.empty()) &&
	((force_nonempty != "--force-nonempty") ||
	 (!g_conf()->mon_debug_unsafe_allow_tier_with_nonempty_snaps))) {
      ss << "tier pool '" << tierpoolstr << "' has snapshot state; it cannot be added as a tier without breaking the pool";
      err = -ENOTEMPTY;
      goto reply;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) || ntp->is_tier()) {
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }
    np->tiers.insert(tierpool_id);
    np->set_snap_epoch(pending_inc.epoch); // tier will update to our snap info
    ntp->tier_of = pool_id;
    ss << "pool '" << tierpoolstr << "' is now (or already was) a tier of '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier remove" ||
             prefix == "osd tier rm") {
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string tierpoolstr;
    cmd_getval(cct, cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    ceph_assert(tp);

    if (!_check_remove_tier(pool_id, p, tp, &err, &ss)) {
      goto reply;
    }

    if (p->tiers.count(tierpool_id) == 0) {
      ss << "pool '" << tierpoolstr << "' is now (or already was) not a tier of '" << poolstr << "'";
      err = 0;
      goto reply;
    }
    if (tp->tier_of != pool_id) {
      ss << "tier pool '" << tierpoolstr << "' is a tier of '"
         << osdmap.get_pool_name(tp->tier_of) << "': "
         // be scary about it; this is an inconsistency and bells must go off
         << "THIS SHOULD NOT HAVE HAPPENED AT ALL";
      err = -EINVAL;
      goto reply;
    }
    if (p->read_tier == tierpool_id) {
      ss << "tier pool '" << tierpoolstr << "' is the overlay for '" << poolstr << "'; please remove-overlay first";
      err = -EBUSY;
      goto reply;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) == 0 ||
	ntp->tier_of != pool_id ||
	np->read_tier == tierpool_id) {
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }
    np->tiers.erase(tierpool_id);
    ntp->clear_tier();
    ss << "pool '" << tierpoolstr << "' is now (or already was) not a tier of '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier set-overlay") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply;
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string overlaypoolstr;
    cmd_getval(cct, cmdmap, "overlaypool", overlaypoolstr);
    int64_t overlaypool_id = osdmap.lookup_pg_pool_name(overlaypoolstr);
    if (overlaypool_id < 0) {
      ss << "unrecognized pool '" << overlaypoolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    const pg_pool_t *overlay_p = osdmap.get_pg_pool(overlaypool_id);
    ceph_assert(overlay_p);
    if (p->tiers.count(overlaypool_id) == 0) {
      ss << "tier pool '" << overlaypoolstr << "' is not a tier of '" << poolstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (p->read_tier == overlaypool_id) {
      err = 0;
      ss << "overlay for '" << poolstr << "' is now (or already was) '" << overlaypoolstr << "'";
      goto reply;
    }
    if (p->has_read_tier()) {
      ss << "pool '" << poolstr << "' has overlay '"
	 << osdmap.get_pool_name(p->read_tier)
	 << "'; please remove-overlay first";
      err = -EINVAL;
      goto reply;
    }

    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    np->read_tier = overlaypool_id;
    np->write_tier = overlaypool_id;
    np->set_last_force_op_resend(pending_inc.epoch);
    pg_pool_t *noverlay_p = pending_inc.get_new_pool(overlaypool_id, overlay_p);
    noverlay_p->set_last_force_op_resend(pending_inc.epoch);
    ss << "overlay for '" << poolstr << "' is now (or already was) '" << overlaypoolstr << "'";
    if (overlay_p->cache_mode == pg_pool_t::CACHEMODE_NONE)
      ss <<" (WARNING: overlay pool cache_mode is still NONE)";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier remove-overlay" ||
             prefix == "osd tier rm-overlay") {
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    if (!p->has_read_tier()) {
      err = 0;
      ss << "there is now (or already was) no overlay for '" << poolstr << "'";
      goto reply;
    }

    if (!_check_remove_tier(pool_id, p, NULL, &err, &ss)) {
      goto reply;
    }

    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    if (np->has_read_tier()) {
      const pg_pool_t *op = osdmap.get_pg_pool(np->read_tier);
      pg_pool_t *nop = pending_inc.get_new_pool(np->read_tier,op);
      nop->set_last_force_op_resend(pending_inc.epoch);
    }
    if (np->has_write_tier()) {
      const pg_pool_t *op = osdmap.get_pg_pool(np->write_tier);
      pg_pool_t *nop = pending_inc.get_new_pool(np->write_tier, op);
      nop->set_last_force_op_resend(pending_inc.epoch);
    }
    np->clear_read_tier();
    np->clear_write_tier();
    np->set_last_force_op_resend(pending_inc.epoch);
    ss << "there is now (or already was) no overlay for '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier cache-mode") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply;
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    if (!p->is_tier()) {
      ss << "pool '" << poolstr << "' is not a tier";
      err = -EINVAL;
      goto reply;
    }
    string modestr;
    cmd_getval(cct, cmdmap, "mode", modestr);
    pg_pool_t::cache_mode_t mode = pg_pool_t::get_cache_mode_from_str(modestr);
    if (mode < 0) {
      ss << "'" << modestr << "' is not a valid cache mode";
      err = -EINVAL;
      goto reply;
    }

    bool sure = false;
    cmd_getval(cct, cmdmap, "yes_i_really_mean_it", sure);

    if (mode == pg_pool_t::CACHEMODE_FORWARD ||
	mode == pg_pool_t::CACHEMODE_READFORWARD) {
      ss << "'" << modestr << "' is no longer a supported cache mode";
      err = -EPERM;
      goto reply;
    }
    if ((mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	 mode != pg_pool_t::CACHEMODE_NONE &&
	 mode != pg_pool_t::CACHEMODE_PROXY &&
	 mode != pg_pool_t::CACHEMODE_READPROXY) &&
	 !sure) {
      ss << "'" << modestr << "' is not a well-supported cache mode and may "
	 << "corrupt your data.  pass --yes-i-really-mean-it to force.";
      err = -EPERM;
      goto reply;
    }

    // pool already has this cache-mode set and there are no pending changes
    if (p->cache_mode == mode &&
	(pending_inc.new_pools.count(pool_id) == 0 ||
	 pending_inc.new_pools[pool_id].cache_mode == p->cache_mode)) {
      ss << "set cache-mode for pool '" << poolstr << "'"
         << " to " << pg_pool_t::get_cache_mode_name(mode);
      err = 0;
      goto reply;
    }

    /* Mode description:
     *
     *  none:       No cache-mode defined
     *  forward:    Forward all reads and writes to base pool [removed]
     *  writeback:  Cache writes, promote reads from base pool
     *  readonly:   Forward writes to base pool
     *  readforward: Writes are in writeback mode, Reads are in forward mode [removed]
     *  proxy:       Proxy all reads and writes to base pool
     *  readproxy:   Writes are in writeback mode, Reads are in proxy mode
     *
     * Hence, these are the allowed transitions:
     *
     *  none -> any
     *  forward -> proxy || readforward || readproxy || writeback || any IF num_objects_dirty == 0
     *  proxy -> readproxy || writeback || any IF num_objects_dirty == 0
     *  readforward -> forward || proxy || readproxy || writeback || any IF num_objects_dirty == 0
     *  readproxy -> proxy || writeback || any IF num_objects_dirty == 0
     *  writeback -> readproxy || proxy
     *  readonly -> any
     */

    // We check if the transition is valid against the current pool mode, as
    // it is the only committed state thus far.  We will blantly squash
    // whatever mode is on the pending state.

    if (p->cache_mode == pg_pool_t::CACHEMODE_WRITEBACK &&
        (mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) {
      ss << "unable to set cache-mode '" << pg_pool_t::get_cache_mode_name(mode)
         << "' on a '" << pg_pool_t::get_cache_mode_name(p->cache_mode)
         << "' pool; only '"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_PROXY)
	 << "','"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_READPROXY)
        << "' allowed.";
      err = -EINVAL;
      goto reply;
    }
    if ((p->cache_mode == pg_pool_t::CACHEMODE_READFORWARD &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_READPROXY &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_PROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_PROXY &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_FORWARD &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READPROXY))) {

      const pool_stat_t* pstats =
        mon->mgrstatmon()->get_pool_stat(pool_id);

      if (pstats && pstats->stats.sum.num_objects_dirty > 0) {
        ss << "unable to set cache-mode '"
           << pg_pool_t::get_cache_mode_name(mode) << "' on pool '" << poolstr
           << "': dirty objects found";
        err = -EBUSY;
        goto reply;
      }
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    np->cache_mode = mode;
    // set this both when moving to and from cache_mode NONE.  this is to
    // capture legacy pools that were set up before this flag existed.
    np->flags |= pg_pool_t::FLAG_INCOMPLETE_CLONES;
    ss << "set cache-mode for pool '" << poolstr
	<< "' to " << pg_pool_t::get_cache_mode_name(mode);
    if (mode == pg_pool_t::CACHEMODE_NONE) {
      const pg_pool_t *base_pool = osdmap.get_pg_pool(np->tier_of);
      ceph_assert(base_pool);
      if (base_pool->read_tier == pool_id ||
	  base_pool->write_tier == pool_id)
	ss <<" (WARNING: pool is still configured as read or write tier)";
    }
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier add-cache") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply;
    string poolstr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string tierpoolstr;
    cmd_getval(cct, cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    ceph_assert(tp);

    if (!_check_become_tier(tierpool_id, tp, pool_id, p, &err, &ss)) {
      goto reply;
    }

    int64_t size = 0;
    if (!cmd_getval(cct, cmdmap, "size", size)) {
      ss << "unable to parse 'size' value '"
         << cmd_vartype_stringify(cmdmap.at("size")) << "'";
      err = -EINVAL;
      goto reply;
    }
    // make sure new tier is empty
    const pool_stat_t *pstats =
      mon->mgrstatmon()->get_pool_stat(tierpool_id);
    if (pstats && pstats->stats.sum.num_objects != 0) {
      ss << "tier pool '" << tierpoolstr << "' is not empty";
      err = -ENOTEMPTY;
      goto reply;
    }
    auto& modestr = g_conf().get_val<string>("osd_tier_default_cache_mode");
    pg_pool_t::cache_mode_t mode = pg_pool_t::get_cache_mode_from_str(modestr);
    if (mode < 0) {
      ss << "osd tier cache default mode '" << modestr << "' is not a valid cache mode";
      err = -EINVAL;
      goto reply;
    }
    HitSet::Params hsp;
    auto& cache_hit_set_type =
      g_conf().get_val<string>("osd_tier_default_cache_hit_set_type");
    if (cache_hit_set_type == "bloom") {
      BloomHitSet::Params *bsp = new BloomHitSet::Params;
      bsp->set_fpp(g_conf().get_val<double>("osd_pool_default_hit_set_bloom_fpp"));
      hsp = HitSet::Params(bsp);
    } else if (cache_hit_set_type == "explicit_hash") {
      hsp = HitSet::Params(new ExplicitHashHitSet::Params);
    } else if (cache_hit_set_type == "explicit_object") {
      hsp = HitSet::Params(new ExplicitObjectHitSet::Params);
    } else {
      ss << "osd tier cache default hit set type '"
	 << cache_hit_set_type << "' is not a known type";
      err = -EINVAL;
      goto reply;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) || ntp->is_tier()) {
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }
    np->tiers.insert(tierpool_id);
    np->read_tier = np->write_tier = tierpool_id;
    np->set_snap_epoch(pending_inc.epoch); // tier will update to our snap info
    np->set_last_force_op_resend(pending_inc.epoch);
    ntp->set_last_force_op_resend(pending_inc.epoch);
    ntp->tier_of = pool_id;
    ntp->cache_mode = mode;
    ntp->hit_set_count = g_conf().get_val<uint64_t>("osd_tier_default_cache_hit_set_count");
    ntp->hit_set_period = g_conf().get_val<uint64_t>("osd_tier_default_cache_hit_set_period");
    ntp->min_read_recency_for_promote = g_conf().get_val<uint64_t>("osd_tier_default_cache_min_read_recency_for_promote");
    ntp->min_write_recency_for_promote = g_conf().get_val<uint64_t>("osd_tier_default_cache_min_write_recency_for_promote");
    ntp->hit_set_grade_decay_rate = g_conf().get_val<uint64_t>("osd_tier_default_cache_hit_set_grade_decay_rate");
    ntp->hit_set_search_last_n = g_conf().get_val<uint64_t>("osd_tier_default_cache_hit_set_search_last_n");
    ntp->hit_set_params = hsp;
    ntp->target_max_bytes = size;
    ss << "pool '" << tierpoolstr << "' is now (or already was) a cache tier of '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd force-create-pg") {
    pg_t pgid;
    string pgidstr;
    cmd_getval(cct, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.pg_exists(pgid)) {
      ss << "pg " << pgid << " should not exist";
      err = -ENOENT;
      goto reply;
    }
    bool sure = false;
    cmd_getval(cct, cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "This command will recreate a lost (as in data lost) PG with data in it, such "
	 << "that the cluster will give up ever trying to recover the lost data.  Do this "
	 << "only if you are certain that all copies of the PG are in fact lost and you are "
	 << "willing to accept that the data is permanently destroyed.  Pass "
	 << "--yes-i-really-mean-it to proceed.";
      err = -EPERM;
      goto reply;
    }
    bool creating_now;
    {
      std::lock_guard<std::mutex> l(creating_pgs_lock);
      auto emplaced = creating_pgs.pgs.emplace(
	pgid,
	creating_pgs_t::pg_create_info(osdmap.get_epoch(),
				       ceph_clock_now()));
      creating_now = emplaced.second;
    }
    if (creating_now) {
      ss << "pg " << pgidstr << " now creating, ok";
      // set the pool's CREATING flag so that (1) the osd won't ignore our
      // create message and (2) we won't propose any future pg_num changes
      // until after the PG has been instantiated.
      if (pending_inc.new_pools.count(pgid.pool()) == 0) {
	pending_inc.new_pools[pgid.pool()] = *osdmap.get_pg_pool(pgid.pool());
      }
      pending_inc.new_pools[pgid.pool()].flags |= pg_pool_t::FLAG_CREATING;
      err = 0;
      goto update;
    } else {
      ss << "pg " << pgid << " already creating";
      err = 0;
      goto reply;
    }
  } else if (boost::starts_with(prefix, "osd pool")) {
    return prepare_pool_command(op, cmdmap);

  } else if (boost::starts_with(prefix, "osd crush") ||
	     prefix == "osd setcrushmap") {
    return prepare_crush_command(op, cmdmap);

  } else if (boost::starts_with(prefix, "osd")) {
    return prepare_osd_command(op, cmdmap);

  } else {
    err = -EINVAL;
  }

 reply:
  getline(ss, rs);
  if (err < 0 && rs.length() == 0)
    rs = cpp_strerror(err);
  mon->reply_command(op, err, rs, rdata, get_last_committed());
  return ret;

 update:
  getline(ss, rs);
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					    get_last_committed() + 1));
  return true;

 wait:
  wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  return true;
}


bool OSDMonitor::_is_removed_snap(int64_t pool, snapid_t snap)
{
  if (!osdmap.have_pg_pool(pool)) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - pool dne" << dendl;
    return true;
  }
  if (osdmap.in_removed_snaps_queue(pool, snap)) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - in osdmap removed_snaps_queue" << dendl;
    return true;
  }
  snapid_t begin, end;
  int r = lookup_purged_snap(pool, snap, &begin, &end);
  if (r == 0) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - purged, [" << begin << "," << end << ")" << dendl;
    return true;
  }
  return false;
}

bool OSDMonitor::_is_pending_removed_snap(int64_t pool, snapid_t snap)
{
  if (pending_inc.old_pools.count(pool)) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - pool pending deletion" << dendl;
    return true;
  }
  if (pending_inc.in_new_removed_snaps(pool, snap)) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - in pending new_removed_snaps" << dendl;
    return true;
  }
  return false;
}

/**
 * Check if it is safe to add a tier to a base pool
 *
 * @return
 * True if the operation should proceed, false if we should abort here
 * (abort doesn't necessarily mean error, could be idempotency)
 */
bool OSDMonitor::_check_become_tier(
    const int64_t tier_pool_id, const pg_pool_t *tier_pool,
    const int64_t base_pool_id, const pg_pool_t *base_pool,
    int *err,
    ostream *ss) const
{
  const std::string &tier_pool_name = osdmap.get_pool_name(tier_pool_id);
  const std::string &base_pool_name = osdmap.get_pool_name(base_pool_id);

  const FSMap &pending_fsmap = mon->mdsmon()->get_pending_fsmap();
  if (pending_fsmap.pool_in_use(tier_pool_id)) {
    *ss << "pool '" << tier_pool_name << "' is in use by CephFS";
    *err = -EBUSY;
    return false;
  }

  if (base_pool->tiers.count(tier_pool_id)) {
    ceph_assert(tier_pool->tier_of == base_pool_id);
    *err = 0;
    *ss << "pool '" << tier_pool_name << "' is now (or already was) a tier of '"
      << base_pool_name << "'";
    return false;
  }

  if (base_pool->is_tier()) {
    *ss << "pool '" << base_pool_name << "' is already a tier of '"
      << osdmap.get_pool_name(base_pool->tier_of) << "', "
      << "multiple tiers are not yet supported.";
    *err = -EINVAL;
    return false;
  }

  if (tier_pool->has_tiers()) {
    *ss << "pool '" << tier_pool_name << "' has following tier(s) already:";
    for (set<uint64_t>::iterator it = tier_pool->tiers.begin();
         it != tier_pool->tiers.end(); ++it)
      *ss << "'" << osdmap.get_pool_name(*it) << "',";
    *ss << " multiple tiers are not yet supported.";
    *err = -EINVAL;
    return false;
  }

  if (tier_pool->is_tier()) {
    *ss << "tier pool '" << tier_pool_name << "' is already a tier of '"
       << osdmap.get_pool_name(tier_pool->tier_of) << "'";
    *err = -EINVAL;
    return false;
  }

  *err = 0;
  return true;
}


/**
 * Check if it is safe to remove a tier from this base pool
 *
 * @return
 * True if the operation should proceed, false if we should abort here
 * (abort doesn't necessarily mean error, could be idempotency)
 */
bool OSDMonitor::_check_remove_tier(
    const int64_t base_pool_id, const pg_pool_t *base_pool,
    const pg_pool_t *tier_pool,
    int *err, ostream *ss) const
{
  const std::string &base_pool_name = osdmap.get_pool_name(base_pool_id);

  // Apply CephFS-specific checks
  const FSMap &pending_fsmap = mon->mdsmon()->get_pending_fsmap();
  if (pending_fsmap.pool_in_use(base_pool_id)) {
    if (base_pool->is_erasure() && !base_pool->allows_ecoverwrites()) {
      // If the underlying pool is erasure coded and does not allow EC
      // overwrites, we can't permit the removal of the replicated tier that
      // CephFS relies on to access it
      *ss << "pool '" << base_pool_name <<
          "' does not allow EC overwrites and is in use by CephFS"
          " via its tier";
      *err = -EBUSY;
      return false;
    }

    if (tier_pool && tier_pool->cache_mode == pg_pool_t::CACHEMODE_WRITEBACK) {
      *ss << "pool '" << base_pool_name << "' is in use by CephFS, and this "
             "tier is still in use as a writeback cache.  Change the cache "
             "mode and flush the cache before removing it";
      *err = -EBUSY;
      return false;
    }
  }

  *err = 0;
  return true;
}

void OSDMonitor::convert_pool_priorities(void)
{
  pool_opts_t::key_t key = pool_opts_t::get_opt_desc("recovery_priority").key;
  int64_t max_prio = 0;
  int64_t min_prio = 0;
  for (const auto &i : osdmap.get_pools()) {
    const auto &pool = i.second;

    if (pool.opts.is_set(key)) {
      int64_t prio = 0;
      pool.opts.get(key, &prio);
      if (prio > max_prio)
	max_prio = prio;
      if (prio < min_prio)
	min_prio = prio;
    }
  }
  if (max_prio <= OSD_POOL_PRIORITY_MAX && min_prio >= OSD_POOL_PRIORITY_MIN) {
    dout(20) << __func__ << " nothing to fix" << dendl;
    return;
  }
  // Current pool priorities exceeds new maximum
  for (const auto &i : osdmap.get_pools()) {
    const auto pool_id = i.first;
    pg_pool_t pool = i.second;

    int64_t prio = 0;
    pool.opts.get(key, &prio);
    int64_t n;

    if (prio > 0 && max_prio > OSD_POOL_PRIORITY_MAX) { // Likely scenario
      // Scaled priority range 0 to OSD_POOL_PRIORITY_MAX
      n = (float)prio / max_prio * OSD_POOL_PRIORITY_MAX;
    } else if (prio < 0 && min_prio < OSD_POOL_PRIORITY_MIN) {
      // Scaled  priority range OSD_POOL_PRIORITY_MIN to 0
      n = (float)prio / min_prio * OSD_POOL_PRIORITY_MIN;
    } else {
      continue;
    }
    if (n == 0) {
      pool.opts.unset(key);
    } else {
      pool.opts.set(key, static_cast<int64_t>(n));
    }
    dout(10) << __func__ << " pool " << pool_id
	     << " recovery_priority adjusted "
	     << prio << " to " << n << dendl;
    pool.last_change = pending_inc.epoch;
    pending_inc.new_pools[pool_id] = pool;
  }
}
