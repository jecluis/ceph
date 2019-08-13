// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 * Copyright (C) 2019 SUSE LLC <contact@suse.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sstream>

#include "common/config.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/cmdparse.h"

#include "include/ceph_assert.h"

#include "mon/Monitor.h"
#include "mon/OSDMonitor.h"

#include "mon/Session.h"

#define dout_subsys ceph_subsys_mon

bool OSDMonitor::preprocess_pg_command(
    MonOpRequestRef op)
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

  } else {
    return false;
  }

 reply:
  string rs;
  getline(ss, rs);
  mon->reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

bool OSDMonitor::prepare_pg_command(MonOpRequestRef op,
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

  if (prefix == "osd pg-temp") {
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
  } else {
    return false;
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
