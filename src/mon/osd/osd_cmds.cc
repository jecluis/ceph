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
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "common/config.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/cmdparse.h"
#include "common/TextTable.h"
#include "common/numa.h"

#include "include/ceph_assert.h"
#include "include/str_map.h"

#include "mon/Monitor.h"
#include "mon/OSDMonitor.h"
#include "mon/AuthMonitor.h"

#include "messages/MMonCommand.h"

#include "json_spirit/json_spirit_reader.h"

#define dout_subsys ceph_subsys_mon


/*
 * support function for 'osd numa-status'
 */
static void dump_cpu_list(Formatter *f, const char *name,
			  const string& strlist)
{
  cpu_set_t cpu_set;
  size_t cpu_set_size;
  if (parse_cpu_set_list(strlist.c_str(), &cpu_set_size, &cpu_set) < 0) {
    return;
  }
  set<int> cpus = cpu_set_to_set(cpu_set_size, &cpu_set);
  f->open_array_section(name);
  for (auto cpu : cpus) {
    f->dump_int("cpu", cpu);
  }
  f->close_section();
}

/*
 * support function for 'osd reweightn'
 */
static int parse_reweights(CephContext *cct,
			   const cmdmap_t& cmdmap,
			   const OSDMap& osdmap,
			   map<int32_t, uint32_t>* weights)
{
  string weights_str;
  if (!cmd_getval(cct, cmdmap, "weights", weights_str)) {
    return -EINVAL;
  }
  std::replace(begin(weights_str), end(weights_str), '\'', '"');
  json_spirit::mValue json_value;
  if (!json_spirit::read(weights_str, json_value)) {
    return -EINVAL;
  }
  if (json_value.type() != json_spirit::obj_type) {
    return -EINVAL;
  }
  const auto obj = json_value.get_obj();
  try {
    for (auto& osd_weight : obj) {
      auto osd_id = std::stoi(osd_weight.first);
      if (!osdmap.exists(osd_id)) {
	return -ENOENT;
      }
      if (osd_weight.second.type() != json_spirit::str_type) {
	return -EINVAL;
      }
      auto weight = std::stoul(osd_weight.second.get_str());
      weights->insert({osd_id, weight});
    }
  } catch (const std::logic_error& e) {
    return -EINVAL;
  }
  return 0;
}

/*
 *
 * osd command handling
 *
 */

bool OSDMonitor::preprocess_osd_command(
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

  if (prefix == "osd stat") {
    osdmap.print_summary(f.get(), ds, "", true);
    if (f)
      f->flush(rdata);
    else
      rdata.append(ds);
  } else if (prefix == "osd dump" ||
	   prefix == "osd tree" ||
	   prefix == "osd tree-from" ||
	   prefix == "osd ls" ||
	   prefix == "osd getmap" ||
	   prefix == "osd getcrushmap" ||
	   prefix == "osd ls-tree" ||
	   prefix == "osd info") {
    string val;

    epoch_t epoch = 0;
    int64_t epochnum;
    cmd_getval(cct, cmdmap, "epoch", epochnum, (int64_t)osdmap.get_epoch());
    epoch = epochnum;
    
    bufferlist osdmap_bl;
    int err = get_version_full(epoch, osdmap_bl);
    if (err == -ENOENT) {
      r = -ENOENT;
      ss << "there is no map for epoch " << epoch;
      goto reply;
    }
    ceph_assert(err == 0);
    ceph_assert(osdmap_bl.length());

    OSDMap *p;
    if (epoch == osdmap.get_epoch()) {
      p = &osdmap;
    } else {
      p = new OSDMap;
      p->decode(osdmap_bl);
    }

    auto sg = make_scope_guard([&] {
      if (p != &osdmap) {
        delete p;
      }
    });

    if (prefix == "osd dump") {
      stringstream ds;
      if (f) {
	f->open_object_section("osdmap");
	p->dump(f.get());
	f->close_section();
	f->flush(ds);
      } else {
	p->print(ds);
      }
      rdata.append(ds);
      if (!f)
	ds << " ";
    } else if (prefix == "osd ls") {
      if (f) {
	f->open_array_section("osds");
	for (int i = 0; i < osdmap.get_max_osd(); i++) {
	  if (osdmap.exists(i)) {
	    f->dump_int("osd", i);
	  }
	}
	f->close_section();
	f->flush(ds);
      } else {
	bool first = true;
	for (int i = 0; i < osdmap.get_max_osd(); i++) {
	  if (osdmap.exists(i)) {
	    if (!first)
	      ds << "\n";
	    first = false;
	    ds << i;
	  }
	}
      }
      rdata.append(ds);
    } else if (prefix == "osd info") {
      int64_t osd_id;
      bool do_single_osd = true;
      if (!cmd_getval(cct, cmdmap, "id", osd_id)) {
	do_single_osd = false;
      }

      if (do_single_osd && !osdmap.exists(osd_id)) {
	ss << "osd." << osd_id << " does not exist";
	r = -EINVAL;
	goto reply;
      }

      if (f) {
	if (do_single_osd) {
	  osdmap.dump_osd(osd_id, f.get());
	} else {
	  osdmap.dump_osds(f.get());
	}
	f->flush(ds);
      } else {
	if (do_single_osd) {
	  osdmap.print_osd(osd_id, ds);
	} else {
	  osdmap.print_osds(ds);
	}
      }
      rdata.append(ds);
    } else if (prefix == "osd tree" || prefix == "osd tree-from") {
      string bucket;
      if (prefix == "osd tree-from") {
        cmd_getval(cct, cmdmap, "bucket", bucket);
        if (!osdmap.crush->name_exists(bucket)) {
          ss << "bucket '" << bucket << "' does not exist";
          r = -ENOENT;
          goto reply;
        }
        int id = osdmap.crush->get_item_id(bucket);
        if (id >= 0) {
          ss << "\"" << bucket << "\" is not a bucket";
          r = -EINVAL;
          goto reply;
        }
      }

      vector<string> states;
      cmd_getval(cct, cmdmap, "states", states);
      unsigned filter = 0;
      for (auto& s : states) {
	if (s == "up") {
	  filter |= OSDMap::DUMP_UP;
	} else if (s == "down") {
	  filter |= OSDMap::DUMP_DOWN;
	} else if (s == "in") {
	  filter |= OSDMap::DUMP_IN;
	} else if (s == "out") {
	  filter |= OSDMap::DUMP_OUT;
	} else if (s == "destroyed") {
	  filter |= OSDMap::DUMP_DESTROYED;
	} else {
	  ss << "unrecognized state '" << s << "'";
	  r = -EINVAL;
	  goto reply;
	}
      }
      if ((filter & (OSDMap::DUMP_IN|OSDMap::DUMP_OUT)) ==
	  (OSDMap::DUMP_IN|OSDMap::DUMP_OUT)) {
        ss << "cannot specify both 'in' and 'out'";
        r = -EINVAL;
        goto reply;
      }
      if (((filter & (OSDMap::DUMP_UP|OSDMap::DUMP_DOWN)) ==
	   (OSDMap::DUMP_UP|OSDMap::DUMP_DOWN)) ||
           ((filter & (OSDMap::DUMP_UP|OSDMap::DUMP_DESTROYED)) ==
           (OSDMap::DUMP_UP|OSDMap::DUMP_DESTROYED)) ||
           ((filter & (OSDMap::DUMP_DOWN|OSDMap::DUMP_DESTROYED)) ==
           (OSDMap::DUMP_DOWN|OSDMap::DUMP_DESTROYED))) {
	ss << "can specify only one of 'up', 'down' and 'destroyed'";
	r = -EINVAL;
	goto reply;
      }
      if (f) {
	f->open_object_section("tree");
	p->print_tree(f.get(), NULL, filter, bucket);
	f->close_section();
	f->flush(ds);
      } else {
	p->print_tree(NULL, &ds, filter, bucket);
      }
      rdata.append(ds);
    } else if (prefix == "osd getmap") {
      rdata.append(osdmap_bl);
      ss << "got osdmap epoch " << p->get_epoch();
    } else if (prefix == "osd getcrushmap") {
      p->crush->encode(rdata, mon->get_quorum_con_features());
      ss << p->get_crush_version();
    } else if (prefix == "osd ls-tree") {
      string bucket_name;
      cmd_getval(cct, cmdmap, "name", bucket_name);
      set<int> osds;
      r = p->get_osds_by_bucket_name(bucket_name, &osds);
      if (r == -ENOENT) {
        ss << "\"" << bucket_name << "\" does not exist";
        goto reply;
      } else if (r < 0) {
        ss << "can not parse bucket name:\"" << bucket_name << "\"";
        goto reply;
      }

      if (f) {
        f->open_array_section("osds");
        for (auto &i : osds) {
          if (osdmap.exists(i)) {
            f->dump_int("osd", i);
          }
        }
        f->close_section();
        f->flush(ds);
      } else {
        bool first = true;
        for (auto &i : osds) {
          if (osdmap.exists(i)) {
            if (!first)
              ds << "\n";
            first = false;
            ds << i;
          }
        }
      }

      rdata.append(ds);
    }
  } else if (prefix == "osd getmaxosd") {
    if (f) {
      f->open_object_section("getmaxosd");
      f->dump_unsigned("epoch", osdmap.get_epoch());
      f->dump_int("max_osd", osdmap.get_max_osd());
      f->close_section();
      f->flush(rdata);
    } else {
      ds << "max_osd = " << osdmap.get_max_osd() << " in epoch " << osdmap.get_epoch();
      rdata.append(ds);
    }
  } else if (prefix == "osd utilization") {
    string out;
    osdmap.summarize_mapping_stats(NULL, NULL, &out, f.get());
    if (f)
      f->flush(rdata);
    else
      rdata.append(out);
    r = 0;
    goto reply;
  } else if (prefix  == "osd find") {
    int64_t osd;
    if (!cmd_getval(cct, cmdmap, "id", osd)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      r = -EINVAL;
      goto reply;
    }
    if (!osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    string format;
    cmd_getval(cct, cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_object_section("osd_location");
    f->dump_int("osd", osd);
    f->dump_object("addrs", osdmap.get_addrs(osd));
    f->dump_stream("osd_fsid") << osdmap.get_uuid(osd);

    // try to identify host, pod/container name, etc.
    map<string,string> m;
    load_metadata(osd, m, nullptr);
    if (auto p = m.find("hostname"); p != m.end()) {
      f->dump_string("host", p->second);
    }
    for (auto& k : {
	"pod_name", "pod_namespace", // set by rook
	"container_name"             // set by ceph-ansible
	}) {
      if (auto p = m.find(k); p != m.end()) {
	f->dump_string(k, p->second);
      }
    }

    // crush is helpful too
    f->open_object_section("crush_location");
    map<string,string> loc = osdmap.crush->get_full_location(osd);
    for (map<string,string>::iterator p = loc.begin(); p != loc.end(); ++p)
      f->dump_string(p->first.c_str(), p->second);
    f->close_section();
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "osd metadata") {
    int64_t osd = -1;
    if (cmd_vartype_stringify(cmdmap["id"]).size() &&
        !cmd_getval(cct, cmdmap, "id", osd)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      r = -EINVAL;
      goto reply;
    }
    if (osd >= 0 && !osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    string format;
    cmd_getval(cct, cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    if (osd >= 0) {
      f->open_object_section("osd_metadata");
      f->dump_unsigned("id", osd);
      r = dump_osd_metadata(osd, f.get(), &ss);
      if (r < 0)
        goto reply;
      f->close_section();
    } else {
      r = 0;
      f->open_array_section("osd_metadata");
      for (int i=0; i<osdmap.get_max_osd(); ++i) {
        if (osdmap.exists(i)) {
          f->open_object_section("osd");
          f->dump_unsigned("id", i);
          r = dump_osd_metadata(i, f.get(), NULL);
          if (r == -EINVAL || r == -ENOENT) {
            // Drop error, continue to get other daemons' metadata
            dout(4) << "No metadata for osd." << i << dendl;
            r = 0;
          } else if (r < 0) {
            // Unexpected error
            goto reply;
          }
          f->close_section();
        }
      }
      f->close_section();
    }
    f->flush(rdata);
  } else if (prefix == "osd versions") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    count_metadata("ceph_version", f.get());
    f->flush(rdata);
    r = 0;
  } else if (prefix == "osd count-metadata") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    string field;
    cmd_getval(cct, cmdmap, "property", field);
    count_metadata(field, f.get());
    f->flush(rdata);
    r = 0;
  } else if (prefix == "osd numa-status") {
    TextTable tbl;
    if (f) {
      f->open_array_section("osds");
    } else {
      tbl.define_column("OSD", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("HOST", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("NETWORK", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("STORAGE", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("AFFINITY", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("CPUS", TextTable::LEFT, TextTable::LEFT);
    }
    for (int i=0; i<osdmap.get_max_osd(); ++i) {
      if (osdmap.exists(i)) {
	map<string,string> m;
	ostringstream err;
	if (load_metadata(i, m, &err) < 0) {
	  continue;
	}
	string host;
	auto p = m.find("hostname");
	if (p != m.end()) {
	  host = p->second;
	}
	if (f) {
	  f->open_object_section("osd");
	  f->dump_int("osd", i);
	  f->dump_string("host", host);
	  for (auto n : { "network_numa_node", "objectstore_numa_node",
		"numa_node" }) {
	    p = m.find(n);
	    if (p != m.end()) {
	      f->dump_int(n, atoi(p->second.c_str()));
	    }
	  }
	  for (auto n : { "network_numa_nodes", "objectstore_numa_nodes" }) {
	    p = m.find(n);
	    if (p != m.end()) {
	      list<string> ls = get_str_list(p->second, ",");
	      f->open_array_section(n);
	      for (auto node : ls) {
		f->dump_int("node", atoi(node.c_str()));
	      }
	      f->close_section();
	    }
	  }
	  for (auto n : { "numa_node_cpus" }) {
	    p = m.find(n);
	    if (p != m.end()) {
	      dump_cpu_list(f.get(), n, p->second);
	    }
	  }
	  f->close_section();
	} else {
	  tbl << i;
	  tbl << host;
	  p = m.find("network_numa_nodes");
	  if (p != m.end()) {
	    tbl << p->second;
	  } else {
	    tbl << "-";
	  }
	  p = m.find("objectstore_numa_nodes");
	  if (p != m.end()) {
	    tbl << p->second;
	  } else {
	    tbl << "-";
	  }
	  p = m.find("numa_node");
	  auto q = m.find("numa_node_cpus");
	  if (p != m.end() && q != m.end()) {
	    tbl << p->second;
	    tbl << q->second;
	  } else {
	    tbl << "-";
	    tbl << "-";
	  }
	  tbl << TextTable::endrow;
	}
      }
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    } else {
      rdata.append(stringify(tbl));
    }
  } else if (prefix == "osd map") {
    string poolstr, objstr, namespacestr;
    cmd_getval(cct, cmdmap, "pool", poolstr);
    cmd_getval(cct, cmdmap, "object", objstr);
    cmd_getval(cct, cmdmap, "nspace", namespacestr);

    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "pool " << poolstr << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    object_locator_t oloc(pool, namespacestr);
    object_t oid(objstr);
    pg_t pgid = osdmap.object_locator_to_pg(oid, oloc);
    pg_t mpgid = osdmap.raw_pg_to_pg(pgid);
    vector<int> up, acting;
    int up_p, acting_p;
    osdmap.pg_to_up_acting_osds(mpgid, &up, &up_p, &acting, &acting_p);

    string fullobjname;
    if (!namespacestr.empty())
      fullobjname = namespacestr + string("/") + oid.name;
    else
      fullobjname = oid.name;
    if (f) {
      f->open_object_section("osd_map");
      f->dump_unsigned("epoch", osdmap.get_epoch());
      f->dump_string("pool", poolstr);
      f->dump_int("pool_id", pool);
      f->dump_stream("objname") << fullobjname;
      f->dump_stream("raw_pgid") << pgid;
      f->dump_stream("pgid") << mpgid;
      f->open_array_section("up");
      for (vector<int>::iterator p = up.begin(); p != up.end(); ++p)
        f->dump_int("osd", *p);
      f->close_section();
      f->dump_int("up_primary", up_p);
      f->open_array_section("acting");
      for (vector<int>::iterator p = acting.begin(); p != acting.end(); ++p)
        f->dump_int("osd", *p);
      f->close_section();
      f->dump_int("acting_primary", acting_p);
      f->close_section(); // osd_map
      f->flush(rdata);
    } else {
      ds << "osdmap e" << osdmap.get_epoch()
        << " pool '" << poolstr << "' (" << pool << ")"
        << " object '" << fullobjname << "' ->"
        << " pg " << pgid << " (" << mpgid << ")"
        << " -> up (" << pg_vector_string(up) << ", p" << up_p << ") acting ("
        << pg_vector_string(acting) << ", p" << acting_p << ")";
      rdata.append(ds);
    }

  } else if (prefix == "osd lspools") {
    if (f)
      f->open_array_section("pools");
    for (map<int64_t, pg_pool_t>::iterator p = osdmap.pools.begin();
	 p != osdmap.pools.end();
	 ++p) {
      if (f) {
	f->open_object_section("pool");
	f->dump_int("poolnum", p->first);
	f->dump_string("poolname", osdmap.pool_name[p->first]);
	f->close_section();
      } else {
	ds << p->first << ' ' << osdmap.pool_name[p->first];
	if (next(p) != osdmap.pools.end()) {
	  ds << '\n';
	}
      }
    }
    if (f) {
      f->close_section();
      f->flush(ds);
    }
    rdata.append(ds);
  } else if (prefix == "osd get-require-min-compat-client") {
    ss << osdmap.require_min_compat_client << std::endl;
    rdata.append(ss.str());
    ss.str("");
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

bool OSDMonitor::prepare_osd_command(
    MonOpRequestRef op,
    const cmdmap_t& cmdmap)
{
  op->mark_osdmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());

  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;
  bool is_err = false;

  string format;
  cmd_getval(cct, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(cct, cmdmap, "prefix", prefix);


  if (prefix == "osd setmaxosd") {
    int64_t newmax;
    if (!cmd_getval(cct, cmdmap, "newmax", newmax)) {
      ss << "unable to parse 'newmax' value '"
         << cmd_vartype_stringify(cmdmap.at("newmax")) << "'";
      err = -EINVAL;
      goto reply;
    }

    if (newmax > g_conf()->mon_max_osd) {
      err = -ERANGE;
      ss << "cannot set max_osd to " << newmax << " which is > conf.mon_max_osd ("
	 << g_conf()->mon_max_osd << ")";
      goto reply;
    }

    // Don't allow shrinking OSD number as this will cause data loss
    // and may cause kernel crashes.
    // Note: setmaxosd sets the maximum OSD number and not the number of OSDs
    if (newmax < osdmap.get_max_osd()) {
      // Check if the OSDs exist between current max and new value.
      // If there are any OSDs exist, then don't allow shrinking number
      // of OSDs.
      for (int i = newmax; i < osdmap.get_max_osd(); i++) {
        if (osdmap.exists(i)) {
          err = -EBUSY;
          ss << "cannot shrink max_osd to " << newmax
             << " because osd." << i << " (and possibly others) still in use";
          goto reply;
        }
      }
    }

    pending_inc.new_max_osd = newmax;
    ss << "set new max_osd = " << pending_inc.new_max_osd;
    goto update;

  } else if (prefix == "osd set-full-ratio" ||
	     prefix == "osd set-backfillfull-ratio" ||
             prefix == "osd set-nearfull-ratio") {
    double n;
    if (!cmd_getval(cct, cmdmap, "ratio", n)) {
      ss << "unable to parse 'ratio' value '"
         << cmd_vartype_stringify(cmdmap.at("ratio")) << "'";
      err = -EINVAL;
      goto reply;
    }
    if (prefix == "osd set-full-ratio")
      pending_inc.new_full_ratio = n;
    else if (prefix == "osd set-backfillfull-ratio")
      pending_inc.new_backfillfull_ratio = n;
    else if (prefix == "osd set-nearfull-ratio")
      pending_inc.new_nearfull_ratio = n;
    ss << prefix << " " << n;
    goto update;

  } else if (prefix == "osd set-require-min-compat-client") {
    string v;
    cmd_getval(cct, cmdmap, "version", v);
    ceph_release_t vno = ceph_release_from_name(v);
    if (!vno) {
      ss << "version " << v << " is not recognized";
      err = -EINVAL;
      goto reply;
    }
    OSDMap newmap;
    newmap.deepish_copy_from(osdmap);
    newmap.apply_incremental(pending_inc);
    newmap.require_min_compat_client = vno;
    auto mvno = newmap.get_min_compat_client();
    if (vno < mvno) {
      ss << "osdmap current utilizes features that require " << mvno
	 << "; cannot set require_min_compat_client below that to " << vno;
      err = -EPERM;
      goto reply;
    }
    bool sure = false;
    cmd_getval(cct, cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      FeatureMap m;
      mon->get_combined_feature_map(&m);
      uint64_t features = ceph_release_features(ceph::to_integer<int>(vno));
      bool first = true;
      bool ok = true;
      for (int type : {
	    CEPH_ENTITY_TYPE_CLIENT,
	    CEPH_ENTITY_TYPE_MDS,
	    CEPH_ENTITY_TYPE_MGR }) {
	auto p = m.m.find(type);
	if (p == m.m.end()) {
	  continue;
	}
	for (auto& q : p->second) {
	  uint64_t missing = ~q.first & features;
	  if (missing) {
	    if (first) {
	      ss << "cannot set require_min_compat_client to " << v << ": ";
	    } else {
	      ss << "; ";
	    }
	    first = false;
	    ss << q.second << " connected " << ceph_entity_type_name(type)
	       << "(s) look like " << ceph_release_name(
		 ceph_release_from_features(q.first))
	       << " (missing 0x" << std::hex << missing << std::dec << ")";
	    ok = false;
	  }
	}
      }
      if (!ok) {
	ss << "; add --yes-i-really-mean-it to do it anyway";
	err = -EPERM;
	goto reply;
      }
    }
    ss << "set require_min_compat_client to " << vno;
    pending_inc.new_require_min_compat_client = vno;
    goto update;

  } else if (prefix == "osd pause") {
    return prepare_set_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd unpause") {
    return prepare_unset_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd set") {
    bool sure = false;
    cmd_getval(g_ceph_context, cmdmap, "yes_i_really_mean_it", sure);

    string key;
    cmd_getval(cct, cmdmap, "key", key);
    if (key == "full")
      return prepare_set_flag(op, CEPH_OSDMAP_FULL);
    else if (key == "pause")
      return prepare_set_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    else if (key == "noup")
      return prepare_set_flag(op, CEPH_OSDMAP_NOUP);
    else if (key == "nodown")
      return prepare_set_flag(op, CEPH_OSDMAP_NODOWN);
    else if (key == "noout")
      return prepare_set_flag(op, CEPH_OSDMAP_NOOUT);
    else if (key == "noin")
      return prepare_set_flag(op, CEPH_OSDMAP_NOIN);
    else if (key == "nobackfill")
      return prepare_set_flag(op, CEPH_OSDMAP_NOBACKFILL);
    else if (key == "norebalance")
      return prepare_set_flag(op, CEPH_OSDMAP_NOREBALANCE);
    else if (key == "norecover")
      return prepare_set_flag(op, CEPH_OSDMAP_NORECOVER);
    else if (key == "noscrub")
      return prepare_set_flag(op, CEPH_OSDMAP_NOSCRUB);
    else if (key == "nodeep-scrub")
      return prepare_set_flag(op, CEPH_OSDMAP_NODEEP_SCRUB);
    else if (key == "notieragent")
      return prepare_set_flag(op, CEPH_OSDMAP_NOTIERAGENT);
    else if (key == "nosnaptrim")
      return prepare_set_flag(op, CEPH_OSDMAP_NOSNAPTRIM);
    else if (key == "pglog_hardlimit") {
      if (!osdmap.get_num_up_osds() && !sure) {
        ss << "Not advisable to continue since no OSDs are up. Pass "
           << "--yes-i-really-mean-it if you really wish to continue.";
        err = -EPERM;
        goto reply;
      }
      // The release check here is required because for OSD_PGLOG_HARDLIMIT,
      // we are reusing a jewel feature bit that was retired in luminous.
      if (osdmap.require_osd_release >= ceph_release_t::luminous &&
         (HAVE_FEATURE(osdmap.get_up_osd_features(), OSD_PGLOG_HARDLIMIT)
          || sure)) {
	return prepare_set_flag(op, CEPH_OSDMAP_PGLOG_HARDLIMIT);
      } else {
	ss << "not all up OSDs have OSD_PGLOG_HARDLIMIT feature";
	err = -EPERM;
	goto reply;
      }
    } else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd unset") {
    string key;
    cmd_getval(cct, cmdmap, "key", key);
    if (key == "full")
      return prepare_unset_flag(op, CEPH_OSDMAP_FULL);
    else if (key == "pause")
      return prepare_unset_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    else if (key == "noup")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOUP);
    else if (key == "nodown")
      return prepare_unset_flag(op, CEPH_OSDMAP_NODOWN);
    else if (key == "noout")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOOUT);
    else if (key == "noin")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOIN);
    else if (key == "nobackfill")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOBACKFILL);
    else if (key == "norebalance")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOREBALANCE);
    else if (key == "norecover")
      return prepare_unset_flag(op, CEPH_OSDMAP_NORECOVER);
    else if (key == "noscrub")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOSCRUB);
    else if (key == "nodeep-scrub")
      return prepare_unset_flag(op, CEPH_OSDMAP_NODEEP_SCRUB);
    else if (key == "notieragent")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOTIERAGENT);
    else if (key == "nosnaptrim")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOSNAPTRIM);
    else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd require-osd-release") {
    string release;
    cmd_getval(cct, cmdmap, "release", release);
    bool sure = false;
    cmd_getval(cct, cmdmap, "yes_i_really_mean_it", sure);
    ceph_release_t rel = ceph_release_from_name(release.c_str());
    if (!rel) {
      ss << "unrecognized release " << release;
      err = -EINVAL;
      goto reply;
    }
    if (rel == osdmap.require_osd_release) {
      // idempotent
      err = 0;
      goto reply;
    }
    ceph_assert(osdmap.require_osd_release >= ceph_release_t::luminous);
    if (!osdmap.get_num_up_osds() && !sure) {
      ss << "Not advisable to continue since no OSDs are up. Pass "
	 << "--yes-i-really-mean-it if you really wish to continue.";
      err = -EPERM;
      goto reply;
    }
    if (rel == ceph_release_t::mimic) {
      if (!mon->monmap->get_required_features().contains_all(
	    ceph::features::mon::FEATURE_MIMIC)) {
	ss << "not all mons are mimic";
	err = -EPERM;
	goto reply;
      }
      if ((!HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_MIMIC))
           && !sure) {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_MIMIC feature";
	err = -EPERM;
	goto reply;
      }
    } else if (rel == ceph_release_t::nautilus) {
      if (!mon->monmap->get_required_features().contains_all(
	    ceph::features::mon::FEATURE_NAUTILUS)) {
	ss << "not all mons are nautilus";
	err = -EPERM;
	goto reply;
      }
      if ((!HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_NAUTILUS))
           && !sure) {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_NAUTILUS feature";
	err = -EPERM;
	goto reply;
      }
    } else if (rel == ceph_release_t::octopus) {
      if (!mon->monmap->get_required_features().contains_all(
	    ceph::features::mon::FEATURE_OCTOPUS)) {
	ss << "not all mons are octopus";
	err = -EPERM;
	goto reply;
      }
      if ((!HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_OCTOPUS))
           && !sure) {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_OCTOPUS feature";
	err = -EPERM;
	goto reply;
      }
    } else {
      ss << "not supported for this release yet";
      err = -EPERM;
      goto reply;
    }
    if (rel < osdmap.require_osd_release) {
      ss << "require_osd_release cannot be lowered once it has been set";
      err = -EPERM;
      goto reply;
    }
    pending_inc.new_require_osd_release = rel;
    goto update;

  } else if (prefix == "osd down" ||
             prefix == "osd out" ||
             prefix == "osd in" ||
             prefix == "osd rm" ||
             prefix == "osd stop") {

    bool any = false;
    bool stop = false;
    bool verbose = true;
    bool definitely_dead = false;

    vector<string> idvec;
    cmd_getval(cct, cmdmap, "ids", idvec);
    cmd_getval(cct, cmdmap, "definitely_dead", definitely_dead);
    derr << "definitely_dead " << (int)definitely_dead << dendl;
    for (unsigned j = 0; j < idvec.size() && !stop; j++) {
      set<int> osds;

      // wildcard?
      if (j == 0 &&
          (idvec[0] == "any" || idvec[0] == "all" || idvec[0] == "*")) {
        if (prefix == "osd in") {
          // touch out osds only
          osdmap.get_out_existing_osds(osds);
        } else {
          osdmap.get_all_osds(osds);
        }
        stop = true;
        verbose = false; // so the output is less noisy.
      } else {
        long osd = parse_osd_id(idvec[j].c_str(), &ss);
        if (osd < 0) {
          ss << "invalid osd id" << osd;
          err = -EINVAL;
          continue;
        } else if (!osdmap.exists(osd)) {
          ss << "osd." << osd << " does not exist. ";
          continue;
        }

        osds.insert(osd);
      }

      for (auto &osd : osds) {
        if (prefix == "osd down") {
	  if (osdmap.is_down(osd)) {
            if (verbose)
	      ss << "osd." << osd << " is already down. ";
	  } else {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_UP);
	    ss << "marked down osd." << osd << ". ";
	    any = true;
	  }
	  if (definitely_dead) {
	    if (!pending_inc.new_xinfo.count(osd)) {
	      pending_inc.new_xinfo[osd] = osdmap.osd_xinfo[osd];
	    }
	    if (pending_inc.new_xinfo[osd].dead_epoch < pending_inc.epoch) {
	      any = true;
	    }
	    pending_inc.new_xinfo[osd].dead_epoch = pending_inc.epoch;
	  }
        } else if (prefix == "osd out") {
	  if (osdmap.is_out(osd)) {
            if (verbose)
	      ss << "osd." << osd << " is already out. ";
	  } else {
	    pending_inc.new_weight[osd] = CEPH_OSD_OUT;
	    if (osdmap.osd_weight[osd]) {
	      if (pending_inc.new_xinfo.count(osd) == 0) {
	        pending_inc.new_xinfo[osd] = osdmap.osd_xinfo[osd];
	      }
	      pending_inc.new_xinfo[osd].old_weight = osdmap.osd_weight[osd];
	    }
	    ss << "marked out osd." << osd << ". ";
            std::ostringstream msg;
            msg << "Client " << op->get_session()->entity_name
                << " marked osd." << osd << " out";
            if (osdmap.is_up(osd)) {
              msg << ", while it was still marked up";
            } else {
              auto period = ceph_clock_now() - down_pending_out[osd];
              msg << ", after it was down for " << int(period.sec())
                  << " seconds";
            }

            mon->clog->info() << msg.str();
	    any = true;
	  }
        } else if (prefix == "osd in") {
	  if (osdmap.is_in(osd)) {
            if (verbose)
	      ss << "osd." << osd << " is already in. ";
	  } else {
	    if (osdmap.osd_xinfo[osd].old_weight > 0) {
	      pending_inc.new_weight[osd] = osdmap.osd_xinfo[osd].old_weight;
	      if (pending_inc.new_xinfo.count(osd) == 0) {
	        pending_inc.new_xinfo[osd] = osdmap.osd_xinfo[osd];
	      }
	      pending_inc.new_xinfo[osd].old_weight = 0;
	    } else {
	      pending_inc.new_weight[osd] = CEPH_OSD_IN;
	    }
	    ss << "marked in osd." << osd << ". ";
	    any = true;
	  }
        } else if (prefix == "osd rm") {
          err = prepare_command_osd_remove(osd);

          if (err == -EBUSY) {
	    if (any)
	      ss << ", ";
            ss << "osd." << osd << " is still up; must be down before removal. ";
	  } else {
            ceph_assert(err == 0);
	    if (any) {
	      ss << ", osd." << osd;
            } else {
	      ss << "removed osd." << osd;
            }
	    any = true;
	  }
        } else if (prefix == "osd stop") {
          if (osdmap.is_stop(osd)) {
            if (verbose)
              ss << "osd." << osd << " is already stopped. ";
          } else if (osdmap.is_down(osd)) {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_STOP);
            ss << "stop down osd." << osd << ". ";
            any = true;
          } else {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_UP | CEPH_OSD_STOP);
            ss << "stop osd." << osd << ". ";
            any = true;
          }
        }
      }
    }
    if (any) {
      goto update;
    }
  } else if (prefix == "osd set-group" ||
             prefix == "osd unset-group" ||
             prefix == "osd add-noup" ||
             prefix == "osd add-nodown" ||
             prefix == "osd add-noin" ||
             prefix == "osd add-noout" ||
             prefix == "osd rm-noup" ||
             prefix == "osd rm-nodown" ||
             prefix == "osd rm-noin" ||
             prefix == "osd rm-noout") {
    bool do_set = prefix == "osd set-group" ||
                  prefix.find("add") != string::npos;
    string flag_str;
    unsigned flags = 0;
    vector<string> who;
    if (prefix == "osd set-group" || prefix == "osd unset-group") {
      cmd_getval(cct, cmdmap, "flags", flag_str);
      cmd_getval(cct, cmdmap, "who", who);
      vector<string> raw_flags;
      boost::split(raw_flags, flag_str, boost::is_any_of(","));
      for (auto& f : raw_flags) {
        if (f == "noup")
          flags |= CEPH_OSD_NOUP;
        else if (f == "nodown")
          flags |= CEPH_OSD_NODOWN;
        else if (f == "noin")
          flags |= CEPH_OSD_NOIN;
        else if (f == "noout")
          flags |= CEPH_OSD_NOOUT;
        else {
          ss << "unrecognized flag '" << f << "', must be one of "
             << "{noup,nodown,noin,noout}";
          err = -EINVAL;
          goto reply;
        }
      }
    } else {
      cmd_getval(cct, cmdmap, "ids", who);
      if (prefix.find("noup") != string::npos)
        flags = CEPH_OSD_NOUP;
      else if (prefix.find("nodown") != string::npos)
        flags = CEPH_OSD_NODOWN;
      else if (prefix.find("noin") != string::npos)
        flags = CEPH_OSD_NOIN;
      else if (prefix.find("noout") != string::npos)
        flags = CEPH_OSD_NOOUT;
      else
        ceph_assert(0 == "Unreachable!");
    }
    if (flags == 0) {
      ss << "must specify flag(s) {noup,nodwon,noin,noout} to set/unset";
      err = -EINVAL;
      goto reply;
    }
    if (who.empty()) {
      ss << "must specify at least one or more targets to set/unset";
      err = -EINVAL;
      goto reply;
    }
    set<int> osds;
    set<int> crush_nodes;
    set<int> device_classes;
    for (auto& w : who) {
      if (w == "any" || w == "all" || w == "*") {
        osdmap.get_all_osds(osds);
        break;
      }
      std::stringstream ts;
      if (auto osd = parse_osd_id(w.c_str(), &ts); osd >= 0) {
        osds.insert(osd);
      } else if (osdmap.crush->name_exists(w)) {
        crush_nodes.insert(osdmap.crush->get_item_id(w));
      } else if (osdmap.crush->class_exists(w)) {
        device_classes.insert(osdmap.crush->get_class_id(w));
      } else {
        ss << "unable to parse osd id or crush node or device class: "
           << "\"" << w << "\". ";
      }
    }
    if (osds.empty() && crush_nodes.empty() && device_classes.empty()) {
      // ss has reason for failure
      err = -EINVAL;
      goto reply;
    }
    bool any = false;
    for (auto osd : osds) {
      if (!osdmap.exists(osd)) {
        ss << "osd." << osd << " does not exist. ";
        continue;
      }
      if (do_set) {
        if (flags & CEPH_OSD_NOUP) {
          any |= osdmap.is_noup_by_osd(osd) ?
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOUP) :
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOUP);
        }
        if (flags & CEPH_OSD_NODOWN) {
          any |= osdmap.is_nodown_by_osd(osd) ?
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NODOWN) :
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NODOWN);
        }
        if (flags & CEPH_OSD_NOIN) {
          any |= osdmap.is_noin_by_osd(osd) ?
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOIN) :
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOIN);
        }
        if (flags & CEPH_OSD_NOOUT) {
          any |= osdmap.is_noout_by_osd(osd) ?
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOOUT) :
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOOUT);
        }
      } else {
        if (flags & CEPH_OSD_NOUP) {
          any |= osdmap.is_noup_by_osd(osd) ?
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOUP) :
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOUP);
        }
        if (flags & CEPH_OSD_NODOWN) {
          any |= osdmap.is_nodown_by_osd(osd) ?
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NODOWN) :
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NODOWN);
        }
        if (flags & CEPH_OSD_NOIN) {
          any |= osdmap.is_noin_by_osd(osd) ?
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOIN) :
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOIN);
        }
        if (flags & CEPH_OSD_NOOUT) {
          any |= osdmap.is_noout_by_osd(osd) ?
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOOUT) :
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOOUT);
        }
      }
    }
    for (auto& id : crush_nodes) {
      auto old_flags = osdmap.get_crush_node_flags(id);
      auto& pending_flags = pending_inc.new_crush_node_flags[id];
      pending_flags |= old_flags; // adopt existing flags first!
      if (do_set) {
        pending_flags |= flags;
      } else {
        pending_flags &= ~flags;
      }
      any = true;
    }
    for (auto& id : device_classes) {
      auto old_flags = osdmap.get_device_class_flags(id);
      auto& pending_flags = pending_inc.new_device_class_flags[id];
      pending_flags |= old_flags;
      if (do_set) {
        pending_flags |= flags;
      } else {
        pending_flags &= ~flags;
      }
      any = true;
    }
    if (any) {
      goto update;
    }

  } else if (prefix == "osd reweight") {
    int64_t id;
    if (!cmd_getval(cct, cmdmap, "id", id)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap.at("id")) << "'";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(cct, cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
         << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply;
    }
    long ww = (int)((double)CEPH_OSD_IN*w);
    if (ww < 0L) {
      ss << "weight must be >= 0";
      err = -EINVAL;
      goto reply;
    }
    if (osdmap.exists(id)) {
      pending_inc.new_weight[id] = ww;
      ss << "reweighted osd." << id << " to " << w << " (" << std::hex
	 << ww << std::dec << ")";
      goto update;
    } else {
      ss << "osd." << id << " does not exist";
      err = -ENOENT;
      goto reply;
    }
  } else if (prefix == "osd reweightn") {
    map<int32_t, uint32_t> weights;
    err = parse_reweights(cct, cmdmap, osdmap, &weights);
    if (err) {
      ss << "unable to parse 'weights' value '"
         << cmd_vartype_stringify(cmdmap.at("weights")) << "'";
      goto reply;
    }
    pending_inc.new_weight.insert(weights.begin(), weights.end());

    // we can't jump to 'update' because we're doing something different
    wait_for_finished_proposal(
	op,
	new Monitor::C_Command(mon, op, 0, rs, rdata, get_last_committed() + 1));
    return true;
  } else if (prefix == "osd lost") {
    int64_t id;
    if (!cmd_getval(cct, cmdmap, "id", id)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap.at("id")) << "'";
      err = -EINVAL;
      goto reply;
    }
    bool sure = false;
    cmd_getval(g_ceph_context, cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "are you SURE?  this might mean real, permanent data loss.  pass "
	    "--yes-i-really-mean-it if you really do.";
      err = -EPERM;
      goto reply;
    } else if (!osdmap.exists(id)) {
      ss << "osd." << id << " does not exist";
      err = -ENOENT;
      goto reply;
    } else if (!osdmap.is_down(id)) {
      ss << "osd." << id << " is not down";
      err = -EBUSY;
      goto reply;
    } else {
      epoch_t e = osdmap.get_info(id).down_at;
      pending_inc.new_lost[id] = e;
      ss << "marked osd lost in epoch " << e;
      goto update;
    }

  } else if (prefix == "osd destroy-actual" ||
	     prefix == "osd purge-actual" ||
	     prefix == "osd purge-new") {
    /* Destroying an OSD means that we don't expect to further make use of
     * the OSDs data (which may even become unreadable after this operation),
     * and that we are okay with scrubbing all its cephx keys and config-key
     * data (which may include lockbox keys, thus rendering the osd's data
     * unreadable).
     *
     * The OSD will not be removed. Instead, we will mark it as destroyed,
     * such that a subsequent call to `create` will not reuse the osd id.
     * This will play into being able to recreate the OSD, at the same
     * crush location, with minimal data movement.
     */

    // make sure authmon is writeable.
    if (!mon->authmon()->is_writeable()) {
      dout(10) << __func__ << " waiting for auth mon to be writeable for "
               << "osd destroy" << dendl;
      mon->authmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return false;
    }

    int64_t id;
    if (!cmd_getval(cct, cmdmap, "id", id)) {
      auto p = cmdmap.find("id");
      if (p == cmdmap.end()) {
	ss << "no osd id specified";
      } else {
	ss << "unable to parse osd id value '"
	   << cmd_vartype_stringify(cmdmap.at("id")) << "";
      }
      err = -EINVAL;
      goto reply;
    }

    bool is_destroy = (prefix == "osd destroy-actual");
    if (!is_destroy) {
      ceph_assert("osd purge-actual" == prefix ||
	     "osd purge-new" == prefix);
    }

    bool sure = false;
    cmd_getval(g_ceph_context, cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "Are you SURE?  Did you verify with 'ceph osd safe-to-destroy'?  "
	 << "This will mean real, permanent data loss, as well "
         << "as deletion of cephx and lockbox keys. "
	 << "Pass --yes-i-really-mean-it if you really do.";
      err = -EPERM;
      goto reply;
    } else if (!osdmap.exists(id)) {
      ss << "osd." << id << " does not exist";
      err = 0; // idempotent
      goto reply;
    } else if (osdmap.is_up(id)) {
      ss << "osd." << id << " is not `down`.";
      err = -EBUSY;
      goto reply;
    } else if (is_destroy && osdmap.is_destroyed(id)) {
      ss << "destroyed osd." << id;
      err = 0;
      goto reply;
    }

    if (prefix == "osd purge-new" &&
	(osdmap.get_state(id) & CEPH_OSD_NEW) == 0) {
      ss << "osd." << id << " is not new";
      err = -EPERM;
      goto reply;
    }

    bool goto_reply = false;

    paxos->plug();
    if (is_destroy) {
      err = prepare_command_osd_destroy(id, ss);
      // we checked above that it should exist.
      ceph_assert(err != -ENOENT);
    } else {
      err = prepare_command_osd_purge(id, ss);
      if (err == -ENOENT) {
        err = 0;
        ss << "osd." << id << " does not exist.";
        goto_reply = true;
      }
    }
    paxos->unplug();

    if (err < 0 || goto_reply) {
      goto reply;
    }

    if (is_destroy) {
      ss << "destroyed osd." << id;
    } else {
      ss << "purged osd." << id;
    }

    // can't jump to 'update' because we're doing something else before
    // returning.
    getline(ss, rs);
    wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, 0, rs, get_last_committed() + 1));
    force_immediate_propose();
    return true;

  } else if (prefix == "osd new") {

    // make sure authmon is writeable.
    if (!mon->authmon()->is_writeable()) {
      dout(10) << __func__ << " waiting for auth mon to be writeable for "
               << "osd new" << dendl;
      mon->authmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return false;
    }

    map<string,string> param_map;

    bufferlist bl = m->get_data();
    string param_json = bl.to_str();
    dout(20) << __func__ << " osd new json = " << param_json << dendl;

    err = get_json_str_map(param_json, ss, &param_map);
    if (err < 0)
      goto reply;

    dout(20) << __func__ << " osd new params " << param_map << dendl;

    paxos->plug();
    err = prepare_command_osd_new(op, cmdmap, param_map, ss, f.get());
    paxos->unplug();

    if (err < 0) {
      goto reply;
    }

    if (f) {
      f->flush(rdata);
    } else {
      rdata.append(ss);
    }

    if (err == EEXIST) {
      // idempotent operation
      err = 0;
      goto reply;
    }

    // can't jump to 'update' because we're doing something else before
    // returning. And we're also waiting on a different thing.

    wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, 0, rs, rdata,
                               get_last_committed() + 1));
    force_immediate_propose();
    return true;

  } else if (prefix == "osd create") {

    // optional id provided?
    int64_t id = -1, cmd_id = -1;
    if (cmd_getval(cct, cmdmap, "id", cmd_id)) {
      if (cmd_id < 0) {
	ss << "invalid osd id value '" << cmd_id << "'";
	err = -EINVAL;
	goto reply;
      }
      dout(10) << " osd create got id " << cmd_id << dendl;
    }

    uuid_d uuid;
    string uuidstr;
    if (cmd_getval(cct, cmdmap, "uuid", uuidstr)) {
      if (!uuid.parse(uuidstr.c_str())) {
        ss << "invalid uuid value '" << uuidstr << "'";
        err = -EINVAL;
        goto reply;
      }
      // we only care about the id if we also have the uuid, to
      // ensure the operation's idempotency.
      id = cmd_id;
    }

    int32_t new_id = -1;
    err = prepare_command_osd_create(id, uuid, &new_id, ss);
    if (err < 0) {
      if (err == -EAGAIN) {
	goto wait;
      }
      // a check has failed; reply to the user.
      goto reply;

    } else if (err == EEXIST) {
      // this is an idempotent operation; we can go ahead and reply.
      if (f) {
        f->open_object_section("created_osd");
        f->dump_int("osdid", new_id);
        f->close_section();
        f->flush(rdata);
      } else {
        ss << new_id;
        rdata.append(ss);
      }
      err = 0;
      goto reply;
    }

    string empty_device_class;
    do_osd_create(id, uuid, empty_device_class, &new_id);

    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", new_id);
      f->close_section();
      f->flush(rdata);
    } else {
      ss << new_id;
      rdata.append(ss);
    }

    // can't jump to 'update' because we're doing a different thing.
    wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, 0, rs, rdata,
                               get_last_committed() + 1));
    return true;

  } else {
    err = -EINVAL;
  }

 reply:
  getline(ss, rs);
  is_err = (err < 0 && rs.length() == 0);
  if (is_err) {
    rs = cpp_strerror(err);
  }
  mon->reply_command(op, err, rs, rdata, get_last_committed());
  return !is_err;

 update:
  getline(ss, rs);
  wait_for_finished_proposal(op,
      new Monitor::C_Command(mon, op, 0, rs, get_last_committed() + 1));
  return true;

 wait:
  wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  return true;
}
