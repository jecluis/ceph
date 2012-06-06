// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef CEPH_MONITOR_DB_STORE_H
#define CEPH_MONITOR_DB_STORE_H

#include "include/types.h"
#include "include/buffer.h"
#include <set>
#include <map>
#include <string>
#include <boost/scoped_ptr.hpp>
#include "os/LevelDBStore.h"

class MonitorDBStore
{
  boost::scoped_ptr<LevelDBStore> db;

 protected:

  struct Op {
    uint8_t type;
    string prefix;
    string key;
    bufferlist bl;

    Op(int t, string p, string k)
      : type(t), prefix(p), key(k) { }
    Op(int t, const string& p, string k, bufferlist& b)
      : type(t), prefix(p), key(k), bl(b) { }

    void encode(bufferlist& encode_bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(type, encode_bl);
      ::encode(prefix, encode_bl);
      ::encode(key, encode_bl);
      ::encode(bl, encode_bl);
      ENCODE_FINISH(encode_bl);
    }

    void decode(bufferlist::iterator& decode_bl) {
      DECODE_START(1, decode_bl);
      ::decode(type, decode_bl);
      ::decode(prefix, decode_bl);
      ::decode(key, decode_bl);
      ::decode(bl, decode_bl);
      DECODE_FINISH(decode_bl);
    }
  };

 public:

  struct Transaction {
    list<Op> ops;

    enum {
      OP_PUT	= 1,
      OP_ERASE	= 2,
    };

    void put(string prefix, string key, bufferlist& bl) {
      ops.push_back(Op(OP_PUT, prefix, key, bl));
    }

    void put(string prefix, version_t ver, bufferlist& bl) {
      ostringstream os;
      os << ver;
      put(prefix, os.str(), bl);
    }

    void put(string prefix, string key, version_t ver) {
      bufferlist bl;
      ::encode(ver, bl);
      put(prefix, key, bl);
    }

    void erase(string prefix, string key) {
      ops.push_back(Op(OP_ERASE, prefix, key));
    }

    void erase(string prefix, version_t ver) {
      ostringstream os;
      os << ver;
      erase(prefix, os.str());
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(1, 1, bl);
      ::encode(ops, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
      DECODE_START(1, bl);
      ::decode(ops, bl);
      DECODE_FINISH(bl);
    }

    void append(Transaction& other) {
      ops.splice(ops.end(), other.ops);
    }

    void append_from_encoded(bufferlist& bl) {
      Transaction other;
      other.decode(bl.begin());
      append(other);
    }

    bool empty() {
      return (ops.size() == 0);
    }
  };

  int apply_transaction(MonitorDBStore::Transaction& t) {
    KeyValueDB::Transaction dbt = db->get_transaction();

    for (list<Op>::iterator it = t.ops.begin(); it != t.ops.end(); ++it) {
      Op& op = *it;
      switch (op.type) {
      case OP_PUT:
	dbt->set(op.prefix, op.key, op.bl);
	break;
      case OP_ERASE:
	dbt->rmkey(op.prefix, op.key);
	break;
      default:
	derr << __func__ << " unknown op type " << op.type << dendl;
	ceph_assert(0);
	break;
      }
    }
    return db->submit_transaction_sync(dbt);
  }

  int get(string prefix, string key, bufferlist& bl) {
    set<string> k;
    k.push_back(key);
    map<string,bufferlist> out;

    db->get(prefix, k, out);
    if (!out.empty())
      bl.append(out[key]);

    return 0;
  }

  int get(string prefix, version_t ver, bufferlist& bl) {
    ostringstream os;
    os << ver;
    return get(prefix, os.str(), bl);
  }

  version_t get(string prefix, string key) {
    bufferlist bl;
    get(prefix, key, bl);
    if (bl.empty()) // if key does not exist, assume its value is 0
      return 0;

    version_t ver;
    ::decode(ver, bl);
    return ver;
  }

  bool exists(const string prefix, const string key) {
    Iterator it = db->iterator(prefix);
    int err = it.lower_bound(key);
    if (err < 0)
      return false;

    return (it.valid() && it.key() == key);
  }

  bool exists(const string prefix, version_t ver) {
    ostringstream os;
    os << ver;
    return exists(prefix, os.str());
  }

  string combine_strings(const string& prefix, const string& value) {
    string out = prefix;
    out.push_back('_');
    out.append(value);
    return out;
  }

  MonitorDBStore(const string& path) {
    LevelDBStore *db_ptr = new LevelDBStore(path);
    db.reset(db_ptr);
  }
  MonitorDBStore(LevelDBStore *db_ptr) {
    db.reset(db_ptr);
  }
  ~MonitorDBStore() { }

};

WRITE_CLASS_ENCODER(MonitorDBStore::Op);
WRITE_CLASS_ENCODER(MonitorDBStore::Transaction);

#endif /* CEPH_MONITOR_DB_STORE_H */
