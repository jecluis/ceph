// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LLC <contact@suse.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MON_COMMAND_H
#define CEPH_MON_COMMAND_H

#include <string>
#include <set>
#include <sstream>

#include <boost/intrusive_ptr.hpp>

#include "mon/Monitor.h"
#include "mon/MonOpRequest.h"

#include "include/types.h"
#include "include/encoding.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/cmdparse.h"
#include "common/RefCountedObj.h"

using namespace std;

template<typename T, typename Stable, typename Pending>
class Command : public RefCountedObject
{

  Command() = delete;

  void wait_for_finished_proposal(MonOpRequestRef op, Context *ctx) {
    service->wait_for_finished_proposal(op, ctx);
  }

 protected:

  Monitor *mon;
  T *service;
  CephContext *cct;

  bool reply_with_data(MonOpRequestRef op,
		       int retcode,
		       stringstream &ss,
		       bufferlist &retdata,
		       version_t version) {
    string retstr;
    getline(ss, retstr);

    bool ret = true;

    if (retcode < 0 && retstr.length() == 0) {
      retstr = cpp_strerror(retcode);
      ret = false;
    }
    mon->reply_command(op, retcode, retstr, retdata, version);
    return ret;
  }

  bool reply(MonOpRequestRef op,
	     int retcode,
	     stringstream &ss,
	     version_t version) {
    bufferlist retdata;
    return reply_with_data(op, retcode, ss, retdata, version);
  }

  bool update(MonOpRequestRef op,
	      stringstream &ss,
	      version_t version) {

    string retstr;
    getline(ss, retstr);

    wait_for_finished_proposal(op,
	new Monitor::C_Command(mon, op, 0, retstr, version));
    return true;
  }
  bool wait_retry(MonOpRequestRef op,
	     stringstream &ss,
	     version_t version) {
    string retstr;
    getline(ss, retstr);

    wait_for_finished_proposal(op,
	new Monitor::C_RetryMessage(mon, op));
    return true;
  }

 public:

  explicit Command(Monitor *_mon, T *svc, CephContext *_cct) :
    RefCountedObject(_cct),
    mon(_mon),
    service(svc),
    cct(_cct)
  {
    ceph_assert(mon != nullptr);
    ceph_assert(service != nullptr);
    ceph_assert(cct != nullptr);
  }

  virtual ~Command() { };

  virtual bool preprocess(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      const Stable &stable_map) { return false; }

  virtual bool prepare(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      Pending &pending_map,
      Stable &stable_map) { return false; }

  virtual bool handles_command(const string &prefix) = 0;

};

typedef std::shared_ptr<Formatter> FormatterRef;

template<typename T, typename Stable, typename Pending>
struct ReadCommand : public Command<T, Stable, Pending>
{

  ReadCommand(Monitor *_mon, T *_svc, CephContext *_cct) :
    Command<T,Stable,Pending>(_mon, _svc, _cct)
  {
    ceph_assert(this->mon != nullptr);
    ceph_assert(this->service != nullptr);
    ceph_assert(this->cct != nullptr);
  }

  virtual ~ReadCommand() { }

  bool preprocess(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      const Stable &stable_map) {

    stringstream ss;
    bufferlist rdata;

    string format;
    cmd_getval(this->cct, cmdmap, "format", format, string("plain"));
    FormatterRef f(Formatter::create(format));

    return do_preprocess(op, cmdmap, ss, rdata, f, stable_map);
  }

 protected:
  virtual bool do_preprocess(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      const Stable &stable_map) = 0;

 private:
  ReadCommand() = delete;
};

template<typename T, typename Stable, typename Pending>
struct WriteCommand : public Command<T, Stable, Pending>
{

  explicit WriteCommand(Monitor *_mon, T *_svc, CephContext *_cct) :
    Command<T, Stable, Pending>(_mon, _svc, _cct)
  { }

  virtual ~WriteCommand() { }

  bool prepare(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      Pending &pending_map,
      Stable &stable_map) {

    op->mark_osdmon_event(__func__);
    stringstream ss;
    bufferlist rdata;

    string format;
    cmd_getval(this->cct, cmdmap, "format", format, string("plain"));
    FormatterRef f(Formatter::create(format));

    return do_prepare(op, cmdmap, ss, rdata, f, pending_map, stable_map);
  }

 protected:
  virtual bool do_prepare(
      MonOpRequestRef op,
      const cmdmap_t &cmdmap,
      stringstream &ss,
      bufferlist rdata,
      FormatterRef f,
      Pending &pending_map,
      Stable &stable_map) = 0;

 private:
  WriteCommand() = delete;
};


#endif // CEPH_MON_COMMAND_H
