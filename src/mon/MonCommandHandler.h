// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Joao Eduardo Luis <joao@suse.de>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

typedef std::shared_ptr<ceph::Formatter> FormatterRef;

template<typename T, typename U>
class MonCommandHandler
{
  bool read_write;

protected:
  std::set<std::string> prefixes;

public:
  MonCommandHandler(std::set<std::string> _prefixes)
    : read_write(false), prefixes(_prefixes)
  {}

  explicit MonCommandHandler(
      const std::set<std::string> _prefixes,
      bool rw)
    : read_write(rw), prefixes(_prefixes)
  {}

  virtual ~MonCommandHandler() { }

  bool can_handle(const std::string &prefix) const
  {
    return prefixes.find(prefix) != prefixes.end();
  }

  bool is_rw() const
  {
    return read_write;
  }

  virtual bool batched_propose() const
  {
    return false;
  }

  /**
   * handle query
   */
  int handle(
      Monitor *mon,
      const T& map,
      MonOpRequestRef op,
      std::map<std::string,cmd_vartype>& cmdmap,
      std::stringstream& ss,
      bufferlist& rdata,
      FormatterRef f)
  {
    assert(!read_write);
    return handle_query(mon, map, op, cmdmap, ss, rdata, f);
  }

  /**
   * handle update
   */
  int handle(
      Monitor *mon,
      const T& map,
      U& pending_map,
      MonOpRequestRef op,
      std::map<std::string,cmd_vartype>& cmdmap,
      std::stringstream& ss)
  {
    assert(read_write);
    return handle_update(mon, map, pending_map, op, cmdmap, ss);
  }

  virtual int handle_query(
      Monitor *mon,
      const T& map,
      MonOpRequestRef op,
      std::map<std::string,cmd_vartype>& cmdmap,
      std::stringstream& ss,
      bufferlist& rdata,
      FormatterRef f) = 0;

  virtual int handle_update(
      Monitor *mon,
      const T& map,
      U& pending_map,
      MonOpRequestRef op,
      std::map<std::string,cmd_vartype>& cmdmap,
      std::stringstream& ss) = 0;
};

template<typename T>
class MonCommandQueryHandler : public MonCommandHandler<T,T>
{

public:
  explicit MonCommandQueryHandler(const std::set<std::string> prefixes)
    : MonCommandHandler<T,T>(prefixes, false)
  {}

  virtual ~MonCommandQueryHandler() {}

  virtual int handle_query(
      Monitor *mon,
      const T& map,
      MonOpRequestRef op,
      std::map<std::string,cmd_vartype>& cmdmap,
      std::stringstream& ss,
      bufferlist& rdata,
      FormatterRef f) override = 0;

  virtual int handle_update(
      Monitor *mon,
      const T& map,
      T& pending_map,
      MonOpRequestRef op,
      std::map<std::string,cmd_vartype>& cmdmap,
      std::stringstream& ss) final
  {
    assert(0 == "query commands do not update");
    return -ENOTSUP;
  }
};

template<typename T, typename U>
class MonCommandUpdateHandler : public MonCommandHandler<T,U>
{

public:
  explicit MonCommandUpdateHandler(const std::set<std::string> prefixes)
    : MonCommandHandler<T,U>(prefixes, true)
  {}

  virtual ~MonCommandUpdateHandler() {}

  virtual int handle_query(
      Monitor *mon,
      const T& map,
      MonOpRequestRef op,
      std::map<std::string,cmd_vartype>& cmdmap,
      std::stringstream& ss,
      bufferlist& rdata,
      FormatterRef f) final
  {
    assert(0 == "update commands do not query");
    return -ENOTSUP;
  }

  virtual int handle_update(
      Monitor *mon,
      const T& map,
      U& pending_map,
      MonOpRequestRef op,
      std::map<std::string,cmd_vartype>& cmdmap,
      std::stringstream& ss) override = 0;
};
