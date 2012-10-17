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
#ifndef CEPH_LTTNG_MON_MONITOR_H_
#define CEPH_LTTNG_MON_MONITOR_H_


#define ceph_lttng_mon_monitor(mon) \
  mon->name, \
  mon->get_state_name()

#endif // CEPH_LTTNG_MON_MONITOR_H_
