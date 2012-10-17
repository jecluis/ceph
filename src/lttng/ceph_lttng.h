// -*- mode:C++; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab
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
#ifndef CEPH_LTTNG_H_
#define CEPH_LTTNG_H_

#ifdef CEPH_LTTNG_ENABLE

#define ceph_lttng__mon__function(func)														\
	do {																														\
		Monitor *tp_mon = this;																				\
		tracepoint(mon, function,																			\
				tp_mon->name.c_str(), tp_mon->get_state_name(),						\
				func);																										\
	} while (0)

#define ceph_lttng__mon__msg(mark, m)															\
	do {																														\
		Monitor *tp_mon = this;																				\
		Message *tp_m = m;																						\
		entity_inst_t source = tp_m->get_source_inst();								\
		string from_name = tp_mon->monmap->get_name(source.addr);			\
		tracepoint(mon, msg,																					\
				tp_mon->name.c_str(), tp_mon->get_state_name(),						\
				mark, from_name.c_str(), tp_m->get_type_name());					\
	} while (0)

#define ceph_lttng__mon__msg_sync(mark, m)												\
	do {																														\
		Monitor *tp_mon = this;																				\
		MMonSync *tp_m = m;																						\
		entity_inst_t source = tp_m->get_source_inst();								\
		string from_name = tp_mon->monmap->get_name(source.addr);			\
		tracepoint(mon, msg_sync,																			\
				tp_mon->name.c_str(), tp_mon->get_state_name(),						\
				mark, from_name.c_str(),																	\
				tp_m->get_opname(tp_m->op));															\
	} while (0)


#define ceph_lttng__mon__store_sync(mark)													\
	do {																														\
		Monitor *tp_mon = this;																				\
		string role = tp_mon->get_sync_role_name(tp_mon->sync_role);	\
		string state = tp_mon->get_sync_state_name(tp_mon->sync_state); \
		tracepoint(mon, store_sync,																		\
				tp_mon->name.c_str(), tp_mon->get_state_name(),						\
				mark, role.c_str(), state.c_str());												\
	} while (0)


#define ceph_lttng__mon__store_sync_requester(mark, info)					\
	do {																														\
		Monitor *tp_mon = this;																				\
		string role = tp_mon->get_sync_role_name(tp_mon->sync_role);	\
		string state = tp_mon->get_sync_state_name(tp_mon->sync_state); \
		string leader_name;																						\
		string provider_name;																					\
		if (tp_mon->sync_leader.get() != NULL) {											\
			entity_inst_t inst = tp_mon->sync_leader->entity;						\
			leader_name = tp_mon->monmap->get_name(inst.addr);					\
		}																															\
		if (tp_mon->sync_provider.get() != NULL) {										\
			entity_inst_t inst = tp_mon->sync_provider->entity;					\
			provider_name = tp_mon->monmap->get_name(inst.addr);				\
		}																															\
		tracepoint(mon, store_sync_requester,													\
							 tp_mon->name.c_str(), tp_mon->get_state_name(),		\
							 mark, info, role.c_str(), state.c_str(),						\
							 leader_name.c_str(),																\
							 provider_name.c_str());														\
	} while (0)

#define ceph_lttng__mon__store_sync_error(mark, info)							\
	do {																														\
		Monitor *tp_mon = this;																				\
		string role = tp_mon->get_sync_role_name(tp_mon->sync_role);	\
		string state = tp_mon->get_sync_state_name(tp_mon->sync_state); \
		tracepoint(mon, store_sync_error,															\
							 tp_mon->name.c_str(), tp_mon->get_state_name(),		\
							 mark, info, role.c_str(), state.c_str());					\
	} while (0)

#define ceph_tp(ssys, comp, args...) \
	ceph_lttng__##ssys##__##comp(args)

#define ceph_tp_entry(ssys, f)																			\
	do {																														\
		string func(f);																								\
		func.append(":entry");																				\
		ceph_tp(ssys, function, func.c_str());												\
	} while (0)

#define ceph_tp_exit(ssys, f)																				\
	do {																														\
		string func(f);																								\
		func.append(":exit");																					\
		ceph_tp(ssys, function, func.c_str());												\
	} while (0)


#define ceph_lttng_mark_okay		"okay"
#define ceph_lttng_mark_barrier	"barrier"
#define ceph_lttng_mark_retry		"retry"
#define ceph_lttng_mark_abort		"abort"

#else
#warning ceph_lttng.h included but not enabled!

#define ceph_tp(ssys, comp, args...) \
		do { } while(0)

#endif // ceph_lttng_subsys

#endif // CEPH_LTTNG_H_
