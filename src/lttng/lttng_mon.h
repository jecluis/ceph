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
#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER mon

#if !defined(_CEPH_LTTNG_MON_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define _CEPH_LTTNG_MON_H

#include <lttng/tracepoint.h>

TRACEPOINT_EVENT(
    mon,
    function,
    TP_ARGS(const char *, mon_name,
	    const char *, state_name,
	    const char *, func_name
	    ),
    TP_FIELDS(
	    ctf_string(mon, mon_name)
	    ctf_string(state, state_name)
	    ctf_string(func, func_name)
	    )
)

TRACEPOINT_EVENT(
    mon,
    msg,
    TP_ARGS(const char *, mon_name,
	    const char *, state_name,
	    const char *, mark_name,
	    const char *, from_str,
	    const char *, type_name
	    ),
    TP_FIELDS(
	    ctf_string(mon, mon_name)
	    ctf_string(state, state_name)
	    ctf_string(mark, mark_name)
	    ctf_string(from, from_str)
	    ctf_string(type, type_name)
	    )
)

TRACEPOINT_EVENT(
    mon,
    msg_sync,
    TP_ARGS(const char *, mon_name,
	    const char *, state_name,
	    const char *, mark_name,
	    const char *, from_str,
	    const char *, op_name
	    ),
    TP_FIELDS(
	    ctf_string(mon, mon_name)
	    ctf_string(state, state_name)
	    ctf_string(mark, mark_name)
	    ctf_string(from, from_str)
	    ctf_string(op, op_name)
	    )
)

TRACEPOINT_EVENT(
    mon,
    store_sync,
    TP_ARGS(const char *, mon_name,
	    const char *, state_name,
	    const char *, mark_name,
	    const char *, role_name,
	    const char *, sync_state_name
	    ),
    TP_FIELDS(
	    ctf_string(mon, mon_name)
	    ctf_string(state, state_name)
	    ctf_string(mark, mark_name)
	    ctf_string(role, role_name)
	    ctf_string(sync_state, sync_state_name)
	    )
)

TRACEPOINT_EVENT(
    mon,
    store_sync_error,
    TP_ARGS(const char *, mon_name,
	    const char *, state_name,
	    const char *, mark_name,
	    const char *, mark_info_str,
	    const char *, role_name,
	    const char *, sync_state_name
	    ),
    TP_FIELDS(
	    ctf_string(mon, mon_name)
	    ctf_string(state, state_name)
	    ctf_string(mark, mark_name)
	    ctf_string(mark_info, mark_info_str)
	    ctf_string(role, role_name)
	    ctf_string(sync_state, sync_state_name)
	    )
)

TRACEPOINT_EVENT(
    mon,
    store_sync_requester,
    TP_ARGS(const char *, mon_name,
	    const char *, state_name,
	    const char *, mark_name,
	    const char *, mark_info_str,
	    const char *, role_name,
	    const char *, sync_state_name,
	    const char *, leader_str,
	    const char *, provider_str
	    ),
    TP_FIELDS(
	    ctf_string(mon, mon_name)
	    ctf_string(state, state_name)
	    ctf_string(mark, mark_name)
	    ctf_string(mark_info, mark_info_str)
	    ctf_string(role, role_name)
	    ctf_string(sync_state, sync_state_name)
	    ctf_string(leader, leader_str)
	    ctf_string(provider, provider_str)
	    )
)

#endif /* _CEPH_LTTNG_MON_H */

#undef TRACEPOINT_INCLUDE_FILE
#define TRACEPOINT_INCLUDE_FILE lttng/lttng_mon.h

/* This part must be outside ifdef protection */
#include <lttng/tracepoint-event.h>

