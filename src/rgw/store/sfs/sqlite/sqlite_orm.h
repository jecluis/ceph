// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#pragma once

// TODO investigate which optimisations are causing gcc to get killed
// Disable optimisations for sqlite_orm for now
#pragma GCC push_options
#pragma GCC optimize ("O0")

#include <sqlite_orm/sqlite_orm.h>

#pragma GCC pop_options
