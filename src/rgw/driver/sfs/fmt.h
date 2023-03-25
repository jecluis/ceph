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
#ifndef RGW_DRIVER_SFS_FMT_H
#define RGW_DRIVER_SFS_FMT_H

#include <fmt/core.h>

#include <filesystem>

template <>
struct fmt::formatter<std::filesystem::path> : formatter<string_view> {
  // parse is inherited from formatter<string_view>
  auto format(std::filesystem::path& p, format_context& ctx) const {
    return formatter<string_view>::format(p.string(), ctx);
  }
};

#endif  // RGW_DRIVER_SFS_FMT_H
