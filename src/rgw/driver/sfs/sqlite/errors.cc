// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */

#include <sqlite3.h>

namespace rgw::sal::sfs::sqlite {

bool critical_error(int ec) {
  switch (ec) {
    case SQLITE_ERROR:
    case SQLITE_INTERNAL:
    case SQLITE_PERM:
    case SQLITE_NOMEM:
    case SQLITE_READONLY:
    case SQLITE_IOERR:
    case SQLITE_CORRUPT:
    case SQLITE_NOTFOUND:
    case SQLITE_FULL:
    case SQLITE_CANTOPEN:
    case SQLITE_PROTOCOL:
    case SQLITE_TOOBIG:
    case SQLITE_MISMATCH:
    case SQLITE_MISUSE:
    case SQLITE_NOLFS:
    case SQLITE_AUTH:
    case SQLITE_RANGE:
    case SQLITE_NOTADB:
      return true;
    default:
      return false;
  }
}

}  // namespace rgw::sal::sfs::sqlite
