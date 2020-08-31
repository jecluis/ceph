// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "svc_role.h"
#include "svc_meta_be.h"

class RGWSI_Role_RADOS: public RGWSI_Role
{
 public:
  RGWSI_Role_RADOS(CephContext *cct) : RGWSI_Role(cct) {}
  ~RGWSI_Role_RADOS() {}

  RGWSI_MetaBackend_Handler * get_be_handler() override;

  int store_info(RGWSI_MetaBackend::Context *ctx,
  		 const RGWRole& role,
  		 RGWObjVersionTracker * const objv_tracker,
  		 real_time * const pmtime,
  		 bool exclusive,
		 std::map<std::string, bufferlist> * pattrs,
  		 optional_yield y) override;

  int store_name(RGWSI_MetaBackend::Context *ctx,
  		 const std::string& name,
  		 RGWObjVersionTracker * const objv_tracker,
  		 real_time * const pmtime,
  		 bool exclusive,
  		 optional_yield y) override;

  int store_path(RGWSI_MetaBackend::Context *ctx,
  		 const std::string& path,
  		 RGWObjVersionTracker * const objv_tracker,
  		 real_time * const pmtime,
  		 bool exclusive,
  		 optional_yield y) override;

  int read_info(RGWSI_MetaBackend::Context *ctx,
  		RGWRole *role,
  		RGWObjVersionTracker * const objv_tracker,
  		real_time * const pmtime,
		std::map<std::string, bufferlist> * pattrs,
  		optional_yield y) override;

  int read_name(RGWSI_MetaBackend::Context *ctx,
  		std::string& name,
  		RGWObjVersionTracker * const objv_tracker,
  		real_time * const pmtime,
  		optional_yield y) override;

  int read_path(RGWSI_MetaBackend::Context *ctx,
  		std::string& path,
  		RGWObjVersionTracker * const objv_tracker,
  		real_time * const pmtime,
  		optional_yield y) override;

  int delete_info(RGWSI_MetaBackend::Context *ctx,
		  const std::string& name,
		  RGWObjVersionTracker * const objv_tracker,
		  optional_yield y) override;


private:
  RGWSI_MetaBackend_Handler *be_handler;
  std::unique_ptr<RGWSI_MetaBackend::Module> be_module;
};
