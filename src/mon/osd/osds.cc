// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 * Copyright (C) 2019 SUSE LLC <contact@suse.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sstream>

#include "common/config.h"
#include "common/errno.h"

#include "include/ceph_assert.h"

#include "mon/Monitor.h"
#include "mon/OSDMonitor.h"
#include "mon/AuthMonitor.h"
#include "mon/ConfigKeyService.h"

/*
 * functions
 *
 *  int validate_osd_create +
 *  int prepare_command_osd_create +
 *  int prepare_command_osd_new +
 *  void do_osd_create +
 *  int prepare_command_osd_destroy
 *  int prepare_command_osd_purge
 *  int prepare_command_osd_remove
 *
 */

#define dout_subsys ceph_subsys_mon

int OSDMonitor::validate_osd_create(
    const int32_t id,
    const uuid_d& uuid,
    const bool check_osd_exists,
    int32_t* existing_id,
    stringstream& ss)
{

  dout(10) << __func__ << " id " << id << " uuid " << uuid
           << " check_osd_exists " << check_osd_exists << dendl;

  ceph_assert(existing_id);

  if (id < 0 && uuid.is_zero()) {
    // we have nothing to validate
    *existing_id = -1;
    return 0;
  } else if (uuid.is_zero()) {
    // we have an id but we will ignore it - because that's what
    // `osd create` does.
    return 0;
  }

  /*
   * This function will be used to validate whether we are able to
   * create a new osd when the `uuid` is specified.
   *
   * It will be used by both `osd create` and `osd new`, as the checks
   * are basically the same when it pertains to osd id and uuid validation.
   * However, `osd create` presumes an `uuid` is optional, for legacy
   * reasons, while `osd new` requires the `uuid` to be provided. This
   * means that `osd create` will not be idempotent if an `uuid` is not
   * provided, but we will always guarantee the idempotency of `osd new`.
   */

  ceph_assert(!uuid.is_zero());
  if (pending_inc.identify_osd(uuid) >= 0) {
    // osd is about to exist
    return -EAGAIN;
  }

  int32_t i = osdmap.identify_osd(uuid);
  if (i >= 0) {
    // osd already exists
    if (id >= 0 && i != id) {
      ss << "uuid " << uuid << " already in use for different id " << i;
      return -EEXIST;
    }
    // return a positive errno to distinguish between a blocking error
    // and an error we consider to not be a problem (i.e., this would be
    // an idempotent operation).
    *existing_id = i;
    return EEXIST;
  }
  // i < 0
  if (id >= 0) {
    if (pending_inc.new_state.count(id)) {
      // osd is about to exist
      return -EAGAIN;
    }
    // we may not care if an osd exists if we are recreating a previously
    // destroyed osd.
    if (check_osd_exists && osdmap.exists(id)) {
      ss << "id " << id << " already in use and does not match uuid "
         << uuid;
      return -EINVAL;
    }
  }
  return 0;
}

int OSDMonitor::prepare_command_osd_create(
    const int32_t id,
    const uuid_d& uuid,
    int32_t* existing_id,
    stringstream& ss)
{
  dout(10) << __func__ << " id " << id << " uuid " << uuid << dendl;
  ceph_assert(existing_id);
  if (osdmap.is_destroyed(id)) {
    ss << "ceph osd create has been deprecated. Please use ceph osd new "
          "instead.";
    return -EINVAL;
  }

  if (uuid.is_zero()) {
    dout(10) << __func__ << " no uuid; assuming legacy `osd create`" << dendl;
  }

  return validate_osd_create(id, uuid, true, existing_id, ss);
}

void OSDMonitor::do_osd_create(
    const int32_t id,
    const uuid_d& uuid,
    const string& device_class,
    int32_t* new_id)
{
  dout(10) << __func__ << " uuid " << uuid << dendl;
  ceph_assert(new_id);

  // We presume validation has been performed prior to calling this
  // function. We assert with prejudice.

  int32_t allocated_id = -1; // declare here so we can jump
  int32_t existing_id = -1;
  if (!uuid.is_zero()) {
    existing_id = osdmap.identify_osd(uuid);
    if (existing_id >= 0) {
      ceph_assert(id < 0 || id == existing_id);
      *new_id = existing_id;
      goto out;
    } else if (id >= 0) {
      // uuid does not exist, and id has been provided, so just create
      // the new osd.id
      *new_id = id;
      goto out;
    }
  }

  // allocate a new id
  allocated_id = _allocate_osd_id(&existing_id);
  dout(10) << __func__ << " allocated id " << allocated_id
           << " existing id " << existing_id << dendl;
  if (existing_id >= 0) {
    ceph_assert(existing_id < osdmap.get_max_osd());
    ceph_assert(allocated_id < 0);
    pending_inc.new_weight[existing_id] = CEPH_OSD_OUT;
    *new_id = existing_id;
  } else if (allocated_id >= 0) {
    ceph_assert(existing_id < 0);
    // raise max_osd
    if (pending_inc.new_max_osd < 0) {
      pending_inc.new_max_osd = osdmap.get_max_osd() + 1;
    } else {
      ++pending_inc.new_max_osd;
    }
    *new_id = pending_inc.new_max_osd - 1;
    ceph_assert(*new_id == allocated_id);
  } else {
    ceph_abort_msg("unexpected condition");
  }

out:
  if (device_class.size()) {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    if (newcrush.get_max_devices() < *new_id + 1) {
      newcrush.set_max_devices(*new_id + 1);
    }
    string name = string("osd.") + stringify(*new_id);
    if (!newcrush.item_exists(*new_id)) {
      newcrush.set_item_name(*new_id, name);
    }
    ostringstream ss;
    int r = newcrush.update_device_class(*new_id, device_class, name, &ss);
    if (r < 0) {
      derr << __func__ << " failed to set " << name << " device_class "
	   << device_class << ": " << cpp_strerror(r) << " - " << ss.str()
	   << dendl;
      // non-fatal... this might be a replay and we want to be idempotent.
    } else {
      dout(20) << __func__ << " set " << name << " device_class "
	       << device_class << dendl;
      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    }
  } else {
    dout(20) << __func__ << " no device_class" << dendl;
  }

  dout(10) << __func__ << " using id " << *new_id << dendl;
  if (osdmap.get_max_osd() <= *new_id && pending_inc.new_max_osd <= *new_id) {
    pending_inc.new_max_osd = *new_id + 1;
  }

  pending_inc.new_state[*new_id] |= CEPH_OSD_EXISTS | CEPH_OSD_NEW;
  if (!uuid.is_zero())
    pending_inc.new_uuid[*new_id] = uuid;
}

int OSDMonitor::prepare_command_osd_new(
    MonOpRequestRef op,
    const cmdmap_t& cmdmap,
    const map<string,string>& params,
    stringstream &ss,
    Formatter *f)
{
  uuid_d uuid;
  string uuidstr;
  int64_t id = -1;

  ceph_assert(paxos->is_plugged());

  dout(10) << __func__ << " " << op << dendl;

  /* validate command. abort now if something's wrong. */

  /* `osd new` will expect a `uuid` to be supplied; `id` is optional.
   *
   * If `id` is not specified, we will identify any existing osd based
   * on `uuid`. Operation will be idempotent iff secrets match.
   *
   * If `id` is specified, we will identify any existing osd based on
   * `uuid` and match against `id`. If they match, operation will be
   * idempotent iff secrets match.
   *
   * `-i secrets.json` will be optional. If supplied, will be used
   * to check for idempotency when `id` and `uuid` match.
   *
   * If `id` is not specified, and `uuid` does not exist, an id will
   * be found or allocated for the osd.
   *
   * If `id` is specified, and the osd has been previously marked
   * as destroyed, then the `id` will be reused.
   */
  if (!cmd_getval(cct, cmdmap, "uuid", uuidstr)) {
    ss << "requires the OSD's UUID to be specified.";
    return -EINVAL;
  } else if (!uuid.parse(uuidstr.c_str())) {
    ss << "invalid UUID value '" << uuidstr << "'.";
    return -EINVAL;
  }

  if (cmd_getval(cct, cmdmap, "id", id) &&
      (id < 0)) {
    ss << "invalid OSD id; must be greater or equal than zero.";
    return -EINVAL;
  }

  // are we running an `osd create`-like command, or recreating
  // a previously destroyed osd?

  bool is_recreate_destroyed = (id >= 0 && osdmap.is_destroyed(id));

  // we will care about `id` to assess whether osd is `destroyed`, or
  // to create a new osd.
  // we will need an `id` by the time we reach auth.

  int32_t existing_id = -1;
  int err = validate_osd_create(id, uuid, !is_recreate_destroyed,
                                &existing_id, ss);

  bool may_be_idempotent = false;
  if (err == EEXIST) {
    // this is idempotent from the osdmon's point-of-view
    may_be_idempotent = true;
    ceph_assert(existing_id >= 0);
    id = existing_id;
  } else if (err < 0) {
    return err;
  }

  if (!may_be_idempotent) {
    // idempotency is out of the window. We are either creating a new
    // osd or recreating a destroyed osd.
    //
    // We now need to figure out if we have an `id` (and if it's valid),
    // of find an `id` if we don't have one.

    // NOTE: we need to consider the case where the `id` is specified for
    // `osd create`, and we must honor it. So this means checking if
    // the `id` is destroyed, and if so assume the destroy; otherwise,
    // check if it `exists` - in which case we complain about not being
    // `destroyed`. In the end, if nothing fails, we must allow the
    // creation, so that we are compatible with `create`.
    if (id >= 0 && osdmap.exists(id) && !osdmap.is_destroyed(id)) {
      dout(10) << __func__ << " osd." << id << " isn't destroyed" << dendl;
      ss << "OSD " << id << " has not yet been destroyed";
      return -EINVAL;
    } else if (id < 0) {
      // find an `id`
      id = _allocate_osd_id(&existing_id);
      if (id < 0) {
        ceph_assert(existing_id >= 0);
        id = existing_id;
      }
      dout(10) << __func__ << " found id " << id << " to use" << dendl;
    } else if (id >= 0 && osdmap.is_destroyed(id)) {
      dout(10) << __func__ << " recreating osd." << id << dendl;
    } else {
      dout(10) << __func__ << " creating new osd." << id << dendl;
    }
  } else {
    ceph_assert(id >= 0);
    ceph_assert(osdmap.exists(id));
  }

  // we are now able to either create a brand new osd or reuse an existing
  // osd that has been previously destroyed.

  dout(10) << __func__ << " id " << id << " uuid " << uuid << dendl;

  if (may_be_idempotent && params.empty()) {
    // nothing to do, really.
    dout(10) << __func__ << " idempotent and no params -- no op." << dendl;
    ceph_assert(id >= 0);
    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", id);
      f->close_section();
    } else {
      ss << id;
    }
    return EEXIST;
  }

  string device_class;
  auto p = params.find("crush_device_class");
  if (p != params.end()) {
    device_class = p->second;
    dout(20) << __func__ << " device_class will be " << device_class << dendl;
  }
  string cephx_secret, lockbox_secret, dmcrypt_key;
  bool has_lockbox = false;
  bool has_secrets = params.count("cephx_secret")
    || params.count("cephx_lockbox_secret")
    || params.count("dmcrypt_key");

  ConfigKeyService *svc = nullptr;
  AuthMonitor::auth_entity_t cephx_entity, lockbox_entity;

  if (has_secrets) {
    if (params.count("cephx_secret") == 0) {
      ss << "requires a cephx secret.";
      return -EINVAL;
    }
    cephx_secret = params.at("cephx_secret");

    bool has_lockbox_secret = (params.count("cephx_lockbox_secret") > 0);
    bool has_dmcrypt_key = (params.count("dmcrypt_key") > 0);

    dout(10) << __func__ << " has lockbox " << has_lockbox_secret
             << " dmcrypt " << has_dmcrypt_key << dendl;

    if (has_lockbox_secret && has_dmcrypt_key) {
      has_lockbox = true;
      lockbox_secret = params.at("cephx_lockbox_secret");
      dmcrypt_key = params.at("dmcrypt_key");
    } else if (!has_lockbox_secret != !has_dmcrypt_key) {
      ss << "requires both a cephx lockbox secret and a dm-crypt key.";
      return -EINVAL;
    }

    dout(10) << __func__ << " validate secrets using osd id " << id << dendl;

    err = mon->authmon()->validate_osd_new(id, uuid,
        cephx_secret,
        lockbox_secret,
        cephx_entity,
        lockbox_entity,
        ss);
    if (err < 0) {
      return err;
    } else if (may_be_idempotent && err != EEXIST) {
      // for this to be idempotent, `id` should already be >= 0; no need
      // to use validate_id.
      ceph_assert(id >= 0);
      ss << "osd." << id << " exists but secrets do not match";
      return -EEXIST;
    }

    if (has_lockbox) {
      svc = (ConfigKeyService*)mon->config_key_service;
      err = svc->validate_osd_new(uuid, dmcrypt_key, ss);
      if (err < 0) {
        return err;
      } else if (may_be_idempotent && err != EEXIST) {
        ceph_assert(id >= 0);
        ss << "osd." << id << " exists but dm-crypt key does not match.";
        return -EEXIST;
      }
    }
  }
  ceph_assert(!has_secrets || !cephx_secret.empty());
  ceph_assert(!has_lockbox || !lockbox_secret.empty());

  if (may_be_idempotent) {
    // we have nothing to do for either the osdmon or the authmon,
    // and we have no lockbox - so the config key service will not be
    // touched. This is therefore an idempotent operation, and we can
    // just return right away.
    dout(10) << __func__ << " idempotent -- no op." << dendl;
    ceph_assert(id >= 0);
    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", id);
      f->close_section();
    } else {
      ss << id;
    }
    return EEXIST;
  }
  ceph_assert(!may_be_idempotent);

  // perform updates.
  if (has_secrets) {
    ceph_assert(!cephx_secret.empty());
    ceph_assert((lockbox_secret.empty() && dmcrypt_key.empty()) ||
           (!lockbox_secret.empty() && !dmcrypt_key.empty()));

    err = mon->authmon()->do_osd_new(cephx_entity,
        lockbox_entity,
        has_lockbox);
    ceph_assert(0 == err);

    if (has_lockbox) {
      ceph_assert(nullptr != svc);
      svc->do_osd_new(uuid, dmcrypt_key);
    }
  }

  if (is_recreate_destroyed) {
    ceph_assert(id >= 0);
    ceph_assert(osdmap.is_destroyed(id));
    pending_inc.new_weight[id] = CEPH_OSD_OUT;
    pending_inc.new_state[id] |= CEPH_OSD_DESTROYED;
    if ((osdmap.get_state(id) & CEPH_OSD_NEW) == 0) {
      pending_inc.new_state[id] |= CEPH_OSD_NEW;
    }
    if (osdmap.get_state(id) & CEPH_OSD_UP) {
      // due to http://tracker.ceph.com/issues/20751 some clusters may
      // have UP set for non-existent OSDs; make sure it is cleared
      // for a newly created osd.
      pending_inc.new_state[id] |= CEPH_OSD_UP;
    }
    pending_inc.new_uuid[id] = uuid;
  } else {
    ceph_assert(id >= 0);
    int32_t new_id = -1;
    do_osd_create(id, uuid, device_class, &new_id);
    ceph_assert(new_id >= 0);
    ceph_assert(id == new_id);
  }

  if (f) {
    f->open_object_section("created_osd");
    f->dump_int("osdid", id);
    f->close_section();
  } else {
    ss << id;
  }

  return 0;
}

int OSDMonitor::prepare_command_osd_destroy(
    int32_t id,
    stringstream& ss)
{
  ceph_assert(paxos->is_plugged());

  // we check if the osd exists for the benefit of `osd purge`, which may
  // have previously removed the osd. If the osd does not exist, return
  // -ENOENT to convey this, and let the caller deal with it.
  //
  // we presume that all auth secrets and config keys were removed prior
  // to this command being called. if they exist by now, we also assume
  // they must have been created by some other command and do not pertain
  // to this non-existent osd.
  if (!osdmap.exists(id)) {
    dout(10) << __func__ << " osd." << id << " does not exist." << dendl;
    return -ENOENT;
  }

  uuid_d uuid = osdmap.get_uuid(id);
  dout(10) << __func__ << " destroying osd." << id
           << " uuid " << uuid << dendl;

  // if it has been destroyed, we assume our work here is done.
  if (osdmap.is_destroyed(id)) {
    ss << "destroyed osd." << id;
    return 0;
  }

  EntityName cephx_entity, lockbox_entity;
  bool idempotent_auth = false, idempotent_cks = false;

  int err = mon->authmon()->validate_osd_destroy(id, uuid,
                                                 cephx_entity,
                                                 lockbox_entity,
                                                 ss);
  if (err < 0) {
    if (err == -ENOENT) {
      idempotent_auth = true;
    } else {
      return err;
    }
  }

  ConfigKeyService *svc = (ConfigKeyService*)mon->config_key_service;
  err = svc->validate_osd_destroy(id, uuid);
  if (err < 0) {
    ceph_assert(err == -ENOENT);
    err = 0;
    idempotent_cks = true;
  }

  if (!idempotent_auth) {
    err = mon->authmon()->do_osd_destroy(cephx_entity, lockbox_entity);
    ceph_assert(0 == err);
  }

  if (!idempotent_cks) {
    svc->do_osd_destroy(id, uuid);
  }

  pending_inc.new_state[id] = CEPH_OSD_DESTROYED;
  pending_inc.new_uuid[id] = uuid_d();

  // we can only propose_pending() once per service, otherwise we'll be
  // defying PaxosService and all laws of nature. Therefore, as we may
  // be used during 'osd purge', let's keep the caller responsible for
  // proposing.
  ceph_assert(err == 0);
  return 0;
}

int OSDMonitor::prepare_command_osd_purge(
    int32_t id,
    stringstream& ss)
{
  ceph_assert(paxos->is_plugged());
  dout(10) << __func__ << " purging osd." << id << dendl;

  ceph_assert(!osdmap.is_up(id));

  /*
   * This may look a bit weird, but this is what's going to happen:
   *
   *  1. we make sure that removing from crush works
   *  2. we call `prepare_command_osd_destroy()`. If it returns an
   *     error, then we abort the whole operation, as no updates
   *     have been made. However, we this function will have
   *     side-effects, thus we need to make sure that all operations
   *     performed henceforth will *always* succeed.
   *  3. we call `prepare_command_osd_remove()`. Although this
   *     function can return an error, it currently only checks if the
   *     osd is up - and we have made sure that it is not so, so there
   *     is no conflict, and it is effectively an update.
   *  4. finally, we call `do_osd_crush_remove()`, which will perform
   *     the crush update we delayed from before.
   */

  CrushWrapper newcrush;
  _get_pending_crush(newcrush);

  bool may_be_idempotent = false;

  int err = _prepare_command_osd_crush_remove(newcrush, id, 0, false, false);
  if (err == -ENOENT) {
    err = 0;
    may_be_idempotent = true;
  } else if (err < 0) {
    ss << "error removing osd." << id << " from crush";
    return err;
  }

  // no point destroying the osd again if it has already been marked destroyed
  if (!osdmap.is_destroyed(id)) {
    err = prepare_command_osd_destroy(id, ss);
    if (err < 0) {
      if (err == -ENOENT) {
        err = 0;
      } else {
        return err;
      }
    } else {
      may_be_idempotent = false;
    }
  }
  ceph_assert(0 == err);

  if (may_be_idempotent && !osdmap.exists(id)) {
    dout(10) << __func__ << " osd." << id << " does not exist and "
             << "we are idempotent." << dendl;
    return -ENOENT;
  }

  err = prepare_command_osd_remove(id);
  // we should not be busy, as we should have made sure this id is not up.
  ceph_assert(0 == err);

  do_osd_crush_remove(newcrush);
  return 0;
}

int OSDMonitor::prepare_command_osd_remove(int32_t id)
{
  if (osdmap.is_up(id)) {
    return -EBUSY;
  }

  pending_inc.new_state[id] = osdmap.get_state(id);
  pending_inc.new_uuid[id] = uuid_d();
  pending_metadata_rm.insert(id);
  pending_metadata.erase(id);

  return 0;
}


