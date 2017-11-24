#!/bin/bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

# We are going to open and close a lot of files, and generate a lot of maps
# that the osds will need to process. If we don't increase the fd ulimit, we
# risk having the osds asserting when handling filestore transactions.
ulimit -n 4096

function run() {

  local dir=$1
  shift

  export CEPH_MON="127.0.0.1:7115"
  export CEPH_ARGS
  CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none --mon-host=$CEPH_MON "

  local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
  for func in $funcs; do
    setup $dir || return 1
    $func $dir || return 1
    teardown $dir || return 1
  done
}


function wait_for_osdmap_manifest() {

  local what=${1:-"true"}

  local -a delays=($(get_timeout_delays $TIMEOUT .1))
  local -i loop=0

  for ((i=0; i < ${#delays[*]}; ++i)); do
    has_manifest=$(ceph report | jq 'has("osdmap_manifest")')
    if [[ "$has_manifest" == "$what" ]]; then
      return 0
    fi

    sleep ${delays[$i]}
  done

  echo "osdmap_manifest never outputted on report"
  ceph report
  return 1
}

function wait_for_trim() {

  local -i epoch=$1
  local -a delays=($(get_timeout_delays $TIMEOUT .1))
  local -i loop=0

  for ((i=0; i < ${#delays[*]}; ++i)); do
    fc=$(ceph report | jq '.osdmap_first_committed')
    if [[ $fc -eq $epoch ]]; then
      return 0
    fi
    sleep ${delays[$i]}
  done

  echo "never trimmed up to epoch $epoch"
  ceph report
  return 1
}

function test_osdmap() {

  local epoch=$1
  local ret=0

  tmp_map=$(mktemp)
  ceph osd getmap $epoch -o $tmp_map || return 1
  if ! osdmaptool --print $tmp_map | grep "epoch $epoch" ; then
    echo "ERROR: failed processing osdmap epoch $epoch"
    ret=1
  fi
  rm $tmp_map
  return $ret
}

function TEST_osdmap_prune() {

  local dir=$1

  run_mon $dir a || return 1
  run_mgr $dir x || return 1
  run_osd $dir 0 || return 1
  run_osd $dir 1 || return 1
  run_osd $dir 2 || return 1

  sleep 5

  # we are getting OSD_OUT_OF_ORDER_FULL health errors, and it's not clear
  # why. so, to make the health checks happy, mask those errors.
  ceph osd set-full-ratio 0.97
  ceph osd set-backfillfull-ratio 0.97

  ceph tell osd.* injectargs '--osd-beacon-report-interval 10' || return 1

  create_pool foo 32
  wait_for_clean || return 1
  wait_for_health_ok || return 1

  ceph tell mon.a injectargs \
    '--mon-debug-block-osdmap-trim '\
    '--mon-debug-extra-checks' || return 1

  CEPH_ARGS= ceph daemon $(get_asok_path mon.a) test generate osdmap 500 || return 1

  report="$(ceph report)"
  fc=$(jq '.osdmap_first_committed' <<< $report)
  lc=$(jq '.osdmap_last_committed' <<< $report)

  [[ $((lc-fc)) -ge 500 ]] || return 1

  ceph tell mon.a injectargs \
    '--mon-min-osdmap-epochs=100 '\
    '--mon-osdmap-full-prune-enabled '\
    '--mon-osdmap-full-prune-min 200 '\
    '--mon-osdmap-full-prune-interval 10 '\
    '--mon-osdmap-full-prune-txsize 100' || return 1

  wait_for_osdmap_manifest || return 1

  manifest="$(ceph report | jq '.osdmap_manifest')"

  first_pinned=$(jq '.first_pinned' <<< $manifest)
  last_pinned=$(jq '.last_pinned' <<< $manifest)
  pinned_maps=( $(jq '.pinned_maps[]' <<< $manifest) )

  # validate pinned maps list
  [[ $first_pinned -eq ${pinned_maps[0]} ]] || return 1
  [[ $last_pinned -eq ${pinned_maps[-1]} ]] || return 1

  # validate pinned maps range
  [[ $first_pinned -lt $last_pinned ]] || return 1
  [[ $last_pinned -lt $lc ]] || return 1
  [[ $first_pinned -eq $fc ]] || return 1

  # ensure all the maps are available, and work as expected
  # this can take a while...

  for ((i=$first_pinned; i <= $last_pinned; ++i)); do
    test_osdmap $i || return 1
  done

  # update pinned maps state:
  #  the monitor may have pruned & pinned additional maps since we last
  #  assessed state, given it's an iterative process.
  #
  manifest="$(ceph report | jq '.osdmap_manifest')"
  first_pinned=$(jq '.first_pinned' <<< $manifest)
  last_pinned=$(jq '.last_pinned' <<< $manifest)
  pinned_maps=( $(jq '.pinned_maps[]' <<< $manifest) )

  # test trimming maps
  #
  # we're going to perform the following tests:
  #
  #  1. force trim to a pinned map
  #  2. force trim to a pinned map's previous epoch
  #  3. trim all maps except the last 200 or so.
  #

  # 1. force trim to a pinned map
  #
  [[ ${#pinned_maps[@]} -gt 10 ]] || return 1

  trim_to=${pinned_maps[1]}
  ceph tell mon.a injectargs --mon-osd-force-trim-to=$trim_to
  ceph tell mon.a injectargs \
    '--mon-min-osdmap-epochs=100 '\
    '--paxos-service-trim-min=1' \
    '--paxos-debug-block-osdmap=false'
  ceph tell mon.a injectargs --mon-debug-block-osdmap-trim=false

  # generate an epoch so we get to trim maps
  ceph osd set noup
  ceph osd unset noup

  wait_for_trim $trim_to || return 1

  report="$(ceph report)"
  fc=$(jq '.osdmap_first_committed' <<< $report)
  [[ $fc -eq $trim_to ]] || return 1

  old_first_pinned=$first_pinned
  old_last_pinned=$last_pinned
  first_pinned=$(jq '.osdmap_manifest.first_pinned' <<< $report)
  last_pinned=$(jq '.osdmap_manifest.last_pinned' <<< $report)
  [[ $first_pinned -eq $trim_to ]] || return 1
  [[ $first_pinned -gt $old_first_pinned ]] || return 1
  [[ $last_pinned -gt $old_first_pinned ]] || return 1

  test_osdmap $trim_to || return 1
  test_osdmap $(( trim_to+1 )) || return 1

  pinned_maps=( $(jq '.osdmap_manifest.pinned_maps[]' <<< $report) )

  # 2. force trim to a pinned map's previous epoch
  #
  [[ ${#pinned_maps[@]} -gt 2 ]] || return 1
  trim_to=$(( ${pinned_maps[1]} - 1))
  ceph tell mon.a injectargs --mon-osd-force-trim-to=$trim_to

  # generate an epoch so we get to trim maps
  ceph osd set noup
  ceph osd unset noup

  wait_for_trim $trim_to || return 1

  report="$(ceph report)"
  fc=$(jq '.osdmap_first_committed' <<< $report)
  [[ $fc -eq $trim_to ]] || return 1

  old_first_pinned=$first_pinned
  old_last_pinned=$last_pinned
  first_pinned=$(jq '.osdmap_manifest.first_pinned' <<< $report)
  last_pinned=$(jq '.osdmap_manifest.last_pinned' <<< $report)
  pinned_maps=( $(jq '.osdmap_manifest.pinned_maps[]' <<< $report) )
  [[ $first_pinned -eq $trim_to ]] || return 1
  [[ ${pinned_maps[1]} -eq $(( trim_to+1)) ]] || return 1

  test_osdmap $first_pinned || return 1
  test_osdmap $(( first_pinned + 1 )) || return 1

  # 3. trim everything
  #
  ceph tell mon.a injectargs --mon-osd-force-trim-to=0

  # generate an epoch so we get to trim maps
  ceph osd set noup
  ceph osd unset noup

  wait_for_osdmap_manifest "false" || return 1

  return 0
}

main mon-osdmap-prune "$@"

