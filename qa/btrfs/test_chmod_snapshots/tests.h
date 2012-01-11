#ifndef CEPH_BTRFS_TESTS_H_
#define CEPH_BTRFS_TESTS_H_

#include <sys/time.h>
#include "list.h"

struct tests_log_chmod {
	struct list_head lst;

	struct timeval start;
	struct timeval end;
};

int tests_run(struct tests_ctl * ctl);

#endif /* CEPH_BTRFS_TESTS_H_ */
