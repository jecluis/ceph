#ifndef CEPH_BTRFS_TESTS_H_
#define CEPH_BTRFS_TESTS_H_

#include <sys/time.h>
#include "list.h"

struct tests_log_chmod {
	struct list_head lst;

	uint64_t start;
	uint64_t end;
};

struct tests_log_snapshot {
	struct list_head lst;

	struct timeval create;
	struct timeval wait_begin;
	struct timeval wait_end;
	struct timeval destroy_begin;
	struct timeval destroy_end;

//	uint8_t op;
//	uint64_t transid;
};

#define TESTS_LOG_SNAP_CREATE	 		(0x01)
#define TESTS_LOG_SNAP_WAIT_BEGIN		(0x02)
#define TESTS_LOG_SNAP_WAIT_END			(0x03)
#define TESTS_LOG_SNAP_DESTROY_BEGIN	(0x04)
#define TESTS_LOG_SNAP_DESTROY_END		(0x05)

int tests_run(struct tests_ctl * ctl);

#endif /* CEPH_BTRFS_TESTS_H_ */
