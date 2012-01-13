#ifndef CEPH_BTRFS_TESTS_H_
#define CEPH_BTRFS_TESTS_H_

#include <sys/time.h>
#include "list.h"

#define TESTS_STATE_NONE			(0x00)
#define TESTS_STATE_CREATE	 		(0x01)
#define TESTS_STATE_WAIT_BEGIN		(0x02)
#define TESTS_STATE_WAIT_END		(0x03)
#define TESTS_STATE_DESTROY_BEGIN	(0x04)
#define TESTS_STATE_DESTROY_END		(0x05)

#define TESTS_NUM_STATES 6


struct tests_log_chmod_result {
	uint32_t latency_max;
	uint32_t latency_min;

	uint32_t latency_sum;
	uint32_t latency_total;
};

struct tests_log_chmod {
//	struct list_head lst;
//
//	uint64_t start;
//	uint64_t end;

	struct tests_log_chmod_result results[TESTS_NUM_STATES];
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

struct tests_ctl;

int tests_run(struct tests_ctl * ctl);

#endif /* CEPH_BTRFS_TESTS_H_ */
