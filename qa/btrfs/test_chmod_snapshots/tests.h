#ifndef CEPH_BTRFS_TESTS_H_
#define CEPH_BTRFS_TESTS_H_

#include <sys/time.h>
#include "list.h"

/* 30 chars for a randomly generated file name
 * ought to be enough for anyone */
#define RND_NAME_LEN 30


#define TESTS_STATE_NONE			(0x00)
#define TESTS_STATE_NEXT_NONE		(0x01)
#define TESTS_STATE_CREATE	 		(0x02)
#define TESTS_STATE_CREATE_NEXT		(0x03)
#define TESTS_STATE_WAIT_BEGIN		(0x04)
#define TESTS_STATE_WAIT_BEGIN_NEXT (0x05)
#define TESTS_STATE_WAIT_END		(0x06)
#define TESTS_STATE_WAIT_END_NEXT	(0x07)
#define TESTS_STATE_DESTROY_BEGIN	(0x08)
#define TESTS_STATE_DB_NEXT			(0x09)
#define TESTS_STATE_POST_DESTROY	(0x0A)
#define TESTS_STATE_PD_NEXT			(0x0B)

#define TESTS_NUM_STATES 			(0x0B+1)

#define TESTS_NUM_BUCKETS			13

static const int TESTS_BUCKETS_LIMITS[TESTS_NUM_BUCKETS] = {
		50, 100, 200,
		1000,
		5000,
		10000,
		50000,
		100000,
		200000,
		500000,
		1000000,
		2000000,
		0
};


static const char * TESTS_STATE_NAME[TESTS_NUM_STATES] = {
	"NONE",
	"NONE (NEXT)",
	"CREATE",
	"CREATE (NEXT)",
	"WAIT BEGIN",
	"WAIT BEGIN (NEXT)",
	"WAIT END",
	"WAIT END (NEXT)",
	"DESTROY BEGIN",
	"DESTROY BEGIN (NEXT)",
	"POST DESTROY",
	"POST DESTROY (NEXT)",
};

struct tests_log_chmod_result {
	uint32_t latency_max;
	uint32_t latency_min;

	uint32_t latency_sum;
	uint32_t latency_total;
};

struct tests_log_chmod {
	uint32_t buckets[TESTS_NUM_BUCKETS][TESTS_NUM_STATES][TESTS_NUM_STATES];

	uint32_t max;
	uint32_t min;

	uint32_t total_latency;
};

struct tests_log_snapshot {
	struct list_head lst;

	struct timeval create;
	struct timeval wait_begin;
	struct timeval wait_end;
	struct timeval destroy_begin;
	struct timeval destroy_end;
};

struct tests_ctl;

int tests_run(struct tests_ctl * ctl);
char * tests_generate_filename(void);

#endif /* CEPH_BTRFS_TESTS_H_ */
