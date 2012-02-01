#ifndef CEPH_BTRFS_TESTS_OPTIONS_H_
#define CEPH_BTRFS_TESTS_OPTIONS_H_

#include <stdint.h>
#include <time.h>
#include "list.h"
#include "tests.h"

/* default values (in seconds), just in case.
 * should be changed as appropriate. */
#define TESTS_DEFAULT_SNAPSHOT_DELAY	10
#define TESTS_DEFAULT_SNAPSHOT_SLEEP	10
#define TESTS_DEFAULT_RUNTIME			60 /* in seconds */


#define TESTS_RUN_CHMOD					0x01
#define TESTS_RUN_SNAPS					0x02
#define TESTS_RUN_SYNCS					0x04
#define TESTS_RUN_CREATES				0x08

#define TESTS_RUN_MODEL_CAS				(TESTS_RUN_CHMOD|TESTS_RUN_SNAPS)
#define TESTS_RUN_MODEL_CS				(TESTS_RUN_CHMOD|TESTS_RUN_SYNCS)
#define TESTS_RUN_MODEL_FCAS			(TESTS_RUN_CREATES|TESTS_RUN_SNAPS)

struct tests_chmod_opts {
	char * filename;
	int num_threads;
};

struct tests_snap_opts {

	uint32_t delay;
	uint32_t sleep;
};

struct tests_options {
	char * 					subvolume_path;
	uint8_t 				init;
	uint8_t					plot;
//	uint8_t 				chmod_only;
//	uint8_t					snapshot_only;
	uint32_t				runtime;

	uint8_t					run_tests;

	struct tests_chmod_opts	chmod_opts;
	struct tests_snap_opts 	snap_opts;
};


struct tests_ctl {
	struct tests_options	options;

	char * 					subvolume_path;
	char * 					snapshot_name;
	char *					destination_path;

	uint8_t 				keep_running;

	uint8_t					current_state;
	uint32_t				current_version;

	int						chmod_threads;

	struct timeval			start;
	uint32_t				total_snaps;
	uint32_t				total_syncs;

	struct tests_log_chmod * log_chmod;

	struct list_head 		log_snapshot;
};

static inline uint64_t tv2ts(struct timeval * tv)
{
	return ((tv->tv_sec * 1000000) + tv->tv_usec);
}

static inline float tvdiff2secs(struct timeval * a, struct timeval * b)
{
	return ((((double) tv2ts(a))-((double) tv2ts(b)))/1000000);
}

extern void tests_options_cleanup(struct tests_options * options);
extern void tests_options_init(struct tests_options * options);
extern int
tests_ctl_init_paths(struct tests_ctl * ctl, char * subvolume, char * snap);
extern int tests_ctl_init(struct tests_ctl * ctl, char * subvol, char * snap,
		int chmod_threads);



#endif /* CEPH_BTRFS_TESTS_OPTIONS_H_ */
