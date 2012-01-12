#ifndef CEPH_BTRFS_TESTS_OPTIONS_H_
#define CEPH_BTRFS_TESTS_OPTIONS_H_

#include <stdint.h>
#include "list.h"


/* default values (in seconds), just in case.
 * should be changed as appropriate. */
#define TESTS_SNAPSHOT_DEFAULT_DELAY	10
#define TESTS_SNAPSHOT_DEFAULT_SLEEP	10

struct tests_chmod_opts {
	char * filename;
};

struct tests_snap_opts {

	uint32_t delay;
	uint32_t sleep;
};

struct tests_options {
	char * 					subvolume_path;
	uint8_t 				init;
	uint32_t 				runtime;

	struct tests_chmod_opts	chmod_opts;
	struct tests_snap_opts 	snap_opts;
};


struct tests_ctl {
	struct tests_options	options;

	char * 					subvolume_path;
	char * 					snapshot_name;
	char *					destination_path;

	uint8_t 				keep_running;

	uint64_t 				snaps_created;
	uint64_t 				snaps_destroyed;
	uint64_t 				chmods_performed;

	struct list_head 		log_chmod;
	struct list_head 		log_snapshot;
};

extern void tests_options_cleanup(struct tests_options * options);
extern void tests_options_init(struct tests_options * options);
extern int
tests_ctl_init_paths(struct tests_ctl * ctl, char * subvolume, char * snap);
extern int tests_ctl_init(struct tests_ctl * ctl, char * subvol, char * snap);

#endif /* CEPH_BTRFS_TESTS_OPTIONS_H_ */
