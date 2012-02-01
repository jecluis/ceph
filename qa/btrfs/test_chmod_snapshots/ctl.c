#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <libgen.h>
#include <limits.h>

#include "ctl.h"
#include "list.h"
#include "tests.h"

void tests_options_cleanup(struct tests_options * options)
{
	if (options) {
		if (options->subvolume_path)
			free(options->subvolume_path);

		if (options->chmod_opts.filename)
			free(options->chmod_opts.filename);
	}
}

void tests_options_init(struct tests_options * options)
{
	if (!options)
		return;

	options->init = 0;
	options->plot = 0;
//	options->chmod_only = 0;
//	options->snapshot_only = 0;

	// default test: chmods & async snapshots
	//options->run_tests = (TESTS_RUN_CHMOD|TESTS_RUN_SNAPS);
	options->run_tests = TESTS_RUN_MODEL_CAS;

	options->subvolume_path = NULL;
	options->runtime = TESTS_DEFAULT_RUNTIME;

	options->chmod_opts.filename = NULL;
	options->chmod_opts.num_threads = 0;

	options->snap_opts.delay = TESTS_DEFAULT_SNAPSHOT_DELAY;
	options->snap_opts.sleep = TESTS_DEFAULT_SNAPSHOT_SLEEP;
}

void tests_ctl_cleanup_paths(struct tests_ctl * ctl)
{
	if (ctl) {
		if (ctl->subvolume_path)
			free(ctl->subvolume_path);
		if (ctl->snapshot_name)
			free(ctl->snapshot_name);
		if (ctl->destination_path)
			free(ctl->destination_path);
	}
}

int tests_ctl_init_paths(struct tests_ctl * ctl, char * subvolume, char * snap)
{
	size_t len;
	char * tmp_str;

	if (!ctl || !subvolume || !snap)
		return -EINVAL;

	len = strlen(subvolume);
	ctl->subvolume_path = malloc(len*sizeof(*ctl->subvolume_path) + 1);
	if (!ctl->subvolume_path)
		return -ENOMEM;

	memcpy(ctl->subvolume_path, subvolume, len);
	ctl->subvolume_path[len] = '\0';

	len = strlen(snap);
	ctl->snapshot_name = malloc(len*sizeof(*ctl->snapshot_name) + 1);
	if (!ctl->snapshot_name) {
		free(ctl->subvolume_path);
		return -ENOMEM;
	}

	memcpy(ctl->snapshot_name, snap, len);
	ctl->snapshot_name[len] = '\0';

	tmp_str = strdup(ctl->subvolume_path);
	ctl->destination_path = dirname(tmp_str);

	return 0;
}

int tests_ctl_init(struct tests_ctl * ctl, char * subvol, char * snap,
		int chmod_threads)
{
	int err;
	int i, j;
	size_t size;

	if (!ctl || !subvol || !snap)
		return -EINVAL;

	err = tests_ctl_init_paths(ctl, subvol, snap);
	if (err < 0)
		return err;

	ctl->keep_running = 1;
	ctl->chmod_threads = (chmod_threads ? chmod_threads : 1);
	ctl->current_state = TESTS_STATE_NONE;
	ctl->current_version = 0;

	ctl->total_snaps = 0;
	ctl->total_syncs = 0;

	err = -ENOMEM;
	size = sizeof(*ctl->log_chmod) * ctl->chmod_threads;
	ctl->log_chmod = (struct tests_log_chmod *)	malloc(size);
	if (!ctl->log_chmod) {
		tests_ctl_cleanup_paths(ctl);
		goto out;
	}
	memset(ctl->log_chmod, 0, size);

	printf("log size: %d\n", size);
	printf("pos[0][0][0]: %d\n", ctl->log_chmod[0].buckets[0][0][0]);

	for (i = 0; i < ctl->chmod_threads; i ++) {
		ctl->log_chmod[i].max = 0;
		ctl->log_chmod[i].min = UINT32_MAX;
	}

	INIT_LIST_HEAD(&ctl->log_snapshot);

	err = 0;

out:
	return err;
}
