#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "ctl.h"
#include "list.h"

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
	options->runtime = 0;
	options->subvolume_path = NULL;

	options->chmod_opts.filename = NULL;

	options->snap_opts.delay = TESTS_SNAPSHOT_DEFAULT_DELAY;
	options->snap_opts.sleep = TESTS_SNAPSHOT_DEFAULT_SLEEP;
}

int tests_ctl_init_paths(struct tests_ctl * ctl, char * subvolume, char * snap)
{
	size_t len;

	if (!ctl || !subvolume || !snap)
		return -EINVAL;

	len = strlen(subvolume);
	ctl->subvolume_path = malloc(len*sizeof(ctl->subvolume_path) + 1);
	if (!ctl->subvolume_path)
		return -ENOMEM;

	memcpy(ctl->subvolume_path, subvolume, len);
	ctl->subvolume_path[len] = '\0';

	len = strlen(snap);
	ctl->snapshot_path = malloc(len*sizeof(ctl->snapshot_path) + 1);
	if (!ctl->snapshot_path) {
		free(ctl->subvolume_path);
		return -ENOMEM;
	}

	memcpy(ctl->snapshot_path, snap, len);
	ctl->snapshot_path[len] = '\0';

	return 0;
}

int tests_ctl_init(struct tests_ctl * ctl, char * subvol, char * snap)
{
	int err;

	if (!ctl || !subvol || !snap)
		return -EINVAL;

	err = tests_ctl_init_paths(ctl, subvol, snap);
	if (err < 0)
		return err;

	ctl->keep_running = 1;
	ctl->chmods_performed = 0;
	ctl->snaps_created = 0;
	ctl->snaps_destroyed = 0;

	INIT_LIST_HEAD(&ctl->log_chmod);
	INIT_LIST_HEAD(&ctl->log_snapshot);

	return 0;
}
