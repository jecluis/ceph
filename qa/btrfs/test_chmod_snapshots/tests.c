#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <string.h>
#include <errno.h>

#if defined(__linux__) || defined(__FreeBSD__)
#include <linux/ioctl.h>
#include <linux/types.h>
#include "../../../src/os/btrfs_ioctl.h"
#endif

#include "ctl.h"
#include "err.h"
#include "tests.h"


#define TOTAL_MODES 12

static mode_t modes[TOTAL_MODES] = {
		S_ISUID, S_ISGID, S_ISVTX,
		S_IRUSR, S_IWUSR, S_IXUSR,
		S_IRGRP, S_IWGRP, S_IXGRP,
		S_IROTH, S_IWOTH, S_IXOTH
};

static void __log_chmod(struct tests_ctl * ctl,
		struct timeval * start, struct timeval * end)
{
	struct tests_log_chmod * log;

	if (!ctl || !start || !end)
		return;

	log = (struct tests_log_chmod *) malloc(sizeof(*log));
	if (!log)
		return;

	memcpy(&log->start, &start, sizeof(*start));
	memcpy(&log->end, &end, sizeof(*end));

	list_add_tail(&log->lst, &ctl->log_chmod);
}

static void __log_snapshot(struct tests_ctl * ctl,
		struct timeval * ts, uint8_t op, uint64_t transid)
{
	struct tests_log_snapshot * log;

	if (!ctl || !ts)
		return;

	log = (struct tests_log_snapshot *) malloc(sizeof(*log));
	if (!log)
		return;

	memcpy(&log->timestamp, ts, sizeof(*ts));
	log->op = op;
	log->transid = transid;

	list_add_tail(&log->lst, &ctl->log_snapshot);
}

static void __log_snapshot_now(struct tests_ctl * ctl,
		uint8_t op, uint64_t transid)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	__log_snapshot(ctl, &tv, op, transid);
}

static void * tests_run_chmod(void * args)
{
	struct tests_ctl * ctl;
	struct timeval tv_start, tv_end;
	int err;
	int i;

	if (!args)
		goto out;

	ctl = (struct tests_ctl *) args;

	i = 0;
	do {
		err = gettimeofday(&tv_start, NULL);
		if (err < 0) {
			perror("obtaining time of day");
			break;
		}

		err = chmod(ctl->options.chmod_opts.filename,
				modes[(i ++)%TOTAL_MODES]);
		if (err < 0) {
			perror("chmod'ing");
			break;
		}

		err = gettimeofday(&tv_end, NULL);
		if (err < 0) {
			perror("obtaining time of day");
			break;
		}

		__log_chmod(ctl, &tv_start, &tv_end);
	} while(ctl->keep_running);

	printf("Chmod'ing finished.\n");


out:
	pthread_exit(NULL);
}

static void * tests_run_snapshot(void * args)
{
	struct tests_ctl * ctl;
	int subvol_fd;
//	char * snap_name;
//	uint64_t transid;
	size_t len;
	int err;

	struct btrfs_ioctl_vol_args_v2	async_vol_args;
	struct btrfs_ioctl_vol_args 	vol_args;

	ctl = (struct tests_ctl *) args;

	if (!ctl || !ctl->subvolume_path)
		return ERR_PTR(-EINVAL);

	subvol_fd = open(ctl->subvolume_path, O_RDONLY);
	if (subvol_fd < 0) {
		perror("opening subvolume path");
		return ERR_PTR(-errno);
	}


	async_vol_args.fd = subvol_fd;
	async_vol_args.flags = BTRFS_SUBVOL_CREATE_ASYNC;
	async_vol_args.transid = 0;

	len = strlen(ctl->snapshot_path);
	len = (len > BTRFS_SUBVOL_NAME_MAX ? BTRFS_SUBVOL_NAME_MAX : len);

	memset(async_vol_args.name, 0, BTRFS_SUBVOL_NAME_MAX + 1);
	memcpy(async_vol_args.name, ctl->snapshot_path, len);

	__log_snapshot_now(ctl, TESTS_LOG_SNAP_CREATE, 0);

	err = ioctl(subvol_fd, BTRFS_IOC_SNAP_CREATE_V2, &async_vol_args);
	if (err < 0) {
		perror("creating async snapshot");
		return ERR_PTR(-errno);
	}

	__log_snapshot_now(ctl, TESTS_LOG_SNAP_WAIT_BEGIN, async_vol_args.transid);

	err = ioctl(subvol_fd, BTRFS_IOC_WAIT_SYNC, &async_vol_args.transid);
	if (err < 0) {
		perror("waiting for snapshot");
		return ERR_PTR(-errno);
	}

	__log_snapshot_now(ctl, TESTS_LOG_SNAP_WAIT_END, async_vol_args.transid);

	sleep(ctl->options.snap_opts.sleep);

	vol_args.fd = subvol_fd;
	memcpy(vol_args.name, async_vol_args.name, BTRFS_SUBVOL_NAME_MAX);

	__log_snapshot_now(ctl, TESTS_LOG_SNAP_DESTROY_BEGIN, 
			async_vol_args.transid);

	err = ioctl(subvol_fd, BTRFS_IOC_SNAP_DESTROY, &vol_args);
	if (err < 0) {
		perror("destroying snapshot");
		return ERR_PTR(-errno);
	}

	__log_snapshot_now(ctl, TESTS_LOG_SNAP_DESTROY_END, 
			async_vol_args.transid);

	return NULL;
}


int tests_run(struct tests_ctl * ctl)
{
	int err;
	void * err_ptr;
	pthread_t tid_chmod;
	time_t start_time;

	err = pthread_create(&tid_chmod, NULL, tests_run_chmod, (void *) ctl);
	if (err) {
		return (-err);
	}

	start_time = time(NULL);
	do {
		if (ctl->options.snap_opts.delay)
			sleep(ctl->options.snap_opts.delay);

		err_ptr = tests_run_snapshot(ctl);
		if (err_ptr && IS_ERR(err_ptr)) {
			fprintf(stderr, "Aborting due to error on snapshots: %s\n",
					strerror((-PTR_ERR(err_ptr))));
			ctl->keep_running = 1;
			break;
		}

		if (ctl->options.runtime > 0) {
			ctl->keep_running =
					((time(NULL) - start_time) < ctl->options.runtime);
		}
	} while(ctl->keep_running);

	printf("Snapshot'ing finished.\n");


	return 0;
}
