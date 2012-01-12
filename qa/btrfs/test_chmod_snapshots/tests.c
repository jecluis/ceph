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
#include <libgen.h>
#include <sys/ioctl.h>

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

	log->start = (start->tv_sec*1000000) + (start->tv_usec);
	log->end = (end->tv_sec*1000000) + (end->tv_usec);

	ctl->chmods_performed ++;

	list_add_tail(&log->lst, &ctl->log_chmod);
}

#if 0
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
#endif

static struct tests_log_snapshot * __log_snapshot_start(void)
{
	struct tests_log_snapshot * log;

	log = (struct tests_log_snapshot *) malloc(sizeof(*log));
	return log;
}

static void __log_snapshot_finish(struct tests_ctl * ctl,
		struct tests_log_snapshot * log)
{
	list_add_tail(&log->lst, &ctl->log_snapshot);
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
	int dstfd, subvol_fd;
	char * dstdir;
	size_t len;
	int err;
	void * ret = NULL;

	struct btrfs_ioctl_vol_args_v2	async_vol_args;
	struct btrfs_ioctl_vol_args 	vol_args;

	struct tests_log_snapshot * snap_log;


	ctl = (struct tests_ctl *) args;

	if (!ctl || !ctl->subvolume_path)
		return ERR_PTR(-EINVAL);

	dstdir = ctl->destination_path;

	dstfd = open(dstdir, O_RDONLY);
	if (dstfd < 0) {
		perror("opening subvolume path");
		return ERR_PTR(-errno);
	}

	subvol_fd = open(dstdir, O_RDONLY);
	if (subvol_fd < 0) {
		perror("opening subvolume path");
		goto err_close_dst;
	}

	async_vol_args.fd = subvol_fd;
	async_vol_args.flags = BTRFS_SUBVOL_CREATE_ASYNC;
	async_vol_args.transid = 0;

	len = strlen(ctl->snapshot_name);
	len = (len > BTRFS_SUBVOL_NAME_MAX ? BTRFS_SUBVOL_NAME_MAX : len);

	memset(async_vol_args.name, 0, BTRFS_SUBVOL_NAME_MAX + 1);
	memcpy(async_vol_args.name, ctl->snapshot_name, len);

	snap_log = __log_snapshot_start();
	if (!snap_log) {
		errno = -ENOMEM;
		goto err_close;
	}

	gettimeofday(&snap_log->create, NULL);

	err = ioctl(dstfd, BTRFS_IOC_SNAP_CREATE_V2, &async_vol_args);
	if (err < 0) {
		perror("creating async snapshot");
		goto err_close;
	}

	gettimeofday(&snap_log->wait_begin, NULL);

	err = ioctl(dstfd, BTRFS_IOC_WAIT_SYNC, &async_vol_args.transid);
	if (err < 0) {
		perror("waiting for snapshot");
		goto err_close;
	}
//	ctl->keep_running = 0;

	gettimeofday(&snap_log->wait_end, NULL);

	sleep(ctl->options.snap_opts.sleep);

	vol_args.fd = async_vol_args.fd;
	memcpy(vol_args.name, async_vol_args.name, BTRFS_SUBVOL_NAME_MAX);

	gettimeofday(&snap_log->destroy_begin, NULL);

	err = ioctl(dstfd, BTRFS_IOC_SNAP_DESTROY, &vol_args);
	if (err < 0) {
		perror("destroying snapshot");
		goto err_close;
	}

	gettimeofday(&snap_log->destroy_end, NULL);
	__log_snapshot_finish(ctl, snap_log);

err_close:
	close(subvol_fd);
err_close_dst:
	close(dstfd);

	if (err < 0) {
		ret = ERR_PTR(-errno);
	}

	return ret;
}


int tests_run(struct tests_ctl * ctl)
{
	int err;
	void * err_ptr;
	pthread_t tid_chmod;
	time_t start_time;

	err = pthread_create(&tid_chmod, NULL, tests_run_chmod, (void *) ctl);
	if (err) {
		fprintf(stderr, "Error creating thread: %s\n", strerror(err));
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
			ctl->keep_running = 0;
			break;
		}

		if (ctl->options.runtime > 0) {
			ctl->keep_running =
					((time(NULL) - start_time) < ctl->options.runtime);
		}
	} while(ctl->keep_running);

	printf("Snapshot'ing finished.\n");

	err = pthread_join(tid_chmod, NULL);
	if (err) {
		fprintf(stderr, "Error joining thread: %s\n", strerror(err));
		return (-err);
	}

	return 0;
}
