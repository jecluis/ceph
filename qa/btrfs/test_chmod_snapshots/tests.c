#define _GNU_SOURCE
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

struct tests_thread_args {
	void * args;
	int tid;
};

#define TOTAL_MODES 12

static mode_t modes[TOTAL_MODES] = {
		S_ISUID, S_ISGID, S_ISVTX,
		S_IRUSR, S_IWUSR, S_IXUSR,
		S_IRGRP, S_IWGRP, S_IXGRP,
		S_IROTH, S_IWOTH, S_IXOTH
};


static int __get_bucket(uint32_t latency) {

	int i;

	for (i = 0; i < TESTS_NUM_BUCKETS; i ++) {
		if (latency < TESTS_BUCKETS_LIMITS[i])
			return i;
	}

	return TESTS_NUM_BUCKETS - 1;
}

static void __log_chmod(struct tests_ctl * ctl, int tid,
		struct timeval * start, struct timeval * end,
		uint8_t start_state, uint8_t end_state)
{
	struct tests_log_chmod * log;
	uint32_t latency;

	if (!ctl || !start || !end)
		return;

	latency = (uint32_t) (tv2ts(end) - tv2ts(start));

	log = &ctl->log_chmod[tid];
	log->buckets[__get_bucket(latency)][start_state][end_state] ++;

	if (log->max < latency)
		log->max = latency;
	if (log->min > latency)
		log->min = latency;
	log->total_latency += latency;
}

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

static void __snapshot_set_state(struct tests_ctl * ctl, uint8_t state)
{
	if (!ctl)
		return;

	uint8_t curr_state = ctl->current_state;
	__sync_val_compare_and_swap(&ctl->current_state, curr_state, state);
}


static void * tests_run_chmod(void * args)
{
	struct tests_thread_args * thread_args;
	struct tests_ctl * ctl;
	struct timeval tv_start, tv_end;
	int err;
	int i;
	uint8_t state_start, state_end;
	uint32_t version_start, version_end;

	if (!args)
		goto out;

	thread_args = (struct tests_thread_args *) args;

	if (!thread_args->args)
		goto out;

	ctl = (struct tests_ctl *) thread_args->args;

	i = 0;
	do {
		err = gettimeofday(&tv_start, NULL);
		if (err < 0) {
			perror("obtaining time of day");
			break;
		}
		state_start = __sync_add_and_fetch(&ctl->current_state, 0);
		version_start = __sync_add_and_fetch(&ctl->current_version, 0);

		err = chmod(ctl->options.chmod_opts.filename,
				modes[(i ++)%TOTAL_MODES]);
		if (err < 0) {
		    fprintf(stderr, "chmod'ing %s @ %s: %s\n", 
			    ctl->options.chmod_opts.filename,
			    get_current_dir_name(),
			    strerror(errno));
			break;
		}

		err = gettimeofday(&tv_end, NULL);
		if (err < 0) {
			perror("obtaining time of day");
			break;
		}
		state_end = __sync_add_and_fetch(&ctl->current_state, 0);
		version_end = __sync_add_and_fetch(&ctl->current_version, 0);

		if (version_end > version_start) {
			printf("chmod > start state: %s (%d), end state: %s (%d)\n",
					TESTS_STATE_NAME[state_start], version_start,
					TESTS_STATE_NAME[state_end], version_end);
			state_end = state_end + 1;
		}

		__log_chmod(ctl, thread_args->tid, &tv_start, &tv_end,
				state_start, state_end);
	} while(ctl->keep_running);

	printf("Chmod'ing finished (with %d ops).\n", i);


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

	printf("Creating snapshot\n");
	gettimeofday(&snap_log->create, NULL);
	__snapshot_set_state(ctl, TESTS_STATE_CREATE);

	err = ioctl(dstfd, BTRFS_IOC_SNAP_CREATE_V2, &async_vol_args);
	if (err < 0) {
		perror("creating async snapshot");
		goto err_close;
	}

	gettimeofday(&snap_log->wait_begin, NULL);
	__snapshot_set_state(ctl, TESTS_STATE_WAIT_BEGIN);

	err = ioctl(dstfd, BTRFS_IOC_WAIT_SYNC, &async_vol_args.transid);
	if (err < 0) {
		perror("waiting for snapshot");
		goto err_close;
	}

	gettimeofday(&snap_log->wait_end, NULL);
	__snapshot_set_state(ctl, TESTS_STATE_WAIT_END);
	__sync_fetch_and_add(&ctl->current_version, 1);
	
	sleep(ctl->options.snap_opts.sleep);

	vol_args.fd = async_vol_args.fd;
	memcpy(vol_args.name, async_vol_args.name, BTRFS_SUBVOL_NAME_MAX);

	gettimeofday(&snap_log->destroy_begin, NULL);
	__snapshot_set_state(ctl, TESTS_STATE_DESTROY_BEGIN);

	err = ioctl(dstfd, BTRFS_IOC_SNAP_DESTROY, &vol_args);
	if (err < 0) {
		perror("destroying snapshot");
		goto err_close;
	}

	gettimeofday(&snap_log->destroy_end, NULL);
	__log_snapshot_finish(ctl, snap_log);
	__snapshot_set_state(ctl, TESTS_STATE_POST_DESTROY);

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
	time_t start_time;
	pthread_t * tid_chmod;
	struct tests_thread_args ** args;
	int i;

	if (!ctl) {
		fprintf(stderr, "tests_run: NULL ctl\n");
		return -EINVAL;
	}

	tid_chmod = (pthread_t *) malloc(sizeof(*tid_chmod)*ctl->chmod_threads);
	if (!tid_chmod)
		return -ENOMEM;

	args = (struct tests_thread_args **)
			malloc(sizeof(*args)*ctl->chmod_threads);
	if (!args) {
		free(tid_chmod);
		return -ENOMEM;
	}

	for (i = 0; i < ctl->chmod_threads; i ++) {
		args[i] = (struct tests_thread_args *) malloc(sizeof(*args[i]));
		if (!args[i]) {
			return -ENOMEM;
		}
		args[i]->args = (void *) ctl;
		args[i]->tid = i;


		err = pthread_create(&tid_chmod[i], NULL,
				tests_run_chmod, (void *) args[i]);
		if (err) {
			fprintf(stderr, "Error creating thread %i: %s\n", i, strerror(err));
			return (-err);
		}
	}

	start_time = time(NULL);
	do {

		__snapshot_set_state(ctl, TESTS_STATE_NONE);

		if (ctl->options.snap_opts.delay) {
			sleep(ctl->options.snap_opts.delay);
		}

		if (ctl->options.chmod_only)
			continue;

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

	for (i = 0; i < ctl->chmod_threads; i ++) {
		err = pthread_join(tid_chmod[i], NULL);
		if (err) {
			fprintf(stderr, "Error joining thread %i: %s\n", i, strerror(err));
			return (-err);
		}
	}

	return 0;
}
