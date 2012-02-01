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
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>

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
	char * filename;

	pid_t r_tid;
	uint8_t do_out = 0;

	if (!args)
		goto out;

	thread_args = (struct tests_thread_args *) args;

	if (!thread_args->args)
		goto out;

	ctl = (struct tests_ctl *) thread_args->args;

	filename = tests_generate_filename();
	if (!filename)
		goto out;

	err = creat(filename, S_IRWXU|S_IRWXG|S_IRWXO);
	if (err < 0) {
		fprintf(stderr, "creating a file (%s @ %s): %s\n",
				filename, get_current_dir_name(), strerror(errno));
		free(filename);
		goto out;
	}

	r_tid = syscall(SYS_gettid);
	printf("chmod #%d (%d) created '%s'\n", thread_args->tid, r_tid, filename);

	i = 0;
	do {
		err = gettimeofday(&tv_start, NULL);
		if (err < 0) {
			perror("obtaining time of day");
			break;
		}
		state_start = __sync_add_and_fetch(&ctl->current_state, 0);
		version_start = __sync_add_and_fetch(&ctl->current_version, 0);

		if ((state_start == TESTS_STATE_CREATE)
				|| (state_start == TESTS_STATE_WAIT_BEGIN)) {
			do_out = 1;
		} else
			do_out = 0;

		err = chmod(filename, modes[(i ++)%TOTAL_MODES]);
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

		printf("chmod %d > %02.5f, %lu > %s-%s, %d-%d\n",
				r_tid,
				tvdiff2secs(&tv_start, &ctl->start),
				tv2ts(&tv_end)-tv2ts(&tv_start),
				TESTS_STATE_NAME[state_start],
				TESTS_STATE_NAME[state_end],
				version_start, version_end);


		__log_chmod(ctl, thread_args->tid, &tv_start, &tv_end,
				state_start, state_end);
	} while(ctl->keep_running);

	printf("Chmod'ing finished (with %d ops).\n", i);

	free(filename);

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

	if (!ctl || !ctl->subvolume_path || !ctl->destination_path)
		return ERR_PTR(-EINVAL);

	dstdir = ctl->destination_path;

	dstfd = open(dstdir, O_RDONLY);
	if (dstfd < 0) {
		perror("opening subvolume path");
		return ERR_PTR(-errno);
	}

	subvol_fd = open(ctl->subvolume_path, O_RDONLY);
	if (subvol_fd < 0) {
		perror("opening subvolume path");
		goto err_close_dst;
	}

	//printf("dstdir: %s, subvol path: %s\n", dstdir, ctl->subvolume_path);

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

//	printf("Creating snapshot\n");
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

#if 0
	printf("[ %lu.%lu ] snap > latency > create: %d, create+wait: %d\n", 
			snap_log->create.tv_sec, snap_log->create.tv_usec,
			(tv2ts(&snap_log->wait_begin)-tv2ts(&snap_log->create)),
			(tv2ts(&snap_log->wait_end)-tv2ts(&snap_log->create)));
#endif

	printf("snap > %02.5f, %lu, %lu\n",
//			(tv2ts(&snap_log->create)-tv2ts(&ctl->start)),
			tvdiff2secs(&snap_log->create, &ctl->start),
			(tv2ts(&snap_log->wait_begin)-tv2ts(&snap_log->create)),
			(tv2ts(&snap_log->wait_end)-tv2ts(&snap_log->create)));
	
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

	ctl->total_snaps ++;

err_close:
	close(subvol_fd);
err_close_dst:
	close(dstfd);

	if (err < 0) {
		ret = ERR_PTR(-errno);
	}

	return ret;
}

void * tests_run_sync(void * args) 
{
	struct tests_ctl * ctl;
	int fd;
	int err;
	struct timeval tv_start, tv_end;

	if (!args)
		return ERR_PTR(-EINVAL);

	ctl = (struct tests_ctl *) args;

	fd = open(ctl->subvolume_path, O_RDONLY);
	if (fd < 0) {
		perror("tests_run_sync: opening subvolume path");
		return ERR_PTR(-errno);
	}

	gettimeofday(&tv_start, NULL);

	__snapshot_set_state(ctl, TESTS_STATE_SYNC_START);

	err = ioctl(fd, BTRFS_IOC_SYNC);
	if (err < 0) {
		perror("tests_run_sync: sync'ing the file system");
		close(fd);
		return ERR_PTR(-errno);
	}
	gettimeofday(&tv_end, NULL);
	__snapshot_set_state(ctl, TESTS_STATE_SYNC_END);

	printf("sync> %02.5f, %lu\n", 
			tvdiff2secs(&tv_start, &ctl->start),
//			(tv2ts(&tv_start)-tv2ts(&ctl->start)), 
			(tv2ts(&tv_end)-tv2ts(&tv_start)));

	ctl->total_syncs ++;
	return NULL;
}

void * tests_run_file_creation(void * args)
{
	struct tests_thread_args * thread_args;
	struct tests_ctl * ctl;
	int cnt;
	const int file_count = 100;
	double max_ts = 0.0;
	uint32_t max_latency = 0, tmp;
	uint8_t max_state_start, max_state_end;
	struct timeval tv_start, tv_end;
	uint8_t state_start, state_end;
	char * filename;
	pid_t r_tid;
	int err;

	if (!args)
		goto out;

	thread_args = (struct tests_thread_args *) args;

	if (!thread_args->args)
		goto out;

	ctl = (struct tests_ctl *) thread_args->args;

	r_tid = syscall(SYS_gettid);

	cnt = 0;
	do {
		filename = tests_generate_filename();
		if (!filename)
			goto out;

		gettimeofday(&tv_start, NULL);
		state_start = __sync_add_and_fetch(&ctl->current_state, 0);

		err = creat(filename, S_IRWXU|S_IRWXG|S_IRWXO);
		if (err < 0) {
			fprintf(stderr, "creating a file (%s @ %s): %s\n",
					filename, get_current_dir_name(), strerror(errno));
			free(filename);
			goto out;
		}
		gettimeofday(&tv_end, NULL);
		state_end = __sync_add_and_fetch(&ctl->current_state, 0);
		cnt ++;

		__log_chmod(ctl, thread_args->tid, &tv_start, &tv_end,
				state_start, state_end);

		close(err);

		tmp = (tv2ts(&tv_end)-tv2ts(&tv_start));
		if (tmp > max_latency) {
			max_latency = tmp;
			max_ts = tvdiff2secs(&tv_start, &ctl->start);
			max_state_start = state_start;
			max_state_end = state_end;
		}

		if (cnt == file_count) {
			printf("creat %d > %02.5f, %lu (%s-%s)\n", 
					r_tid, max_ts, max_latency,
					TESTS_STATE_NAME[max_state_start],
					TESTS_STATE_NAME[max_state_end]);
			max_ts = max_latency = 0;
			cnt = 0;
			usleep(25000);
		}

	} while (ctl->keep_running);

out:
	pthread_exit(NULL);
}


int tests_run(struct tests_ctl * ctl)
{
	int err;
	void * err_ptr;
	time_t start_time, curr_time;
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
	}


	srand(time(NULL));

	gettimeofday(&ctl->start, NULL);

	if (!(ctl->options.run_tests & TESTS_RUN_CHMOD))
		goto no_chmod;


	for (i = 0; i < ctl->chmod_threads; i ++) {
	
		err = pthread_create(&tid_chmod[i], NULL,
				tests_run_chmod, (void *) args[i]);
		if (err) {
			fprintf(stderr, "Error creating thread %i: %s\n", i, strerror(err));
			return (-err);
		}
	}

no_chmod:

	if (!(ctl->options.run_tests & TESTS_RUN_CREATES))
		goto no_creates;

	
	for (i = 0; i < ctl->chmod_threads; i ++) {

		err = pthread_create(&tid_chmod[i], NULL,
				tests_run_file_creation, (void *) args[i]);
		if (err) {
			fprintf(stderr, "Error creating thread %i: %s\n", i, strerror(err));
			return (-err);
		}
	}


no_creates:

	start_time = time(NULL);
	do {

		__snapshot_set_state(ctl, TESTS_STATE_NONE);

		if (ctl->options.snap_opts.delay) {
			sleep(ctl->options.snap_opts.delay);
		}

		if (ctl->options.run_tests & TESTS_RUN_SNAPS) {

			err_ptr = tests_run_snapshot(ctl);
			if (err_ptr && IS_ERR(err_ptr)) {
				fprintf(stderr, "Aborting due to error on snapshots: %s\n",
						strerror((-PTR_ERR(err_ptr))));
				ctl->keep_running = 0;
				break;
			}
		}

		if (ctl->options.run_tests & TESTS_RUN_SYNCS) {
			err_ptr = tests_run_sync(ctl);
			if (err_ptr && IS_ERR(err_ptr)) {
				fprintf(stderr, "Aborting due to error on sync: %s\n",
						strerror((-PTR_ERR(err_ptr))));
				ctl->keep_running = 0;
				break;
			}
		}

		curr_time = time(NULL);
		if ((curr_time - start_time) >= ctl->options.runtime)
			ctl->keep_running = 0;

	} while(ctl->keep_running);

	if (ctl->options.run_tests & TESTS_RUN_SNAPS)
		printf("Snapshot'ing finished with %d tests\n", ctl->total_snaps);
	if (ctl->options.run_tests & TESTS_RUN_SYNCS)
		printf("Sync'ing finished with %d tests.\n", ctl->total_syncs);

	for (i = 0; i < ctl->chmod_threads; i ++) {
		err = pthread_join(tid_chmod[i], NULL);
		if (err) {
			fprintf(stderr, "Error joining thread %i: %s\n", i, strerror(err));
			return (-err);
		}
	}

	return 0;
}

char * tests_generate_filename(void)
{
	static const char alphanum[] =
			"0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";

	char * str;
	int i;


	str = (char *) malloc(RND_NAME_LEN + 1);
	if (!str)
		return NULL;

	for (i = 0; i < RND_NAME_LEN; i ++) {
		str[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
	}
	str[RND_NAME_LEN] = '\0';

	return str;
}

