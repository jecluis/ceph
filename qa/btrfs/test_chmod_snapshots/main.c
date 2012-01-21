#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <pthread.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <unistd.h>
#include <limits.h>

#include "ctl.h"
#include "err.h"
#include "tests.h"
#include "list.h"


//static char * __generate_filename(void);

static struct option longopts[] = {
		{ "sleep", required_argument, NULL, 's' },
		{ "delay", required_argument, NULL, 'd' },
		{ "threads", required_argument, NULL, 't' },
		{ "runtime", required_argument, NULL, 'r' },
		{ "only", required_argument, NULL, 'o' },
		{ "init", no_argument, NULL, 'i' },
		{ "plot", no_argument, NULL, 'p' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
};

struct tests_ctl ctl;


void print_usage(const char * name)
{
	printf(
		"Ceph Btrfs testss\n"
		"Usage:  %s [options...] <subvolume> <snapshot_name>\n\n"
		"Options:\n"
		"        -i, --init               Randomly initialize the FS\n"
		"        -s, --sleep=VAL          Sleep (in seconds) during snapshots\n"
		"                                 (default: %d)\n"
		"        -d, --delay=VAL          Delay (in seconds) between snapshots\n"
		"                                 (default: %d)\n"
		"        -t, --threads=VAL        Number of threads for chmod test\n"
		"        -r, --runtime=SECS       Run for SECS seconds\n"
		"                                 (default: %d)\n"
		"        -o, --only=[c|s]         Only run the chmod or snapshot test\n"
		"        -p, --plot               Output GnuPlot data instead of usual dump\n"
		"        -h, --help               This information\n"
		"\n", name,
		TESTS_DEFAULT_SNAPSHOT_SLEEP,
		TESTS_DEFAULT_SNAPSHOT_DELAY,
		TESTS_DEFAULT_RUNTIME
	);
}

/**
 * do_getopt - Obtain CLI options and populate 'struct tests_options'.
 */
int do_getopt(int * argc, char ** argv, struct tests_options * options)
{
	int ch, cleanup = 0, err = -EINVAL;
	const char * name;

	if (!argv || !options)
		return -EINVAL;

	name = argv[0];

	tests_options_init(options);

	while (((ch = getopt_long(*argc, argv, "s:d:t:r:o:iph", longopts, NULL)) != -1)
			&& !cleanup) {
		switch (ch) {
		case 'i':
			options->init = 1;
			break;
		case 's':
			options->snap_opts.sleep = strtol(optarg, NULL, 10);
			break;
		case 'd':
			options->snap_opts.delay = strtol(optarg, NULL, 10);
			break;
		case 't':
			options->chmod_opts.num_threads = strtol(optarg, NULL, 10);
			break;
		case 'r':
			options->runtime = strtol(optarg, NULL, 10);
			break;
		case 'o':
			printf("optarg = %s\n", optarg);
			if (*optarg == 'c')
				options->chmod_only = 1;
			else if (*optarg == 's') {
				options->snapshot_only = 1;
				printf("snapshots only!\n");
			}
			break;
		case 'p':
			options->plot = 1;
			break;
		case 'h':
			print_usage(name);
			cleanup = 1;
			err = 0;
			break;
		case '?':
		default:
			fprintf(stderr, "unrecognized option: '%s'\n", argv[optind]);
			print_usage(name);
			cleanup = 1;
			break;
		}
	}
	*argc -= optind;

	if (cleanup) {
		tests_options_cleanup(options);
		return err;
	}

	return 0;
}

int do_world_init(struct tests_ctl * ctl)
{
	char * str;
	int err;

	if (!ctl)
		return -EINVAL;

	str = tests_generate_filename();
//	if (IS_ERR(str))
//		return PTR_ERR(str);
	if (!str)
		return -ENOMEM;

	ctl->options.chmod_opts.filename = str;

	err = creat(ctl->options.chmod_opts.filename, S_IRWXU|S_IRWXG|S_IRWXO);
	if (err < 0) {
		free(str);
		return (-errno);
	}

	return 0;
}

int do_tests(struct tests_ctl * ctl)
{
	int ret;

	if (!ctl)
		return -EINVAL;

	ret = tests_run(ctl);

	return ret;
}

static struct tests_log_chmod * __results_obtain_chmod(struct tests_ctl * ctl,
		struct tests_log_snapshot * snap)
{
	int i;
	struct tests_log_chmod * log = NULL;

	if (!ctl || !snap)
		return NULL;

	return NULL;
}

void do_print_results(struct tests_ctl * ctl)
{
	struct tests_log_chmod * log_chmod;
	struct tests_log_chmod_result * log_chmod_result;
	struct tests_log_snapshot * log_snap;
	uint64_t chmod_start_ts, chmod_end_ts;
	uint64_t chmod_diff;

	struct list_head * lst_chmod_ptr, * lst_snap_ptr;

	uint64_t affected_create_sum = 0, affected_create_total = 0;
	uint64_t affected_wait_sum = 0, affected_wait_total = 0;
	uint64_t affected_delete_sum = 0, affected_delete_total = 0;
	double affected_create_avg = 0, affected_wait_avg = 0;
	double affected_delete_avg = 0;

	uint64_t unaffected_sum = 0;
	uint64_t unaffected_total = 0;
	double unaffected_avg = 0;

	uint64_t chmods_performed = 0;

	uint32_t max, min;
	uint64_t sum, total = 0, plot_sum = 0;

	int cnt = 0;
	int t_i, bucket_i, start_i, end_i;

	if (!ctl) {
		fprintf(stderr, "do_print_results: NULL ctl struct\n");
		return;
	}

	if (ctl->options.plot) {
		printf("# <bucket size> <count>\n");
	}
	for (bucket_i = 0; bucket_i < TESTS_NUM_BUCKETS; bucket_i ++) {
		if (!ctl->options.plot) {
			if (TESTS_BUCKETS_LIMITS[bucket_i]) {
				printf("latency < %d:\n", 
					TESTS_BUCKETS_LIMITS[bucket_i]);
			} else {
				printf("latency >= %d:\n", 
					TESTS_BUCKETS_LIMITS[bucket_i-1]);
			}
		}

		plot_sum = 0;
		for (start_i = 0; start_i < TESTS_NUM_STATES; start_i += 2) {
			for (end_i = 0; end_i < TESTS_NUM_STATES; end_i ++) {

				sum = 0;
				for (t_i = 0; t_i < ctl->chmod_threads; t_i ++) {
					log_chmod = &ctl->log_chmod[t_i];

					cnt = log_chmod->buckets[bucket_i][start_i][end_i];
					if (!cnt)
						continue;

					sum += cnt;
				}

				if ((sum != 0) && (!ctl->options.plot)) {
					printf("    %s -- %s: %lu\n",
							TESTS_STATE_NAME[start_i],
							TESTS_STATE_NAME[end_i], 
							sum);
				} else {
					plot_sum += sum;
				}
				total += sum;
			}
		}

		if (ctl->options.plot) {
			if (bucket_i+1 < TESTS_NUM_BUCKETS) {
				printf("<%d %lu\n", TESTS_BUCKETS_LIMITS[bucket_i], plot_sum);
			} else
				printf(">=%d %lu\n", TESTS_BUCKETS_LIMITS[bucket_i-1], plot_sum);
		}
	}

	printf("\ntotal chmods = %lu\n", total);

	for (lst_snap_ptr = ctl->log_snapshot.next; !list_empty(lst_snap_ptr); ) {

		log_snap = list_entry(lst_snap_ptr, struct tests_log_snapshot, lst);

		lst_snap_ptr = lst_snap_ptr->next;
		list_del(&log_snap->lst);
		free(log_snap);
	}
}

void sighandler(int sig)
{
	printf("Ordering everybody to eventually stop...\n");
	ctl.keep_running = 0;
}

int main(int argc, char ** argv)
{
	int err;

	int p_argc;
	char ** p_argv;
	struct timeval tv_start, tv_end;

	p_argc = argc;

	err = do_getopt(&p_argc, argv, &ctl.options);
	if (err < 0) {
		fprintf(stderr, "unable to obtain options: %s\n", strerror(err));
		return 1;
	}

	if (p_argc < 2) {
		print_usage(argv[0]);
		goto err_cleanup;
	}

	p_argv = argv + (argc - p_argc);

	err = tests_ctl_init(&ctl, p_argv[0], p_argv[1],
			ctl.options.chmod_opts.num_threads);
	if (err < 0) {
		print_usage(argv[0]);
		goto err_cleanup;
	}

	// chdir into subvolume
	err = chdir(ctl.subvolume_path);
	if (err < 0) {
		perror("Changing to subvolume directory");
		goto err_cleanup;
	}

#if 0
	err = do_world_init(&ctl);
	if (err < 0) {
		fprintf(stderr, "initiating world: %s\n", strerror(err));
		goto err_cleanup;
	}
#endif
	signal(SIGINT, sighandler);

	gettimeofday(&tv_start, NULL);
	err = do_tests(&ctl);
	if (err < 0) {
		goto err_cleanup;
	}
	gettimeofday(&tv_end, NULL);

	printf("Test ran for %lu usecs\n", 
			(tv2ts(&tv_end) - tv2ts(&tv_start)));

	do_print_results(&ctl);

	return 0;

err_cleanup:
	tests_options_cleanup(&ctl.options);
	return 1;
}

