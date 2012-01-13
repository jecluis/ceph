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

/* 30 chars for a randomly generated file name
 * ought to be enough for anyone */
#define RND_NAME_LEN 30

static char * __generate_filename(void);

static struct option longopts[] = {
		{ "sleep", required_argument, NULL, 's' },
		{ "delay", required_argument, NULL, 'd' },
		{ "threads", required_argument, NULL, 't' },
		{ "init", no_argument, NULL, 'i' },
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
		"        -d, --delay=VAL          Delay (in seconds) between snapshots\n"
		"        -t, --threads=VAL        Number of threads for chmod test\n"
		"        -h, --help               This information\n"
		"\n", name
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

	while (((ch = getopt_long(*argc, argv, "s:d:t:ih", longopts, NULL)) != -1)
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

#if 0
	if (ctl->options.init) {
		// init the whole world
	}
#endif

	str = __generate_filename();
	if (IS_ERR(str))
		return PTR_ERR(str);

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
#if 0
	i = 0;
	while (i < ctl->chmod_threads) {
		if (list_empty(ctl->log_chmod[i])) {
			i ++;
			continue;
		}

		log = list_entry(ctl->log_chmod[i]->next, struct tests_log_chmod, lst);
		if (log->start > tv2ts(&snap->destroy_end)) {
			/* this condition presumes that the last test performed is always
			 * a snapshot (since they take longer). Even if it is not, it will
			 * be negligible, as it will always be only a single chmod more,
			 * and a single chmod performance gets diluted in the whole
			 * universe of chmods ran during the test. (one chmod per thread)
			 */
			i ++;
			continue;
		}

		list_del(&log->lst);
		return log;
	}
#endif
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
	uint64_t sum, total;

	int cnt = 0;
	int i, j;

	if (!ctl) {
		fprintf(stderr, "do_print_results: NULL ctl struct\n");
		return;
	}


	for (i = 0; cnt < ctl->chmod_threads; cnt ++) {
		for (j = 0; j < TESTS_NUM_STATES; j ++)
		chmods_performed += ctl->log_chmod[cnt]->results[j]->latency_total;
	}

	for (lst_snap_ptr = ctl->log_snapshot.next; !list_empty(lst_snap_ptr); ) {

		log_snap = list_entry(lst_snap_ptr, struct tests_log_snapshot, lst);

		for (i = 0; i < TESTS_NUM_STATES; i ++) {

			max = 0;
			min = UINT32_MAX;
			sum = total = 0;

			for (j = 0; j < ctl->chmod_threads; j ++) {

				log_chmod = ctl->log_chmod[j];
				log_chmod_result = log_chmod->results[i];

				if (max < log_chmod_result->latency_max)
					max = log_chmod_result->latency_max;

				if (min > log_chmod_result->latency_min)
					min = log_chmod_result->latency_min;

				sum += log_chmod_result->latency_sum;
				total += log_chmod_result->latency_total;
			}
			printf( "STATE %d:\n"
					"    max (us) = %d\n"
					"    max (s)  = %f\n"
					"    min (us) = %d\n"
					"    min (s)  = %f\n"
					"    avg (us) = %f\n",
					max, ((double) max / 1000000),
					min, ((double) min / 1000000),
					((double) sum) / ((double) total));
		}

		lst_snap_ptr = lst_snap_ptr->next;
		list_del(&log_snap->lst);
		free(log_snap);
	}
#if 0
	cnt = 0;

	//lst_chmod_ptr = ctl->log_chmod.next;

		while ((log_chmod = __results_obtain_chmod(ctl, log_snap))) {

//		}
//		for (; !list_empty(lst_chmod_ptr); ) {
			cnt ++;
			printf("%d / %llu\r", cnt,
					(long long unsigned int) chmods_performed);

//			log_chmod = list_entry(lst_chmod_ptr, struct tests_log_chmod, lst);
			chmod_start_ts = log_chmod->start;
			chmod_end_ts = log_chmod->end;
			chmod_diff = (chmod_end_ts - chmod_start_ts);

			if (chmod_end_ts < tv2ts(&log_snap->create)) {
				unaffected_sum += chmod_diff;
				unaffected_total ++;
				goto lbl_next;
			}

			if (chmod_end_ts < tv2ts(&log_snap->wait_begin)) {
				// add to wait create stats
				affected_create_sum += chmod_diff;
				affected_create_total ++;
				goto lbl_next;
			}

			if (chmod_end_ts < tv2ts(&log_snap->wait_end)) {
				// add to wait stats
				affected_wait_sum += chmod_diff;
				affected_wait_total ++;
				goto lbl_next;
			}

			if (chmod_end_ts < tv2ts(&log_snap->destroy_begin)) {
				if (chmod_start_ts < tv2ts(&log_snap->wait_end)) {
					// add to wait stats
					affected_wait_sum += chmod_diff;
					affected_wait_total ++;
					goto lbl_next;
				}
				// okay
				unaffected_sum += chmod_diff;
				unaffected_total ++;
				goto lbl_next;
			}

			if ((chmod_end_ts < tv2ts(&log_snap->destroy_end))
					|| (chmod_start_ts < tv2ts(&log_snap->destroy_end))) {
				// add to delete stats
				affected_delete_sum += chmod_diff;
				affected_delete_total ++;
				goto lbl_next;
			}

//			continue;

lbl_next:
			//lst_chmod_ptr = lst_chmod_ptr->next;
			//list_del(&log_chmod->lst);
			free(log_chmod);
			continue;
		}
		lst_snap_ptr = lst_snap_ptr->next;
		list_del(&log_snap->lst);
		free(log_snap);
	}

	printf("\n");

	printf("Average time (us) for chmod operations:\n");
	if (unaffected_total > 0) {
		unaffected_avg = ((double) unaffected_sum / unaffected_total);
	}
	if (affected_create_total > 0) {
		affected_create_avg =
				((double) affected_create_sum / affected_create_total);
	}
	if (affected_wait_total > 0) {
		affected_wait_avg =
				((double) affected_wait_sum / affected_wait_total);
	}
	if (affected_delete_total > 0) {
		affected_delete_avg =
				((double) affected_delete_sum / affected_delete_total);
	}

	printf(	"unaffected chmods avg (us): %f\n"
			"affected by create (us):    %f\n"
			"affected by wait (us):      %f\n"
			"affected by delete (us):    %f\n",
			unaffected_avg, affected_create_avg,
			affected_wait_avg, affected_delete_avg);
#endif
#if 0
	printf(	"unaffected chmods avg (us): %llu, %llu\n"
			"affected by create (us):    %llu, %llu\n"
			"affected by wait (us):      %llu, %llu\n"
			"affected by delete (us):    %llu, %llu\n",
			unaffected_sum, unaffected_total,
			affected_create_sum, affected_create_total,
			affected_wait_sum, affected_wait_total,
			affected_delete_sum, affected_delete_total);
#endif
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

	err = do_world_init(&ctl);
	if (err < 0) {
		fprintf(stderr, "initiating world: %s\n", strerror(err));
		goto err_cleanup;
	}

	signal(SIGINT, sighandler);

	gettimeofday(&tv_start, NULL);
	err = do_tests(&ctl);
	if (err < 0) {
		goto err_cleanup;
	}
	gettimeofday(&tv_end, NULL);

	printf("Test ran for %llu usecs\n", 
			(tv2ts(&tv_end) - tv2ts(&tv_start)));

	do_print_results(&ctl);

	return 0;

err_cleanup:
	tests_options_cleanup(&ctl.options);
	return 1;
}

static char * __generate_filename(void)
{
	static const char alphanum[] =
			"0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";

	char * str;
	int i;

	srand(time(NULL));

	str = (char *) malloc(RND_NAME_LEN + 1);
	if (!str)
		return ERR_PTR(-ENOMEM);

	for (i = 0; i < RND_NAME_LEN; i ++) {
		str[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
	}
	str[RND_NAME_LEN] = '\0';

	return str;
}
