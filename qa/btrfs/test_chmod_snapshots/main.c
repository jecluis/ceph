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
		{ "init", no_argument, NULL, 'i' },
		{ "help", no_argument, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
};

struct tests_ctl ctl;


void print_usage(const char * name)
{
	printf(
		"Ceph Btrfs testss\n"
		"Usage:  %s [options...] <subvolume> <snapshot>\n\n"
		"Options:\n"
		"        -i, --init               Randomly initialize the FS\n"
		"        -s, --sleep=VAL          Sleep (in seconds) during snapshots\n"
		"        -d, --delay=VAL          Delay (in seconds) between snapshots\n"
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

	while (((ch = getopt_long(*argc, argv, "s:d:ih", longopts, NULL)) != -1)
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

static inline uint64_t tv2ts(struct timeval * tv)
{
	return ((tv->tv_sec * 1000000) + tv->tv_usec);
}

void do_print_results(struct tests_ctl * ctl)
{
	struct tests_log_chmod * log_chmod;
	struct tests_log_snapshot * log_snap;
	uint64_t chmod_start_ts, chmod_end_ts;
	uint64_t chmod_diff;

	struct list_head * lst_chmod_ptr, * lst_snap_ptr;

	uint64_t affected_create_sum = 0, affected_create_total = 0;
	uint64_t affected_wait_sum = 0, affected_wait_total = 0;
	uint64_t affected_delete_sum = 0, affected_delete_total = 0;

	uint64_t unaffected_sum = 0;
	uint64_t unaffected_total = 0;

	int cnt = 0;

	if (!ctl) {
		fprintf(stderr, "do_print_results: NULL ctl struct\n");
		return;
	}


	lst_chmod_ptr = ctl->log_chmod.next;
	for (lst_snap_ptr = ctl->log_snapshot.next; !list_empty(lst_snap_ptr); ) {

		log_snap = list_entry(lst_snap_ptr, struct tests_log_snapshot, lst);

		for (; !list_empty(lst_chmod_ptr); ) {

			cnt ++;
			printf("%d / %llu\r", cnt,
					(long long unsigned int) ctl->chmods_performed);

			log_chmod = list_entry(lst_chmod_ptr, struct tests_log_chmod, lst);
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

			break;

lbl_next:
			lst_chmod_ptr = lst_chmod_ptr->next;
			list_del(&log_chmod->lst);
//			free(log_chmod);
			continue;
		}
		lst_snap_ptr = lst_snap_ptr->next;
		list_del(&log_snap->lst);
	}

	printf("\n");
	printf(	"unaffected chmods avg (us): %f\n"
			"affected by create (us):    %f\n"
			"affected by wait (us):      %f\n"
			"affected by delete (us):    %f\n",
			(double) (unaffected_sum/unaffected_total),
			(double) (affected_create_sum/affected_create_total),
			(double) (affected_wait_sum/affected_wait_total),
			(double) (affected_delete_sum/affected_delete_total));
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

	err = tests_ctl_init(&ctl, p_argv[0], p_argv[1]);
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

	err = do_tests(&ctl);
	if (err < 0) {
		goto err_cleanup;
	}

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
