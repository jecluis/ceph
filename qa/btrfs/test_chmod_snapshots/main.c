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

#include "ctl.h"
#include "err.h"
#include "tests.h"

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
	argv += optind;

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


int main(int argc, char ** argv)
{
	struct tests_ctl ctl;
	int err;

	char ** p_argv; // xxx: figure out how to do this without being ugly.
	int p_argc;

	p_argc = argc;
	p_argv = argv;

	err = do_getopt(&p_argc, p_argv, &ctl.options);
	if (err < 0) {
		fprintf(stderr, "unable to obtain options: %s\n", strerror(err));
		return 1;
	}

	if (p_argc < 2) {
		print_usage(argv[0]);
		goto err_cleanup;
	}

	err = tests_ctl_init(&ctl, argv[1], argv[2]);
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

	err = do_tests(&ctl);
	if (err < 0) {

	}

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
