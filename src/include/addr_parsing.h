/*
 * addr_parsing.h
 *
 *  Created on: Sep 14, 2010
 *      Author: gregf
 *      contains functions used by Ceph to convert named addresses
 *      (eg ceph.com) into IP addresses (ie 127.0.0.1).
 */

#ifndef ADDR_PARSING_H_
#define ADDR_PARSING_H_

#ifdef __cplusplus
extern "C" {
#endif

int safe_cat(char **pstr, int *plen, int pos, const char *str2);

/**
 * resolves one name into a list of addresses
 *
 * Returns a string allocated by malloc; caller must free.
 *
 * @param orig_str name to resolve.
 * @param[out] str_p if defined, copy of @p orig_str that can be modified.
 * @returns a comma-delimited list of addresses to be freed by the caller.
 */
char *resolve_one_name(const char *orig_str, char *str_p);

/*
 * returns a string allocated by malloc; caller must free
 */
char *resolve_addrs(const char *orig_str);

#ifdef __cplusplus
}
#endif

#endif /* ADDR_PARSING_H_ */
