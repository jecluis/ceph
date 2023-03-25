// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#ifndef RGW_DRIVER_SFS_SQLITE_SQLITE_MULTIPART_H
#define RGW_DRIVER_SFS_SQLITE_SQLITE_MULTIPART_H

#include "buckets/multipart_conversions.h"
#include "dbconn.h"

namespace rgw::sal::sfs::sqlite {

class SQLiteMultipart {
  DBConnRef conn;

 public:
  explicit SQLiteMultipart(DBConnRef _conn);
  virtual ~SQLiteMultipart() = default;

  /**
   * @brief Obtain a vector of all multipart uploads on a given bucket,
   * according to the specified parameters.
   *
   * @param bucket_name The name of the bucket for which to list multipart
   * uploads.
   * @param prefix Prefix to match when listing multipart uploads.
   * @param marker Pagination marker, it's the first multipart upload to obtain.
   * @param delim ??
   * @param max_uploads Maximum number of multipart uploads to obtain, per page.
   * @param is_truncated Whether the results have been truncated due to
   * pagination.
   * @return std::optional<std::vector<DBOPMultipart>>
   */
  std::optional<std::vector<DBOPMultipart>> list_multiparts(
      const std::string& bucket_name, const std::string& prefix,
      const std::string& marker, const std::string& delim,
      const int& max_uploads, bool* is_truncated
  ) const;

  /**
   * @brief Abort on-going multipart uploads on a given bucket.
   *
   * @param bucket_name The name of the bucket for which multipart uploads will
   * be aborted.
   * @return the number of aborted multipart uploads, or negative in case of
   * error.
   */
  int abort_multiparts(const std::string& bucket_name) const;

  /**
   * @brief Get the converted Multipart entry from the database.
   *
   * @param upload_id The Multipart Upload's ID to obtain.
   * @return Multipart Upload entry, or `nullopt` if not found.
   */
  std::optional<DBOPMultipart> get_multipart(const std::string& upload_id
  ) const;

  /**
   * @brief Insert a new Multipart Upload entry into the database.
   *
   * @param mp The Multipart Upload to insert.
   * @return The new entry's ID.
   */
  uint insert(const DBOPMultipart& mp) const;

  /**
   * @brief Obtains a vector of DB multipart part entries.
   *
   * @param upload_id The upload id for which to obtain parts.
   * @param num_parts Maximum number of parts to obtain.
   * @param marker The next part's marker to obtain.
   * @param next_marker The marker of the next part not returned, if any.
   * @param truncated  Whether there are more entries after the ones returner.
   * @return std::vector<DBMultipartPart>
   */
  std::vector<DBMultipartPart> list_parts(
      const std::string& upload_id, int num_parts, int marker, int* next_marker,
      bool* truncated
  ) const;

  /**
   * @brief Obtains a vector of a multipart upload's parts, ordered by part num.
   *
   * @param upload_id The upload id for which to obtain parts.
   * @return std::vector<DBOPMultipartPart>
   */
  std::vector<DBMultipartPart> get_parts(const std::string& upload_id) const;

  /**
   * @brief Obtain a single part for a given multipart upload.
   *
   * @param upload_id The multipart upload ID.
   * @param part_num The part's number.
   * @return std::optional<DBMultipartPart>
   */
  std::optional<DBMultipartPart> get_part(
      const std::string& upload_id, uint32_t part_num
  ) const;

  /**
   * @brief Either creates a new part if it doesn't exist, or resets an existing
   * part as if it was a new part.
   *
   * @param upload_id The upload ID.
   * @param part_num The part's number.
   * @return std::optional<DBMultipartPart>
   */
  std::optional<DBMultipartPart> create_or_reset_part(
      const std::string& upload_id, uint32_t part_num, std::string* error_str
  ) const;

  /**
   * @brief Finish a given individual part's upload.
   *
   * @param upload_id The multipart upload ID.
   * @param part_num The part's number.
   * @param etag The part's etag.
   * @param bytes_written Number of bytes written during this part's upload.
   * @return true The database was properly updated with this information.
   * @return false The database was not updated.
   */
  bool finish_part(
      const std::string& upload_id, uint32_t part_num, const std::string& etag,
      uint64_t bytes_written
  ) const;

  /**
   * @brief Abort an on-going Multipart Upload.
   *
   * @param upload_id The Multipart Upload's ID to abort.
   * @return Whether the Multipart Upload was aborted.
   */
  bool abort(const std::string& upload_id) const;

  /**
   * @brief Mark an on-going Multipart Upload as being complete.
   *
   * @param upload_id The Multipart Upload's ID.
   * @return true if a multipart upload was marked complete.
   * @return false if no multipart upload was found.
   */
  bool mark_complete(const std::string& upload_id) const;

  /**
   * @brief Mark an on-going Multipart Upload as being aggregating its multiple
   * parts into a single file.
   *
   * @param upload_id The Multipart Upload's ID.
   * @return true if a multipart upload was marked as aggregating.
   * @return false if no multipart upload was found.
   */
  bool mark_aggregating(const std::string& upload_id) const;

  /**
   * @brief Mark an on-going Multipart Upload as being done, nothing more to do.
   *
   * @param upload_id The Multipart Upload's ID.
   * @return true if a multipart upload was marked as done.
   * @return false if no multipart upload was found.
   */
  bool mark_done(const std::string& upload_id) const;
};

}  // namespace rgw::sal::sfs::sqlite

#endif  // RGW_DRIVER_SFS_SQLITE_SQLITE_MULTIPART_H
