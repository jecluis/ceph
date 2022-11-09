# Changelog
All notable changes to this project will be documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
- rgw/sfs: new on-disk format, based on filesystem hash tree for data
  and sqlite for metadata.
- rgw/sfs: maintain one single sqlite database connection.
- rgw/sfs: protect sqlite access with 'std::shared_lock'; allows multiple
  parallel reads, but only one write at a time.


### Changed
- rgw/sfs: no longer create directory hierarchy when initing the store; instead,
  ensure the sfs path exists by creating its directory if missing.


### Removed
- rgw/sfs: remove unused data and metadata functions, artifacts from our
  previous file-based implementation.


## [0.2.0] - 2022-07-28

- Nothing changed on this version.


## [0.1.0] - 2022-07-14

### Added
- sfs: support for object GET/PUT/LIST/DELETE
- sfs: support for bucket listing, create
- common: obtain env variable contents
- ci: build and test radosgw with sfs
- sfs: introduce sqlite orm library
- sfs: keep users in sqlite
- sfs: basic user management via REST api
