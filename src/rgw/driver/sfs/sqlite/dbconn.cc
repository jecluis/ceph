#include "dbconn.h"

#include <filesystem>
#include <system_error>

#include "common/dout.h"

namespace rgw::sal::sfs::sqlite {

void DBConn::check_metadata_is_compatible(CephContext* ctt) {
  int db_version = 0;
  try {
    db_version = storage.pragma.user_version();
    lsubdout(ctt, rgw, 10) << "db user version: " << db_version << dendl;
  } catch (const std::system_error& e) {
    lsubdout(ctt, rgw, -1) << "error opening db: " << e.code().message() << " ("
                           << e.code().value() << "), " << e.what() << dendl;
    throw e;
  }

  if (db_version == 0) {
    // must have just been created, set version!
    storage.pragma.user_version(SFS_METADATA_VERSION);
  } else if (db_version < SFS_METADATA_VERSION) {
    // perform schema update
    throw sqlite_sync_exception("Existing metadata format too old!");
  } else if (db_version > SFS_METADATA_VERSION) {
    // we won't be able to read a database in the future.
    throw sqlite_sync_exception(
        "Existing metadata too far ahead! Please upgrade!"
    );
  }
}

}  // namespace rgw::sal::sfs::sqlite
