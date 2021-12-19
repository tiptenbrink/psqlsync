import os
import shutil
import logging

from psqlsync.lib import *

__all__ = ['backup', 'restore']


logger = logging.getLogger(__name__)


def backup(db_url, postgres_db, local_file_path, storage_engine, filename_compressed, manager_config, verbose=False):
    logger.info('Backing up {} database to {}'.format(postgres_db, local_file_path))
    if verbose:
        logger.info(f"DB url: {db_url}")
    result = backup_postgres_db(db_url, local_file_path, verbose)
    if verbose:
        for line in result.splitlines():
            logger.info(line)

    logger.info("Backup complete")
    logger.info("Compressing {}".format(local_file_path))
    comp_file = compress_file(local_file_path)
    logger.info(storage_engine)
    if storage_engine == 'LOCAL':
        logger.info('Moving {} to local storage...'.format(comp_file))
        move_to_local_storage(comp_file, filename_compressed, manager_config)
        logger.info("Moved to {}{}".format(manager_config.get('LOCAL_BACKUP_PATH'), filename_compressed))


def restore(time, restore_filename, storage_engine, manager_config, postgres_restore, dest_db, postgres_db,
            postgres_host,
            postgres_port,
            postgres_user,
            postgres_password,
            restore_uncompressed, verbose=False):

    try:
        os.remove(restore_filename)
    except Exception as e:
        logger.info(e)
    all_backup_keys = list_available_backups(storage_engine, manager_config)
    backup_match = [s for s in all_backup_keys if time in s]
    if backup_match:
        logger.info("Found the following backup : {}".format(backup_match))
    else:
        logger.error("No match found for backups with date : {}".format(time))
        logger.info("Available keys : {}".format([s for s in all_backup_keys]))
        exit(1)

    if storage_engine == 'LOCAL':
        logger.info("Choosing {} from local storage".format(backup_match[0]))
        shutil.copy('{}/{}'.format(manager_config.get('LOCAL_BACKUP_PATH'), backup_match[0]),
                    restore_filename)
        logger.info("Fetch complete")

    logger.info("Extracting {}".format(restore_filename))
    ext_file = extract_file(restore_filename)
    # cleaned_ext_file = remove_faulty_statement_from_dump(ext_file)
    logger.info("Extracted to : {}".format(ext_file))
    logger.info("Creating temp database for restore : {}".format(postgres_restore))
    tmp_database = create_db(postgres_host,
                             postgres_restore,
                             postgres_port,
                             postgres_user,
                             postgres_password)
    logger.info("Created temp database for restore : {}".format(tmp_database))
    logger.info("Restore starting")
    tmp_db_url = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_restore}'
    if verbose:
        logger.info(f"DB url: {tmp_db_url}")
    result = restore_postgres_db(tmp_db_url, restore_uncompressed, verbose)
    if verbose:
        for line in result.splitlines():
            logger.info(line)
    logger.info("Restore complete")
    if dest_db is not None:
        restored_db_name = dest_db
        logger.info("Switching restored database with new one : {} > {}".format(
            postgres_restore, restored_db_name
        ))
    else:
        restored_db_name = postgres_db
        logger.info("Switching restored database with active one : {} > {}".format(
            postgres_restore, restored_db_name
        ))

    swap_after_restore(postgres_host,
                       postgres_restore,
                       restored_db_name,
                       postgres_port,
                       postgres_user,
                       postgres_password)
    logger.info("Database restored and active.")
