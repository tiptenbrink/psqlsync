import argparse
import datetime
import logging
import tempfile
from pathlib import Path

import toml

from psqlsync.lib import *
import psqlsync.actions as act


def run():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    args_parser = argparse.ArgumentParser(description='psqlsync')
    args_parser.add_argument("--action",
                             metavar="action",
                             choices=['list', 'list_dbs', 'restore', 'backup'],
                             help="'list' (backups), 'list_dbs' (available dbs), 'restore' (requires --time), 'backup'",
                             required=True)
    args_parser.add_argument("--time",
                             metavar="YYYYMMdd-HHmmss",
                             help="Time to use for restore (show with --action list). If unique, will smart match.\n"
                                  "(If there's just one backup matching YYYMM, providing that is enough)")
    args_parser.add_argument("--dest-db",
                             metavar="dest_db",
                             default=None,
                             help="Name of the new restored database")
    verbose_nm = 'verbose'
    args_parser.add_argument(f"--{verbose_nm}",
                             default=False,
                             help="Verbose output")
    cfg_pth_nm = 'config'
    args_parser.add_argument(f"--{cfg_pth_nm}",
                             required=True,
                             help="Database configuration file path (.toml)")
    args = args_parser.parse_args()
    cli_args = vars(args)
    cfg = toml.load(cli_args.get(cfg_pth_nm))

    postgresql_cfg = cfg.get('postgresql')
    postgres_host = postgresql_cfg.get('host')
    postgres_port = postgresql_cfg.get('port')
    postgres_db = postgresql_cfg.get('db')
    postgres_restore = "{}_psqlsync_temp_restore".format(postgres_db)
    postgres_user = postgresql_cfg.get('user')
    postgres_password = postgresql_cfg.get('password')
    storage_engine = cfg.get('setup').get('storage_engine')
    timestr = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = 'backup-{}-{}.dump'.format(timestr, postgres_db)
    filename_compressed = '{}.gz'.format(filename)
    temp_dir = Path(tempfile.gettempdir()).absolute()
    restore_filename = temp_dir.joinpath('restore.dump.gz')
    restore_uncompressed = temp_dir.joinpath('restore.dump')
    local_storage_path = cfg.get('local_storage').get('path')
    verbose = cli_args.get(verbose_nm)

    manager_config = {
        'BACKUP_PATH': temp_dir,
        'LOCAL_BACKUP_PATH': local_storage_path
    }
    local_file_path = manager_config.get('BACKUP_PATH').joinpath(filename)

    # list task
    if args.action == "list":
        backup_objects = sorted(list_available_backups(storage_engine, manager_config), reverse=True)
        for key in backup_objects:
            logger.info("Key : {}".format(key))
    # list databases task
    elif args.action == "list_dbs":
        db_url = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'
        if verbose:
            logger.info(f"DB url: {db_url}")
        result = list_postgres_databases(db_url)
        for line in result.splitlines():
            logger.info(line)
    # backup task
    elif args.action == "backup":
        db_url = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'
        act.backup(db_url, postgres_db, local_file_path, storage_engine, filename_compressed, manager_config, logger,
                   verbose)
    # restore task
    elif args.action == "restore":
        if not args.time:
            logger.warning('No date was chosen for restore. Run again with the "list" '
                           'action to see available restore dates')
        act.restore(args.time, restore_filename, storage_engine, manager_config, postgres_restore, args.dest_db,
                    postgres_db,
                    postgres_host,
                    postgres_port,
                    postgres_user,
                    postgres_password,
                    restore_uncompressed, logger, verbose)
    else:
        logger.warning("No valid argument was given.")
        logger.warning(args)
