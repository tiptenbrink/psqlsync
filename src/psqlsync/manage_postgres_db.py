import argparse
import datetime
import logging
import subprocess
import os
import shutil
import tempfile
from pathlib import Path

import configparser
import gzip
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def list_available_backups(storage_engine, manager_config):
    key_list = []
    if storage_engine == 'LOCAL':
        backup_folder = manager_config.get('LOCAL_BACKUP_PATH')
        try:

            backup_list = os.listdir(backup_folder)
        except FileNotFoundError:
            raise FileNotFoundError(f'Could not found {backup_folder} when searching for backups.'
                                    f'Check your .config file settings')
    else:
        raise ValueError("Only 'LOCAL' storage engine is supported!")

    for bckp in backup_list:
        key_list.append(bckp)
    return key_list


def list_postgres_databases(host, database_name, port, user, password):
    process = subprocess.Popen(
        ['psql',
         '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
         '--list'],
        stdout=subprocess.PIPE
    )
    output = process.communicate()[0]
    if int(process.returncode) != 0:
        raise ChildProcessError("psql --list failed. Return code : {}".format(process.returncode))

    return output


def backup_postgres_db(host, database_name, port, user, password, dest_file, verbose):
    """
    Backup postgres db to a file.
    """
    if verbose:
        try:
            process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
                 '-Fc',
                 '-f', dest_file,
                 '-v'],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)
    else:

        try:
            process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
                 '-f', dest_file],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if process.returncode != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)


def compress_file(src_file):
    compressed_file = "{}.gz".format(str(src_file))
    with open(src_file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    return compressed_file


def extract_file(src_file):
    extracted_file, extension = os.path.splitext(src_file)

    with gzip.open(src_file, 'rb') as f_in:
        with open(extracted_file, 'wb') as f_out:
            for line in f_in:
                f_out.write(line)
    return extracted_file


def restore_postgres_db(db_host, db, port, user, password, backup_file, verbose):
    """Restore postgres db from a file."""
    try:
        subprocess_params = [
            'pg_restore',
            '--no-owner',
            '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user,
                                                          password,
                                                          db_host,
                                                          port,
                                                          db)
        ]

        if verbose:
            subprocess_params.append('-v')

        subprocess_params.append(backup_file)
        process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
        output = process.communicate()[0]

        if int(process.returncode) != 0:
            print('Command failed. Return code : {}'.format(process.returncode))

        return output
    except Exception as e:
        print("Issue with the db restore : {}".format(e))


def create_db(db_host, database, db_port, user_name, user_password):
    try:
        con = psycopg2.connect(dbname='postgres', port=db_port,
                               user=user_name, host=db_host,
                               password=user_password)
    except Exception as e:
        print(e)
        exit(1)

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    try:
        cur.execute("SELECT pg_terminate_backend( pid ) "
                    "FROM pg_stat_activity "
                    "WHERE pid <> pg_backend_pid( ) "
                    "AND datname = '{}'".format(database))
        cur.execute("DROP DATABASE IF EXISTS {} ;".format(database))
    except Exception as e:
        print(e)
        exit(1)
    cur.execute("CREATE DATABASE {} ;".format(database))
    cur.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(database, user_name))
    return database


def swap_after_restore(db_host, restore_database, new_active_database, db_port, user_name, user_password):
    try:
        con = psycopg2.connect(dbname='postgres', port=db_port,
                               user=user_name, host=db_host,
                               password=user_password)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute("SELECT pg_terminate_backend( pid ) "
                    "FROM pg_stat_activity "
                    "WHERE pid <> pg_backend_pid( ) "
                    "AND datname = '{}'".format(new_active_database))
        cur.execute("DROP DATABASE IF EXISTS {}".format(new_active_database))
        cur.execute('ALTER DATABASE "{}" RENAME TO "{}";'.format(restore_database, new_active_database))
    except Exception as e:
        print(e)
        exit(1)


def move_to_local_storage(comp_file, filename_compressed, manager_config):
    """ Move compressed backup into {LOCAL_BACKUP_PATH}. """
    backup_folder = manager_config.get('LOCAL_BACKUP_PATH')
    try:
        check_folder = os.listdir(backup_folder)
    except FileNotFoundError:
        os.mkdir(backup_folder)
    shutil.move(comp_file, '{}{}'.format(manager_config.get('LOCAL_BACKUP_PATH'), filename_compressed))


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
    args_parser.add_argument("--verbose",
                             default=False,
                             help="Verbose output")
    args_parser.add_argument("--configfile",
                             required=True,
                             help="Database configuration file")
    args = args_parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.configfile)

    postgres_host = config.get('postgresql', 'host')
    postgres_port = config.get('postgresql', 'port')
    postgres_db = config.get('postgresql', 'db')
    postgres_restore = "{}_restore".format(postgres_db)
    postgres_user = config.get('postgresql', 'user')
    postgres_password = config.get('postgresql', 'password')
    storage_engine = config.get('setup', 'storage_engine')
    timestr = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = 'backup-{}-{}.dump'.format(timestr, postgres_db)
    filename_compressed = '{}.gz'.format(filename)
    temp_dir = Path(tempfile.gettempdir()).absolute()
    restore_filename = temp_dir.joinpath('restore.dump.gz')
    restore_uncompressed = temp_dir.joinpath('restore.dump')
    local_storage_path = config.get('local_storage', 'path', fallback='./backups/')

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
        result = list_postgres_databases(postgres_host,
                                         postgres_db,
                                         postgres_port,
                                         postgres_user,
                                         postgres_password)
        for line in result.splitlines():
            logger.info(line)
    # backup task
    elif args.action == "backup":
        logger.info('Backing up {} database to {}'.format(postgres_db, local_file_path))
        result = backup_postgres_db(postgres_host,
                                    postgres_db,
                                    postgres_port,
                                    postgres_user,
                                    postgres_password,
                                    local_file_path, args.verbose)
        if args.verbose:
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
    # restore task
    elif args.action == "restore":
        if not args.time:
            logger.warning('No date was chosen for restore. Run again with the "list" '
                           'action to see available restore dates')
        else:
            try:
                os.remove(restore_filename)
            except Exception as e:
                logger.info(e)
            all_backup_keys = list_available_backups(storage_engine, manager_config)
            backup_match = [s for s in all_backup_keys if args.time in s]
            if backup_match:
                logger.info("Found the following backup : {}".format(backup_match))
            else:
                logger.error("No match found for backups with date : {}".format(args.time))
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
            result = restore_postgres_db(postgres_host,
                                         postgres_restore,
                                         postgres_port,
                                         postgres_user,
                                         postgres_password,
                                         restore_uncompressed,
                                         args.verbose)
            if args.verbose:
                for line in result.splitlines():
                    logger.info(line)
            logger.info("Restore complete")
            if args.dest_db is not None:
                restored_db_name = args.dest_db
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
    else:
        logger.warning("No valid argument was given.")
        logger.warning(args)
