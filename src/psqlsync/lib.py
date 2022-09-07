import subprocess
import os
import shutil
from pathlib import Path

import gzip
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

__all__ = ['list_available_backups', 'list_postgres_databases', 'backup_postgres_db', 'compress_file', 'extract_file',
           'restore_postgres_db', 'move_to_local_storage', 'create_db', 'swap_after_restore']


def list_available_backups(storage_engine, manager_config):
    key_list = []
    if storage_engine == 'LOCAL':
        backup_folder = manager_config.get('LOCAL_BACKUP_PATH')
        try:

            backup_list = os.listdir(backup_folder)
        except FileNotFoundError:
            raise FileNotFoundError(f'Could not find {backup_folder} when searching for backups.'
                                    f'Check your config file settings')
    else:
        raise ValueError("Only 'LOCAL' storage engine is supported!")

    for bckp in backup_list:
        key_list.append(bckp)
    return key_list


def list_postgres_databases(db_url):
    process = subprocess.Popen(
        ['psql',
         f'--dbname={db_url}',
         '--list'],
        stdout=subprocess.PIPE
    )
    output = process.communicate()[0]
    if int(process.returncode) != 0:
        raise ChildProcessError("psql --list failed. Return code : {}".format(process.returncode))

    return output


def backup_postgres_db(db_url, dest_file, verbose):
    """
    Backup postgres db to a file.
    """
    if verbose:
        process = subprocess.Popen(
            ['pg_dump',
             f'--dbname={db_url}',
             '-Fc',
             '-f', dest_file,
             '-v'],
            stdout=subprocess.PIPE
        )
        output = process.communicate()[0]
        if int(process.returncode) != 0:
            raise ChildProcessError("psql --backup failed. Return code : {}".format(process.returncode))
        return output
    else:
        process = subprocess.Popen(
            ['pg_dump',
             f'--dbname={db_url}',
             '-Fc',
             '-f', dest_file],
            stdout=subprocess.PIPE
        )
        output = process.communicate()[0]
        if process.returncode != 0:
            raise ChildProcessError("psql --backup failed. Return code : {}".format(process.returncode))
        return output


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


def restore_postgres_db(db_url, backup_file, verbose):
    """Restore postgres db from a file."""
    subprocess_params = [
        'pg_restore',
        '--no-owner',
        f'--dbname={db_url}'
    ]

    if verbose:
        subprocess_params.append('-v')

    subprocess_params.append(backup_file)
    process = subprocess.Popen(subprocess_params, stdout=subprocess.PIPE)
    output = process.communicate()[0]

    if int(process.returncode) != 0:
        raise ChildProcessError("psql --restore failed. Return code : {}".format(process.returncode))

    return output


def create_db(db_host, database, db_port, user_name, user_password):
    con = psycopg2.connect(dbname='postgres', port=db_port,
                           user=user_name, host=db_host,
                           password=user_password)

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute("SELECT pg_terminate_backend( pid ) "
                "FROM pg_stat_activity "
                "WHERE pid <> pg_backend_pid( ) "
                "AND datname = '{}'".format(database))
    cur.execute("DROP DATABASE IF EXISTS {} ;".format(database))
    cur.execute("CREATE DATABASE {} ;".format(database))
    cur.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(database, user_name))
    return database


def swap_after_restore(db_host, restore_database, new_active_database, db_port, user_name, user_password):
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


def move_to_local_storage(comp_file, filename_compressed, manager_config):
    """ Move compressed backup into {LOCAL_BACKUP_PATH}. """
    backup_folder = Path(manager_config.get('LOCAL_BACKUP_PATH'))
    if not backup_folder.exists():
        backup_folder.mkdir()
    shutil.move(comp_file, '{}{}'.format(manager_config.get('LOCAL_BACKUP_PATH'), filename_compressed))
