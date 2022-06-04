from pathlib import Path
from functools import partial
import tempfile
import logging
import tomli
import trio
from dirgh import find_download

import psqlsync.actions


logger = logging.getLogger(__name__)


def prepare(target, owner, repo, repo_dir, overwrite, config, token, verbose):
    logger.info("Downloading backup files...")

    run = partial(find_download, owner, repo, repo_dir, target, overwrite=overwrite,
                  token=token)
    trio.run(run)
    latest_save = find_latest(target)

    temp_dir = Path(tempfile.gettempdir()).absolute()
    restore_filename = temp_dir.joinpath('restore.dump.gz')
    manager_config = {
        'BACKUP_PATH': temp_dir,
        'LOCAL_BACKUP_PATH': target
    }
    with open(config, "rb") as f:
        cfg = tomli.load(f)
    restore_uncompressed = temp_dir.joinpath('restore.dump')
    postgresql_cfg = cfg.get('postgresql')
    postgres_host = postgresql_cfg.get('host')
    postgres_port = postgresql_cfg.get('port')
    postgres_db = postgresql_cfg.get('db')
    postgres_restore = "{}_psqlsync_temp_restore".format(postgres_db)
    postgres_user = postgresql_cfg.get('user')
    postgres_password = postgresql_cfg.get('password')
    psqlsync.actions.restore(latest_save, restore_filename, 'LOCAL', manager_config, postgres_restore, None,
                             postgres_db,
                             postgres_host,
                             postgres_port,
                             postgres_user,
                             postgres_password,
                             restore_uncompressed, verbose)
    logger.info("Sync finished successfully.")


def find_latest(target: str):
    db_path = Path(target)
    globbed = db_path.glob("*")
    backup_files = []

    for glb in globbed:
        if glb.is_file():
            if "backup" in glb.name and "gz" in glb.suffix:
                dash_split = glb.name.split('-')
                backup_time = '-'.join([p for p in dash_split if p.isnumeric()])
                backup_files.append(backup_time)

    if not backup_files:
        raise ValueError("No backup files could be found, did download succeed?")

    # ascending order
    backup_files = sorted(backup_files)

    return backup_files[-1]
