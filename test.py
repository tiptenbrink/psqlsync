import logging
from psqlsync.ghmoteqlync.cli import run as remote_run


def run_here():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    remote_run("DSAV-Dodeka", "backend", "test_dumps", "dodeka", verbose=False)


run_here()