from psqlsync.ghmoteqlync.cli import run as remote_run


def run_here():
    remote_run("DSAV-Dodeka", "backend", "test_dumps", "dodeka")


run_here()