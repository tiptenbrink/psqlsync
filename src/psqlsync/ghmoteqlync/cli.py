import os
import argparse
import getpass
import logging

from psqlsync.ghmoteqlync.sync import prepare


logger = logging.getLogger(__name__)


def run(owner, repo, repo_dir, app_name="psqlsync", config_def=None, target_def=None, verbose=True):
    restore_app_name = f'ghmoteqlync-{app_name}'
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description="Download and restore test database.")

    token_nm = 'token'
    token_help = f"user authentication token, OAuth or personal access token. Required to download from repo. Only " \
                 f"use this when running automated scripts. It does not remember the token."
    parser.add_argument('-t', f'--{token_nm}', help=token_help, default=None, required=False)

    input_token_nm = 'asktoken'
    input_token_help = f"make the program prompt for a password. Use this if running this script yourself. You only " \
                       f"have to do this once."
    parser.add_argument(f'--{input_token_nm}', help=input_token_help, default=False, required=False,
                        action='store_true')
    cfg_pth_nm = 'config'
    parser.add_argument(f"--{cfg_pth_nm}",
                        required=config_def is None, default=config_def,
                        help="Database configuration file path (.toml)")
    overwrite_nm = 'nooverwrite'
    overwrite_help = "Overwrite target directory."
    parser.add_argument('-N', f'--{overwrite_nm}', help=overwrite_help, default=True, required=False,
                        action='store_false')

    target_nm = 'target'
    target_help = f"output directory. If requesting a directory, this will overwrite the directory name. By default, " \
                  f"the content will be placed in backups/{repo}"
    target_default = target_def if target_def is not None else f"backups/{repo}"
    parser.add_argument('-o', f'--{target_nm}', help=target_help, default=target_default, required=False)

    config = vars(parser.parse_args())

    # GHMOTEQLYNC_<APP NAME>_GH_TOKEN
    pass_key = f"{restore_app_name}_gh_token".replace('-', '_')
    pass_key_env = pass_key.upper()
    if config[input_token_nm]:
        key_pass = getpass.getpass("Input your token:\n")
    else:
        if config[token_nm] is None:
            key_pass = os.environ.get(pass_key_env)
            if key_pass is None:
                raise ValueError(f"Please supply token or set the {pass_key_env} environment variable!")
        else:
            key_pass = config[token_nm]

    prepare(config[target_nm], owner, repo, repo_dir, config[overwrite_nm], config[cfg_pth_nm], key_pass, verbose)
