import argparse
import os
import tomllib

from utils import make_path


def str_to_bool(value: str) -> bool | None:
    if value == '':
        return None
    if value.lower() in ('yes', 'y', 'true', '1'):
        return True
    if value.lower() in ('no', 'n', 'false', '0'):
        return False
    raise argparse.ArgumentTypeError(f'Expected a boolean value, got: {value!r}')


class Config:
    db_path: str
    root_path: str
    skip_dirnames: set[str]
    file_exts: set[str]
    prompt: bool

    _required_fields = ['db_path', 'root_path', 'file_exts', 'prompt']

    file_read_buffer_size: int = 5 * 1024 * 1024
    gather_filesystem_batch_size: int = 25_000
    gather_metadata_batch_size: int = 500

    def __init__(self):
        self.args = self.parse_args()
        self.toml_path = self.find_toml_path()
        self.toml_data: dict = self.load_toml()
        self.merge_toml_and_cli_configs()

    def print_and_verify(self):
        print()
        print('Here are your configs:')
        print()
        print(f'  db_path: {self.db_path}')
        print(f'  root_path: {self.root_path}')
        print(f'  skip_dirnames: {self.skip_dirnames}')
        print(f'  file_exts: {self.file_exts}')
        print()

        if not self.prompt:
            return

        assert input('Proceed? (y/n): ') == 'y'

    def parse_args(self):
        parser = argparse.ArgumentParser(description='Scanner - Filesystem media catalog and metadata gatherer')
        parser.add_argument('-c',              type=str,         default='', help='Path to TOML configuration file')
        parser.add_argument('--db-path',       type=str,         default='', help='Path to SQLite database')
        parser.add_argument('--root-path',     type=str,         default='', help='Root directory to scan recursively')
        parser.add_argument('--skip-dirnames', type=str,         default='', help='Comma-separated directory names to skip during scanning. E.g. thumb,backup')
        parser.add_argument('--file-exts',     type=str,         default='', help='Comma-separated file extensions to scan. E.g. jpeg,jpg,png,gif,webm,mp4')
        parser.add_argument('--prompt',        type=str_to_bool, default='', help='Prompt user to verify configs? (y/n)')
        return parser.parse_args()

    def find_toml_path(self):
        if self.args.c:
            if os.path.exists(self.args.c):
                print(f'Config file found: {self.args.c}')
                return self.args.c

            raise ValueError(f'Config file not found: {self.args.c}')
        
        config_path = make_path('scanner.toml')
        if os.path.exists(config_path):
            print(f'Config file found: {config_path}')
            return config_path

        raise ValueError(f'Config file not found: {config_path}')

    def load_toml(self):
        with open(self.toml_path, 'rb') as f:
            return tomllib.load(f)

    def merge_toml_and_cli_configs(self):
        """
        Config priorities: cli > toml
        """
        not_found = []
        for field in self._required_fields:
            cli_val = getattr(self.args, field, None)
            if isinstance(cli_val, str) and not cli_val:
                cli_val = None

            toml_val = self.toml_data.get(field, None)
            if isinstance(toml_val, str) and not toml_val:
                toml_val = None

            if cli_val is not None:
                setattr(self, field, cli_val)
            elif toml_val is not None:
                setattr(self, field, toml_val)
            else:
                not_found.append(field)

        if not_found:
            raise ValueError(f'We need these args: {not_found}')

        self.db_path = os.path.abspath(self.db_path)
        if not os.path.isfile(self.db_path):
            print(f'Warning: {self.db_path} is not an existing sqlite file. It will be created.')

        self.root_path = os.path.abspath(self.root_path)
        if not os.path.isdir(self.root_path):
            raise ValueError(f'Not a directory: {self.root_path}')

        self.skip_dirnames = set(self.skip_dirnames.split(',')) if hasattr(self, 'skip_dirnames') else None
        self.file_exts = set([e.lower() for e in self.file_exts.split(',')])
