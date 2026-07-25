#!/usr/bin/env python3

import os
import sys


def migrate(root_dir: str) -> None:
    dirs_completed = 0
    files_completed = 0

    for first_entry in os.scandir(root_dir):
        if not first_entry.is_dir():
            continue

        first = first_entry.name
        if len(first) != 2:
            continue

        for second_entry in os.scandir(first_entry.path):
            if not second_entry.is_dir():
                continue

            second = second_entry.name
            if len(second) != 2:
                continue

            for third_entry in os.scandir(second_entry.path):
                if not third_entry.is_dir():
                    continue

                for file_entry in os.scandir(third_entry.path):
                    if not file_entry.is_file():
                        continue

                    destination_dir = os.path.join(root_dir, first[0], first[1])
                    destination = os.path.join(destination_dir, file_entry.name)

                    os.makedirs(destination_dir, exist_ok=True)
                    os.rename(file_entry.path, destination)

                    files_completed += 1
                    print(f'\rdirs: {dirs_completed} files: {files_completed}', end='')

                os.rmdir(third_entry.path)
                dirs_completed += 1
                print(f'\rdirs: {dirs_completed} files: {files_completed}', end='')

            os.rmdir(second_entry.path)
            dirs_completed += 1
            print(f'\rdirs: {dirs_completed} files: {files_completed}', end='', flush=True)

        os.rmdir(first_entry.path)
        dirs_completed += 1
        print(f'\rdirs: {dirs_completed} files: {files_completed}', end='', flush=True)

    print()


if len(sys.argv) != 2:
    print(f'usage: {sys.argv[0]} ROOT_DIR', file=sys.stderr)
    sys.exit(1)

migrate(sys.argv[1])
