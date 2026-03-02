This is migration guide is still a work in progress.

### Migrating from the Asagi filepath convention to the Ritual Scanner filepath convention

Note: You are responsible for any data loss.

1. Stop your Ritual archive to avoid downloading more media
1. Run [fclones](https://github.com/pkolaczk/fclones) to remove all duplicated files, include hard links from any previous runs that used `fclones link --priority oldest < dupes.txt`.
    - `fclones group /path/to/root/media --cache --match-links > dupes_ml.txt` treats all hard linked files as duplicates.
    - `fclones remove --priority oldest < dupes_ml.txt` removes the oldest replicas.
1. Run the scanner against your media's root path, see `scanner/README.md`.
1. Run the migration script, see `migrations/asagi_to_ritual_scanner.py`
    - This is a self-contained file to avoid any config and/or code conflicts.
1. Change Ritual's `filepath_convention` config from the default `asagi` to `ritual_scanner`
1. Restart your Ritual archive
