## Sutra


### About

AsagiMediaFP uses the following.

- `.../<board>/image/tim[:4]/tim[4:6]/tim<ext>` for full media.
- `.../<board>/thumb/tim[:4]/tim[4:6]/tim<s.jpg>` for thumbnails.

`tim` comes from the 4chan API.

If `tim = 1234567890123456` then the filepath could be,

- `.../ck/image/1234/56/1234567890123456.webm` for the full media.
- `.../ck/thumb/1234/56/1234567890123456s.jpg` for the thumbnail.

This results in storing duplicate files across difference board and tim values if your API downloader doesn't do anything fancy here.

SutraMediaFPs uses the following. It does not modify the Asagi database schema. It allows you to store media in the form,

- `.../img/md5[:2]/md5[2:4]/md5[4:6]/md5.ext/md5<ext>` for full media.
- `.../thb/md5[:2]/md5[2:4]/md5[4:6]/md5.ext/md5<.jpg>` for thumbnails.

Note: This is the `md5_computed` value from the scanner database, not the API-reported `md5`. Base64 characters `+` and `/` are replaced with `-` and `_` for filesystem safety.


### User Guide

If you have an existing archive, Sutra is meant to be run after a few other migrations steps. See `migrations/README.md`. You'll likely hit `hashtab.md5_computed` unique constrain errors if you try using this without migrating to the SutraMediaFP first.

If you don't have an existing archive, you have less to worry about.

1. `cp scanner_template.toml scanner.toml`
1. Set values in `scanner.toml`
1. `python scanner.py -c scanner.toml`


### Notes

- Arbitrary filepath constructs will be supported by [Ayase Quart](https://github.com/sky-cake/ayase-quart) in the future to make use of this.
- Cloudflare-computed md5-b64 hashes often differ from the 4chan API's reported md5-b64 hashes on some boards. That's why there are two md5 b64 columns: `md5` (API-reported) and `md5_computed` (locally computed against the downloaded file).
- This handles file extensions that are mixed-case.
- SutrasMediaFP can scale to more disks by dividing across `.../[img|thb]/md5[:2]` paths.
    - Hint: `location ~ ^/img/([A-Za-z0-9_-])/ {root /mnt/disk_a;}`
