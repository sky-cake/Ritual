## Ritual Scanner


### About

The current Asagi filepath convention is,

- `.../<board>/image/tim[:4]/tim[4:6]/tim<ext>` for full media.
- `.../<board>/thumb/tim[:4]/tim[4:6]/tim<s.jpg>` for thumbnails.

`tim` comes from the 4chan API.

If `tim = 1234567890123456` then the filepath could be,

- `.../ck/image/1234/56/1234567890123456.webm` for the full media.
- `.../ck/thumb/1234/56/1234567890123456s.jpg` for the thumbnail.

This results in storing duplicate files across difference board and tim values if your API downloader doesn't do anything fancy here.

Ritual Scanner is an extension to Asagi conventions. It does not modify the Asagi database schema. It allows you to store media in the form,

- `.../img/sha256[:2]/sha256[2:4]/sha256[4:6]/sha256.ext/sha256<ext>` for full media.
- `.../thb/sha256[:2]/sha256[2:4]/sha256[4:6]/sha256.ext/sha256<.jpg>` for thumbnails.

`sha256` is the computed sha256 hash of the full media file with each character being `[0-9a-f]`.


### User Guide

If you have an existing archive, Ritual Scanner is meant to be run after a few other migrations steps. See `migrations/README.md`. You'll likely hit `hashtab.sha256` unique constrain errors if you try using this without migrating to the Ritual Scanner filepath convention first.

If you don't have an existing archive, you have less to worry about.

1. `cp scanner_template.toml scanner.toml`
1. Set values in `scanner.toml`
1. `python scanner.py -c scanner.toml`


### Notes

- Arbitrary filepath constructs will be supported by [Ayase Quart](https://github.com/sky-cake/ayase-quart) in the future to make use of this.
- Cloudflare md5-b64 hashes differ from the 4chan API's given md5-b64 hashes differ quite a lot on some boards. That's why there are two md5 b64 columns, `md5_b64_given` and `md5_b64_computed`.
- This handles file extensions that are mixed-case.
- Ritual Scanner's filepath convention can scale to more disks by dividing across `.../[img|thb]/sha256[:2]` paths.
    - Hint: `location ~ ^/img/([0-8][0-9a-f])/ {root /mnt/disk_a;}`
