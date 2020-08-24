# TODO

## Short-term
- 7z: binary param, opts param
- bz2 handling? with perf. warning?
- show performance statistics on break too (?)
- optimize: use less processes with 7z?
- optimize: disable 7z multi-threading?
- bug with -l -v the stats line is incorrectly colored

## Long-term
- filter by title, user, comment, minor, timestamp (between, supremum, infimum)
- search added/removed text?
- parse siteinfo and allow passing namespaces by name?
- print match statistics (how many matches in how many articles, percentage of pages matching, ...)
- progress display
- output formats
- check passed namespaces and allow textual namespace specification
- README.md
- multiple output formats: normal, csv, json, wikitext
- for text output: one-line-per-match
- ci, more tests

## A man can dream...
- Aarch64 Neon memchr implementation
- non-copying XML parser

## wdget
- verify sha1/md5 checksums while dl'ing
- colorize
- list available dumps dates
- mirror list/shortcut
- --target-dir
- option to unpack while dl?
- --no-progress
- delete part files by default and until resum is implemented
- shorter progress text to avoid terminal overlow at normal widths