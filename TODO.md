# TODO

## Short-term
- bz2 handling? with perf. warning?

## Long-term
- filter by title, user, comment, minor, timestamp (between, before, after, --as-of)
- search added/removed text?
- parse siteinfo and allow passing namespaces by name?
- print match statistics (how many matches in how many articles, percentage of pages matching, ...)
- check passed namespaces and allow textual namespace specification
- README.md
- output formats: normal, csv, json, wikitext
- for text output: one-line-per-match
- ci, more tests
- optimize: use less processes with 7z?
- optimize: disable 7z multi-threading?
- make use of index and parallelize single-file bzip2 extraction by using multi-streams
- make use of index when bzip2 searching with --intitle

## A man can dream...
- Aarch64 Neon memchr implementation
- non-copying XML parser

## Abandonded ideas
- show performance statistics on break too
- progress display

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