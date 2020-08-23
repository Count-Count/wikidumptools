# TODO

## Short-term
- pass config object to search_dump()
- check passed namespaces and allow textual namespace specification
- 7z binary param
- bz2 handling? with perf. warning?
- multi-revision searching
- show performance statistics on break too (?)
- optimize: use less processes with 7z?
- optimize: disable 7z multi-threading?

## Long-term
- filter by title, user
- search added/removed text?
- parse siteinfo and allow passing namespaces by name?
- print all matching articles
- print match statistics (how many matches in how many articles, percentage of pages matching, ...)
- search revisions as of a certain date in allversions dump
- progress display

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
- delete part files by default
- shorter progress text to avoid terminal overlow at normal widths