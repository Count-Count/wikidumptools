# TODO

## Short-term
- parallelity configurable
- pass config object to search_dump()
- check passed namespaces and allow textual namespace specification
- search 7z files directly since extraction is not feasible (using 7z binary for now)
- p7z on non-Windows, param
- bz2 handling?
- multi-revision searching
- show performance statistics on break too (?)
- optimize: use less processes with 7z?
- optimize: disable parallelism with 7z?

## Long-term
- handle bz2 compressed files
- filter by title, user
- search added text?
- parse siteinfo and allow passing namespaces by name?
- print all matching articles
- print match statistics (how many matches in how many articles, percentage of pages matching, ...)
- search revisions as of a certain date in allversions dump
- progress display

## A man can dream...
- Aarch64 Neon memchr implementation
- non-copying XML parser

## wdget
- verify checksum while dl'ing
- colorize
- list available dumps dates
- mirror list/shortcut
- --target-dir
- option to unpack while dl?
- --no-progress
- delete part files by default
- shorter progress text to avoid terminal overlow at normal widths