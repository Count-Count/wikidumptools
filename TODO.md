# TODO

## Short-term
- parallelity configurable
- pass config object to search_dump()
- check passed namespaces and allow textual namespace specification
- search 7z files directly since extraction is not feasible (using 7z binary for now)

## Long-term
- dl tool
- multi-file
- handle bz2/xz compressed files
- search all revisions
- filter by title, user
- search added text?
- parse siteinfo and allow passing namespaces by name?
- print all matching articles
- print match statistics (how many matches in how many articles, percentage of pages matching, ...)
- search revisions as of a certain date in allversions dump

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