# TODO

## Short-term
- refactor: main calls fn returning Result<()> for full control over dropping and exit code

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
- 10MiB 7z test file in repo for tests and benchmarks
- only print matches
- print captured groups, maybe also s/../../

## A man can dream...
- Aarch64 Neon memchr implementation
- non-copying XML parser

## Abandonded ideas
- show performance statistics on break too
- progress display

## wdget
- mirror list/shortcut
- two-line progress text to avoid terminal overflow at normal widths
- progress: show ETA?
- list-dumps: also show #files, total size, description as table
- show list-wikis as table
- supress progress with --no-progress or --quiet instead of !-verbose, only show progress with tty
- print downloaded files in quiet mode?
- verify sha1 checksums while dl'ing (only with --verify option)
- colorize (+clap colorize)
- option to extract .gz/bz2/.xz while dl'ing?
- --overwrite (or --force ?)
- --resume-partial/--keep-partial
- automatically try again if intermittent network issue (or w/ --retry)?
- verify (given files) subcommand
- --target-dir/-t
- tests?

## further ideas:
- wdcat, wdls
- better error handling