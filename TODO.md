# TODO

## High prio
- facilitate cargo install

### wdet:
- -d/--decompress
- mirror-list
- better downloading stats (percentage complete) with ETA

### wdgrep
- ci, more tests, coverage
- set up benchmarking
- README.md
- filter by title: --intitle

## Long-term
- refactor: main calls fn returning Result<()> for full control over dropping and exit code
- benchmark mimalloc instead of snmalloc (much more widely used)
- search progress display
- parse siteinfo and allow passing namespaces by name?
- print match statistics (how many matches in how many articles, percentage of pages matching, ...)
- output formats: normal, csv, json, wikitext
- only print matches
- for text output: one-line-per-match as an option
- benchmark: use less processes with 7z?
- benchmark (Windows): disable 7z multi-threading?
- print captured groups, maybe also s/../../
- kib/mib/gib, hh:mm:ss
- clap_generate
- use (color-)eyre instead of anyhow for backtraces
- support multiple regex engines/regex engine switching

### Full dump only improvements
- only useful for full dump: filter by user, comment, minor, timestamp (between, before, after, --as-of)
- search added/removed text?


## A man can dream...
- Aarch64 Neon memchr implementation
- non-copying XML parser
- SIMD UTF-8

## Abandonded ideas
- show performance statistics on break too
- make use of index and parallelize single-file bzip2 extraction by using multi-streams (abandoned: bzip2 too slow in any case, no need to waste time on it)
- make use of index when bzip2 searching with --intitle (abandoned: bzip2 too slow in any case, no need to waste time on it)

## wdget
- -d/--decompress option
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
- dump update into wdget or wdupdate
- wdcat, wdls
- better error handling