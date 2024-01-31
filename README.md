This is a utility to remove old files from a shared filesystem.

It will kove files into a backup folder structure at a designated
location so that users have some chance to restore essential
files. The target directory must be on the same filesystem because
rename is used to move the files instead of copies.

The destination folder will have one folder per uid with moved files
renamed as numbers starting from 0. A file named `map.json` will be
present in each directory containing a map from each renamed file to
its previous path, with bash shell escape for special characters
(either `\'` or something like `$'\022'` for invalid utf-8 raw bytes).

```
Usage: cleanup [OPTIONS] --dest <DEST> <PATH>

Arguments:
  <PATH>  Path to scan

Options:
      --dest <DEST>                  Destination path for the move
      --num-threads <NUM_THREADS>    Number of threads to use [default: number of cores]
      --dry-run                      Only print the files that would be moved, but don't move anything
      --exclude-file <EXCLUDE_FILE>  File containing paths to exclude
      --older <OLDER>                Number of days old the files must be to be selected [default: 0]
      --noatime                      Don't look at atime to determine age
      --nomtime                      Don't look at mtime to determine age
      --noctime                      Don't look at ctime to determine age
  -h, --help                         Print help
  -V, --version                      Print version
```

`--older`, by default will look at atime, mtime and ctime and if all
three are older than the specified cutoff, the file is considered
stale and will be moved.

If one of the `--no[acm]time` option is specified, this means that
that check will not be performed and only the other are considered for
age. If all of them are specified, all files are considered old and
will be moved.

The `--exclude-file` argument refers to a file in the gitignore format
and will exclude all files and directories matching the patterns for
processing. This mean that directories will not be recursed into so
negative patterns under an excluded directory will not work.
