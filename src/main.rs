use std::ffi::OsString;
use std::os::unix::{fs::MetadataExt, ffi::OsStringExt};
use std::io::Write;

use anyhow::{Result, Context, anyhow};
use clap::{Parser};
use num_cpus;
use chrono::{Duration, offset::{Local, TimeZone}, LocalResult};
use jwalk::{WalkDirGeneric, Parallelism};
use ignore::gitignore::GitignoreBuilder;

#[derive(Parser)]
#[command(name="cleanup",version="0.0.1")]
struct Args {
    #[arg(help="Path to scan")]
    path: OsString,

    #[arg(long="dest", help="Destination path for the move")]
    dest: OsString,

    #[arg(long="num-threads", help="Number of threads to use")]
    num_threads: Option<usize>,

    #[arg(long="dry-run", help="Only print the files that would be moved, but don't move anything")]
    dry_run: bool,

    #[arg(long="exclude-file", help="File containing paths to exclude")]
    exclude_file: Option<OsString>,

    #[arg(long="older", help="Number of days old the files must be to be selected", default_value="0")]
    older: i64,

    #[arg(long="noatime", help="Don't look at atime to determine age")]
    noatime: bool,
    #[arg(long="nomtime", help="Don't look at mtime to determine age")]
    nomtime: bool,
    #[arg(long="noctime", help="Don't look at ctime to determine age")]
    noctime: bool,
}

fn format_time(ts: i64) -> Result<String> {
    match Local.timestamp_opt(ts, 0) {
        LocalResult::None => Ok("Invalid date    ".into()),
        LocalResult::Single(d) => Ok(d.format("%Y-%m-%d %H:%M").to_string()),
        LocalResult::Ambiguous(_, _) => return Err(anyhow!("This should not happen (1)")),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    let dest = std::fs::canonicalize(args.dest)
        .context("Canonicalizng destination path")?;
    let path = std::fs::canonicalize(args.path)
        .context("Canonicalizing source path")?;

    let cutoff = (Local::now() - Duration::days(args.older)).timestamp();

    let filter = match args.exclude_file {
        Some(file) => {
            let fp = std::fs::canonicalize(file)
                .context("Canonicalizing exclude file")?;
            let mut builder = GitignoreBuilder::new(&path);
            builder.add(fp).map_or_else(|| Ok(()), |v| Err(v)).context("parsing exclude file")?;
            Some(builder.build()?)
        }
        None => None,
    };

    let num_t = args.num_threads.unwrap_or(num_cpus::get());
    
    let walk_dir = WalkDirGeneric::<((), Option<Result<std::fs::Metadata>>)>::new(path.clone())
        .skip_hidden(false)
        .follow_links(false)
        .parallelism(Parallelism::RayonNewPool(num_t))
        .process_read_dir(move |_, _, _, children| {
            // Remove skipped paths
            if let Some(filt) = &filter {
                children.retain(|dir_entry_result| {
                    dir_entry_result.as_ref().map(|dir_entry| {
                        match filt.matched(dir_entry.path(), dir_entry.file_type.is_dir()) {
                            ignore::Match::None => true,
                            ignore::Match::Ignore(_) => false,
                            ignore::Match::Whitelist(_) => true,
                        }
                    }).unwrap_or(false)
                });
            }
            // attach metadata to every file
            children.iter_mut().for_each(|dir_entry_result| {
                if let Ok(dir_entry) = dir_entry_result {
                    if dir_entry.file_type.is_file() {
                        dir_entry.client_state = Some(dir_entry.metadata().map_err(|e| e.into()));
                    }
                }
            });
            children.retain(|dir_entry_result| {
                dir_entry_result.as_ref().map(|dir_entry| {
                    dir_entry.client_state.as_ref().map_or(true, |rmeta| {
                        rmeta.as_ref().map_or(true, |meta| {
                            let mut res = false;
                            if !args.noatime {
                                res = res || meta.atime() < cutoff
                            }
                            if !args.nomtime {
                                res = res || meta.mtime() < cutoff
                            }
                            if !args.noctime {
                                res = res || meta.ctime() < cutoff
                            }
                            res
                        })
                    })
                }).unwrap_or(false)
            });
        });

    let mut stdout = std::io::stdout();
    stdout.write_all("atime             ctime             mtime             UID     Path\n".as_bytes())?;
    for entry in walk_dir {
        if let Ok(ent) = entry {
            if ent.file_type.is_file() {
                let fpath = ent.path();
                let fdest = dest.join(fpath.strip_prefix(&path)?);
                let fvec = fpath.clone().into_os_string().into_vec();
                let meta = ent.client_state.unwrap().expect("No stat data");
                let atime = format_time(meta.atime())?;
                let ctime = format_time(meta.ctime())?;
                let mtime = format_time(meta.mtime())?;
                let owner = meta.uid();
                let info = format!("{}, {}, {}, {:6}, ",
                                   atime, ctime, mtime, owner);
                stdout.write_all(info.as_bytes())?;
                stdout.write_all(&fvec)?;
                stdout.write_all(b"\n")?;
                if !args.dry_run {
                    std::fs::rename(fpath, fdest)?;
                    // send emails also
                }
            }
        }
    }
    Ok(())
}
