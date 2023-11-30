use std::ffi::OsString;
use std::os::unix::fs::MetadataExt;
use anyhow::{Error, Result, Context};

use clap::{Parser};
use chrono::{Duration, offset::Local};
use jwalk::{WalkDirGeneric, Parallelism};
use ignore::gitignore::GitignoreBuilder;

#[derive(Parser)]
#[command(name="cleanup",version="0.0.1")]
struct Args {
    #[arg(help="Path to scan")]
    path: OsString,

    #[arg(long="dest", help="Destination path for the move")]
    dest: OsString,

    #[arg(long="num_threads", help="Number of threads to use", default_value="4")]
    num_threads: usize,

    #[arg(long="exclude_file", help="File containing paths to exclude")]
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

fn main() -> Result<()> {
    let args = Args::parse();

    let dest = std::fs::canonicalize(args.dest)
        .context("Canonicalizng destination path")?;
    let path = std::fs::canonicalize(args.path)
        .context("Canonicalizing source path")?;
    if dest.starts_with(&path) {
        return Err(Error::msg("destination is inside of source"));
    }
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
    
    let walk_dir = WalkDirGeneric::<((), Option<Result<std::fs::Metadata>>)>::new(path)
        .skip_hidden(false)
        .follow_links(false)
        .parallelism(Parallelism::RayonNewPool(args.num_threads))
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

    for entry in walk_dir {
        if let Ok(ent) = entry {
            if ent.file_type.is_file() {
                println!("{}", ent.path().display());
            }
        }
    }
    Ok(())
}
