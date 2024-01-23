use std::ffi::OsString;
use std::os::unix::{fs::MetadataExt, ffi::OsStringExt};
use std::io::Write;
use std::iter::successors;
use std::collections::hash_map::HashMap;
use std::path::PathBuf;

use anyhow::{Result, Context, anyhow};
use clap::{Parser};
use num_cpus;
use chrono::{Duration, offset::{Local, TimeZone}, LocalResult};
use jwalk::{WalkDirGeneric, Parallelism};
use ignore::gitignore::GitignoreBuilder;
use itertools::Itertools;

#[derive(Parser)]
#[command(name="cleanup",version="0.0.1")]
struct Args {
    #[arg(help="Path to scan")]
    path: OsString,

    #[arg(long="dest", help="Destination path for the move")]
    dest: OsString,

    #[arg(long="num-threads", help="Number of threads to use [default: number of cores]")]
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

fn shell_escape(c: char, buf: &mut String) {
    let mut s = vec![0, 0, 0, 0];
    if c.is_control() {
        c.encode_utf8(&mut s);
        for b in &s[..c.len_utf8()] {
            buf.push_str(&"'$'\\");
            buf.push_str(format!("{:03o}", b).as_str());
            buf.push_str("''");
        }
    } else if c == '\'' {
        buf.push_str(&"\\'");
    } else {
        buf.push(c);
    }
}

fn escape_path(path: &PathBuf) -> String {
    let vec = path.clone().into_os_string().into_vec();
    let mut res = String::with_capacity(512);
    let mut buf = Vec::with_capacity(8);
    res.push('\'');
    for b in vec {
        buf.push(b);
        if let Ok(s) = std::str::from_utf8(&buf) {
            for c in s.chars() {
                shell_escape(c, &mut res);
            }
            buf.clear();
        }
    }
    res.push('\'');
    res
}

fn main() -> Result<()> {
    let args = Args::parse();

    let dest = std::fs::canonicalize(args.dest)
        .context("Canonicalizng destination path")?;
    let path = std::fs::canonicalize(args.path)
        .context("Canonicalizing source path")?;

    let cutoff = (Local::now() - Duration::days(args.older)).timestamp();

    let filter = if dest.starts_with(&path) || args.exclude_file.is_some() {
        let mut builder = GitignoreBuilder::new(&path);
        if dest.starts_with(&path) {
            let line = String::from("/") +
                dest.strip_prefix(&path)?.to_str().ok_or(
                    anyhow!("dest path must be valid utf-8"))?.to_owned()
                + "/";
            builder.add_line(None, &line)?;
        }
        if let Some(file) = args.exclude_file {
            let fp = std::fs::canonicalize(file)
                .context("Canonicalizing exclude file")?;
            if fp.starts_with(&path) {
                let line = String::from("/") +
                    fp.strip_prefix(&path)?.to_str().ok_or(
                        anyhow!("exclude-file path must be valid utf-8"))?;
                builder.add_line(None, &line)?;
            }
            builder.add(fp).map_or_else(|| Ok(()), |v| Err(v)).context("parsing exclude file")?;
        }
        Some(builder.build()?)
    } else {
        None
    };

    let num_t = args.num_threads.unwrap_or(num_cpus::get());
    
    let walk_dir = WalkDirGeneric::<((), Option<std::fs::Metadata>)>::new(path.clone())
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
                        dir_entry.client_state = dir_entry.metadata().ok();
                    }
                }
            });
            children.retain(|dir_entry_result| {
                dir_entry_result.as_ref().map(|dir_entry| {
                    dir_entry.client_state.as_ref().map_or(true, |meta| {
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
                }).unwrap_or(false)
            });
        });

    let mut stdout = std::io::stdout();
    stdout.write_all("atime             ctime             mtime             UID     Path\n".as_bytes())?;
    for (owner, g) in &walk_dir.into_iter().filter_map(|item| {
        if let Ok(ent) = item {
            if ent.file_type.is_file() {
                return match ent.client_state {
                    Some(_) => Some(ent),
                    _ => None
                }
            }
        }
        return None;
    }).sorted_unstable_by_key(|ref item| {
        item.client_state.as_ref().unwrap().uid()
    }).group_by(|ref item| {
        item.client_state.as_ref().unwrap().uid()
    }) {
        let dest = dest.join(owner.to_string());
        if !args.dry_run {
            std::fs::create_dir(&dest)?;
        }
        let mut path_map = HashMap::new();
        for (n, ent) in successors(Some(0), |n| Some(n + 1)).zip(g) {
            let fpath = ent.path();
            let fdest = dest.join(n.to_string());
            let epath = escape_path(&fpath);
            let meta = ent.client_state.unwrap();
            let atime = format_time(meta.atime())?;
            let ctime = format_time(meta.ctime())?;
            let mtime = format_time(meta.mtime())?;
            println!("{}, {}, {}, {:6}, {}", atime, ctime, mtime, owner, epath);
            path_map.insert(escape_path(&fdest), epath);
            if !args.dry_run {
                std::fs::rename(fpath, fdest)?;
            }
        }
        if !args.dry_run {
            let f = std::fs::File::create(dest.join("map.json"))?;
            serde_json::to_writer(f, &path_map)?;
        }
    }
    Ok(())
}
