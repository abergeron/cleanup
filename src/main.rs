use std::ffi::OsString;
use std::os::unix::{fs::MetadataExt, ffi::OsStringExt, fs::DirBuilderExt};
use std::io::Write;
use std::collections::HashMap;
use std::path::PathBuf;

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
                    anyhow!("dest path must be valid utf-8"))?
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
                        let mut res = true;
                        if !args.noatime {
                            res = res && meta.atime() < cutoff
                        }
                        if !args.nomtime {
                            res = res && meta.mtime() < cutoff
                        }
                        if !args.noctime {
                            res = res && meta.ctime() < cutoff
                        }
                        res
                    })
                }).unwrap_or(false)
            });
        });

    let mut stdout = std::io::stdout();
    stdout.write_all("atime             ctime             mtime             UID     Path\n".as_bytes())?;
    let paths_db = sled::Config::new()
        .path(dest.join("paths.db"))
        .mode(sled::Mode::HighThroughput)
        .use_compression(true)
        .open()?;
    let uid_tree = paths_db.open_tree("cleanup_uids")?;
    for ent in walk_dir.into_iter().filter_map(|item| {
        if let Ok(ent) = item {
            if ent.file_type.is_file() {
                return match ent.client_state {
                    Some(_) => Some(ent),
                    _ => None
                }
            }
        }
        return None;
    }) {
        let meta = ent.client_state.as_ref().unwrap();
        let uid = meta.uid();
        let n = u32::from_le_bytes(uid_tree.update_and_fetch(uid.to_le_bytes(), |old| {
            let number = match old {
                Some(bytes) => {
                    let number = u32::from_le_bytes(bytes.try_into().unwrap());
                    number + 1
                }
                None => 0,
            };
            Some(number.to_le_bytes().to_vec())
        })?.unwrap().as_ref().try_into().unwrap());
        let udest = dest.join(uid.to_string());
        if !udest.is_dir() && !args.dry_run {
            std::fs::DirBuilder::new().mode(0o700).create(&udest)?;
            std::os::unix::fs::chown(&udest, Some(uid), None)?;
        }
        let fpath = ent.path();
        let fdest = udest.join(n.to_string());
        let epath = escape_path(&fpath);
        let atime = format_time(meta.atime())?;
        let ctime = format_time(meta.ctime())?;
        let mtime = format_time(meta.mtime())?;
        println!("{}, {}, {}, {:6}, {}", atime, ctime, mtime, uid, epath);
        let path_map = paths_db.open_tree(uid.to_le_bytes())?;
        if !args.dry_run {
            std::fs::rename(fpath, &fdest)?;
        }
        path_map.insert(escape_path(&fdest), epath.into_bytes())?;
    }
    if !args.dry_run {
        // Generate the map.json files from the central database
        for name in paths_db.tree_names() {
            if name.len() != 4 {
                continue;
            }
            let uid = u32::from_le_bytes(name.as_ref().try_into()?);
            let path_map: HashMap<String, String> =
                paths_db.open_tree(name)?.iter().map(|item| {
                    let (key, val) = item.unwrap();
                    (String::from_utf8(key.to_vec()).unwrap(),
                     String::from_utf8(val.to_vec()).unwrap())
                }).collect();
            let mpath = dest.join(uid.to_string()).join("map.json");
            let f = std::fs::File::create(&mpath)?;
            std::os::unix::fs::chown(&mpath, Some(uid), None)?;
            serde_json::to_writer(f, &path_map)?;
        }
    }
    paths_db.flush()?;
    Ok(())
}
