use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt::Write;
use std::os::unix::{fs::DirBuilderExt, fs::MetadataExt};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::{
    offset::{Local, TimeZone},
    Duration, LocalResult,
};
use clap::Parser;
use ignore::{gitignore::GitignoreBuilder, overrides::OverrideBuilder, WalkBuilder, WalkState};

#[derive(Parser)]
#[command(name = "cleanup", version = "0.0.1")]
struct Args {
    #[arg(help = "Path to scan")]
    path: OsString,

    #[arg(long = "dest", help = "Destination path for the move")]
    dest: OsString,

    #[arg(
        long = "num-threads",
        help = "Number of threads to use [default: number of cores]"
    )]
    num_threads: Option<usize>,

    #[arg(
        long = "dry-run",
        help = "Only print the files that would be moved, but don't move anything"
    )]
    dry_run: bool,

    #[arg(long = "exclude-file", help = "File containing paths to exclude")]
    exclude_file: Option<OsString>,

    #[arg(
        long = "older",
        help = "Number of days old the files must be to be selected",
        default_value = "0"
    )]
    older: i64,

    #[arg(long = "noatime", help = "Don't look at atime to determine age")]
    noatime: bool,
    #[arg(long = "nomtime", help = "Don't look at mtime to determine age")]
    nomtime: bool,
    #[arg(long = "noctime", help = "Don't look at ctime to determine age")]
    noctime: bool,
}

struct CleanContext {
    noatime: bool,
    noctime: bool,
    nomtime: bool,
    dry_run: bool,
    dest: PathBuf,
    path: PathBuf,
    cutoff: i64,
    paths_db: sled::Db,
    uid_tree: sled::Tree,
}

fn format_time(ts: i64) -> Result<String> {
    match Local.timestamp_opt(ts, 0) {
        LocalResult::None => Ok("Invalid date    ".into()),
        LocalResult::Single(d) => Ok(d.format("%Y-%m-%d %H:%M").to_string()),
        LocalResult::Ambiguous(_, _) => return Err(anyhow!("This should not happen (1)")),
    }
}

fn escape_path(path: &Path) -> String {
    let input = path.as_os_str().as_encoded_bytes();
    let mut res = String::with_capacity(input.len());
    let mut encoding = false;
    res.push('\'');
    for b in input {
        if b >= &0x7f || b < &0x20 {
            if !encoding {
                res.push_str(&"'$'");
                encoding = true;
            }
            write!(res, "\\{:03o}", b).unwrap();
        } else {
            if encoding {
                res.push_str("''");
                encoding = false;
            }
            res.push(char::from_u32((*b).into()).unwrap());
        }
    }
    res.push('\'');
    res
}
#[cfg(test)]
mod tests {
    use crate::escape_path;
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;
    #[test]
    fn test_escape_path_invalid_utf8() {
        let source = [0x66, 0x6f, 0x80, 0x6f];
        let os_str = OsStr::from_bytes(&source);
        let res = escape_path(Path::new(os_str));
        assert_eq!(res, "'fo'$'\\200''o'");
    }
    #[test]
    fn test_escape_path_sparkle_heart() {
        let source = [240, 159, 146, 150];
        let os_str = OsStr::from_bytes(&source);
        let res = escape_path(Path::new(os_str));
        assert_eq!(res, "''$'\\360\\237\\222\\226'");
    }
    #[test]
    fn test_escape_path_limits() {
        let source = [1, 31, 32, 0x7f, 0x7e];
        let os_str = OsStr::from_bytes(&source);
        let res = escape_path(Path::new(os_str));
        assert_eq!(res, "''$'\\001\\037'' '$'\\177''~'");
    }
}

fn process_ent(
    ent_result: Result<ignore::DirEntry, ignore::Error>,
    ctx: &CleanContext,
) -> WalkState {
    match ent_result {
        Ok(ent) => {
            if ent.file_type().map_or(false, |v| v.is_file()) {
                match process_file(ent, ctx) {
                    Ok(state) => state,
                    Err(e) => {
                        eprintln!("{}", e)
                    }
                };
            };
        }
        // We skip errors
        Err(e) => {
            eprintln!("{}", e)
        }
    };
    WalkState::Continue
}

fn process_file(ent: ignore::DirEntry, ctx: &CleanContext) -> Result<()> {
    let meta = ent.metadata()?;
    let mut old = true;
    if !ctx.noatime {
        old = old && meta.atime() < ctx.cutoff
    }
    if !ctx.nomtime {
        old = old && meta.mtime() < ctx.cutoff
    }
    if !ctx.noctime {
        old = old && meta.ctime() < ctx.cutoff
    }
    if !old {
        return Ok(());
    }
    let uid = meta.uid();
    let n = u32::from_le_bytes(
        ctx.uid_tree
            .update_and_fetch(uid.to_le_bytes(), |old| {
                let number = match old {
                    Some(bytes) => {
                        let number = u32::from_le_bytes(bytes.try_into().unwrap());
                        number + 1
                    }
                    None => 0,
                };
                Some(number.to_le_bytes().to_vec())
            })
            .unwrap()
            .unwrap()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let udest = ctx.dest.join(uid.to_string());
    if !udest.is_dir() && !ctx.dry_run {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .recursive(true)
            .create(&udest)?;
        std::os::unix::fs::lchown(&udest, Some(uid), None)?;
    }
    let fpath = ent.path();
    let fdest = udest.join(n.to_string());
    let epath = escape_path(fpath);
    let atime = format_time(meta.atime())?;
    let ctime = format_time(meta.ctime())?;
    let mtime = format_time(meta.mtime())?;
    {
        // This is to ensure that output is not mixed
        use std::io::Write;
        let stdout = std::io::stdout();
        writeln!(
            &mut stdout.lock(),
            "{}, {}, {}, {:6}, {}",
            atime,
            ctime,
            mtime,
            uid,
            epath
        )?;
    }
    let path_map = ctx.paths_db.open_tree(uid.to_le_bytes())?;
    if !ctx.dry_run {
        std::fs::rename(fpath, &fdest)?;
    }
    path_map.insert(escape_path(&fdest), epath.into_bytes())?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let _dest = std::fs::canonicalize(args.dest).context("Canonicalizng destination path")?;
    let _paths_db = sled::Config::new()
        .path(_dest.join("paths.db"))
        .mode(sled::Mode::HighThroughput)
        .use_compression(true)
        .open()?;

    let ctx = CleanContext {
        noatime: args.noatime,
        nomtime: args.nomtime,
        noctime: args.noctime,
        dry_run: args.dry_run,
        cutoff: (Local::now() - Duration::days(args.older)).timestamp(),
        dest: _dest,
        path: std::fs::canonicalize(args.path).context("Canonicalizing source path")?,
        uid_tree: _paths_db.open_tree("cleanup_uids")?,
        paths_db: _paths_db,
    };

    let num_t = args.num_threads.unwrap_or(0);

    let mut walk_dir_build = WalkBuilder::new(&ctx.path);
    walk_dir_build.follow_links(false);
    walk_dir_build.threads(num_t);
    walk_dir_build.standard_filters(false);

    let mut overrides = OverrideBuilder::new(&ctx.path);
    if let Some(file) = args.exclude_file {
        let fp = std::fs::canonicalize(file).context("Canonicalizing exclude file")?;
        let mut builder = GitignoreBuilder::new(&ctx.path);
        builder.add(&fp);
        let filter = builder.build().context("parsing exclude file")?;
        walk_dir_build.filter_entry(move |dirent| {
            !filter
                .matched(
                    dirent.path(),
                    dirent.file_type().map_or(false, |v| v.is_dir()),
                )
                .is_ignore()
        });
        if fp.starts_with(&ctx.path) {
            overrides.add(
                &("!/".to_owned()
                    + fp.strip_prefix(&ctx.path)?
                        .to_str()
                        .ok_or(anyhow!("ignore file path must be valid utf-8"))?),
            )?;
        }
    }

    if ctx.dest.starts_with(&ctx.path) {
        overrides.add(
            &("!/".to_owned()
                + ctx
                    .dest
                    .strip_prefix(&ctx.path)?
                    .to_str()
                    .ok_or(anyhow!("dest path must be valid utf-8"))?
                + "/"),
        )?;
    }
    walk_dir_build.overrides(overrides.build()?);
    let walk_dir = walk_dir_build.build_parallel();

    println!("atime             ctime             mtime             UID     Path");

    // This works in parallel
    walk_dir.run(|| Box::new(|ent| process_ent(ent, &ctx)));

    if !ctx.dry_run {
        // Generate the map.json files from the central database
        for name in ctx.paths_db.tree_names() {
            if name.len() != 4 {
                continue;
            }
            let uid = u32::from_le_bytes(name.as_ref().try_into()?);
            let path_map: HashMap<String, String> = ctx
                .paths_db
                .open_tree(name)?
                .iter()
                .map(|item| {
                    let (key, val) = item.unwrap();
                    (
                        String::from_utf8(key.to_vec()).unwrap(),
                        String::from_utf8(val.to_vec()).unwrap(),
                    )
                })
                .collect();
            let mpath = ctx.dest.join(uid.to_string()).join("map.json");
            let f = std::fs::File::create(&mpath)?;
            std::os::unix::fs::chown(&mpath, Some(uid), None)?;
            serde_json::to_writer(f, &path_map)?;
        }
    }
    ctx.paths_db.flush()?;
    Ok(())
}
