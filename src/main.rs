use std::{
    collections::{BTreeMap as Map, HashSet as Set},
    fs::File,
    path::Path,
    path::PathBuf,
};

use memmap::Mmap;
use parse_mediawiki_sql::{schemas::Page, schemas::TemplateLinks, types::PageNamespace};
use pico_args::Error as PicoArgsError;
use thiserror::Error;
use unicase::UniCase;

#[derive(Debug, Error)]
enum Error {
    #[error("Error parsing arguments")]
    PicoArgs(#[from] PicoArgsError),
    #[error("Failed to {action} at {}", path.canonicalize().as_ref().unwrap_or(path).display())]
    IoError {
        action: &'static str,
        source: std::io::Error,
        path: PathBuf,
    },
}
unsafe fn memory_map(path: &Path) -> Result<Mmap, Error> {
    Mmap::map(&File::open(path).map_err(|source| Error::IoError {
        action: "open file",
        source,
        path: path.into(),
    })?)
    .map_err(|source| Error::IoError {
        action: "memory map file",
        source,
        path: path.into(),
    })
}

fn get_mmap_from_args(
    args: &mut pico_args::Arguments,
    keys: impl Into<pico_args::Keys>,
    default: &str,
) -> anyhow::Result<Mmap> {
    let path = args
        .value_from_os_str(keys, |opt| {
            Result::<_, PicoArgsError>::Ok(PathBuf::from(opt))
        })
        .unwrap_or_else(|_| default.into());
    Ok(unsafe { memory_map(&path)? })
}

fn main() -> anyhow::Result<()> {
    let mut args = pico_args::Arguments::from_env();
    let page_sql = get_mmap_from_args(&mut args, ["-p", "--page"], "page.sql")?;
    let template_links_sql =
        get_mmap_from_args(&mut args, ["-t", "--template-links"], "templatelinks.sql")?;
    let template_titles: Set<_> = parse_mediawiki_sql::iterate_sql_insertions(&page_sql)
        .filter_map(
            |Page {
                 namespace, title, ..
             }| {
                if namespace == PageNamespace::from(10) {
                    Some(title.into_inner())
                } else {
                    None
                }
            },
        )
        .collect();
    let mut wanted_template_counts = parse_mediawiki_sql::iterate_sql_insertions(&template_links_sql)
        .filter_map(
            |TemplateLinks {
                 namespace, title, ..
             }| {
                // Not Template:tracking or a subpage of it.
                let title = title.into_inner();
                if namespace == PageNamespace::from(10)
                    && !title.starts_with("tracking")
                    && !template_titles.contains(&title)
                {
                    Some(UniCase::new(title))
                } else {
                    None
                }
            },
        )
        .fold(Map::new(), |mut counts, title| {
            *counts.entry(title).or_insert(0usize) += 1;
            counts
        }).into_iter().collect::<Vec<_>>();
    wanted_template_counts.sort_by(|(title1, count1), (title2, count2)| {
        count1.cmp(&count2).reverse().then_with(|| {
            title1.cmp(&title2)
        })
    });
    for (title, count) in wanted_template_counts {
        println!("{}\t{}", title, count);
    }
    Ok(())
}
