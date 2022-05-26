use std::{
    collections::{BTreeMap as Map, HashSet as Set},
    convert::TryFrom,
    path::PathBuf,
};

use parse_mediawiki_sql::{
    field_types::PageNamespace, schemas::Page, schemas::TemplateLink, utils::memory_map,
};
use unicase::UniCase;

fn main() -> anyhow::Result<()> {
    let mut args = pico_args::Arguments::from_env();
    let mut get_mmap_from_args = |keys, default: &str| {
        #[allow(clippy::redundant_closure)]
        let path = args
            .value_from_os_str(keys, |opt| PathBuf::try_from(opt))
            .unwrap_or_else(|_| default.into());
        unsafe { memory_map(&path) }
    };
    let page_sql = get_mmap_from_args(["-p", "--page"], "page.sql")?;
    let template_links_sql = get_mmap_from_args(["-t", "--template-links"], "templatelinks.sql")?;
    let template_titles: Set<_> = parse_mediawiki_sql::iterate_sql_insertions(&page_sql)
        .filter_map(
            |Page {
                 namespace, title, ..
             }| {
                if namespace == PageNamespace(10) {
                    Some(title.into_inner())
                } else {
                    None
                }
            },
        )
        .collect();
    let mut wanted_template_counts =
        parse_mediawiki_sql::iterate_sql_insertions(&template_links_sql)
            .filter_map(
                |TemplateLink {
                     namespace, title, ..
                 }| {
                    // Not Template:tracking or a subpage of it.
                    let title = title.into_inner();
                    if namespace == PageNamespace(10)
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
            })
            .into_iter()
            .collect::<Vec<_>>();
    wanted_template_counts.sort_by(|(title1, count1), (title2, count2)| {
        count1
            .cmp(&count2)
            .reverse()
            .then_with(|| title1.cmp(&title2))
    });
    for (mut title, count) in wanted_template_counts {
        // This does not violate `String`'s invariants because it only replaces ASCII bytes with ASCII bytes.
        unsafe {
            let title = title.as_bytes_mut();
            for b in title {
                if *b == b'_' {
                    *b = b' ';
                }
            }
        }
        println!("{}\t{}", title, count);
    }
    Ok(())
}
