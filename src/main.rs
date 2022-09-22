use std::{
    collections::{BTreeMap as Map, HashSet as Set},
    convert::TryFrom,
    path::PathBuf,
};

use parse_mediawiki_sql::{
    field_types::PageNamespace, iterate_sql_insertions, schemas::Page, schemas::TemplateLink,
    utils::memory_map,
};
use unicase::UniCase;

const MAIN_NAMESPACE: PageNamespace = PageNamespace(0);
const TEMPLATE_NAMESPACE: PageNamespace = PageNamespace(10);
const RECONSTRUCTION_NAMESPACE: PageNamespace = PageNamespace(118);

fn main() -> anyhow::Result<()> {
    let mut args = pico_args::Arguments::from_env();
    let mut get_mmap_from_args = |keys, default: &str| {
        let path = args
            .value_from_os_str(keys, |opt| PathBuf::try_from(opt))
            .unwrap_or_else(|_| default.into());
        unsafe { memory_map(&path) }
    };
    let page_sql = get_mmap_from_args(["-p", "--page"], "page.sql")?;
    let template_links_sql = get_mmap_from_args(["-t", "--template-links"], "templatelinks.sql")?;

    let (template_titles, entry_ids) = iterate_sql_insertions(&page_sql).fold(
        (Set::new(), Set::new()),
        |(mut titles, mut ids),
         Page {
             id,
             namespace,
             title,
             ..
         }| {
            if namespace == TEMPLATE_NAMESPACE {
                titles.insert(title.into_inner());
            } else if namespace == MAIN_NAMESPACE || namespace == RECONSTRUCTION_NAMESPACE {
                ids.insert(id);
            }
            (titles, ids)
        },
    );
    let mut wanted_template_counts = iterate_sql_insertions(&template_links_sql)
        .filter_map(
            |TemplateLink {
                 from,
                 namespace,
                 title,
                 ..
             }| {
                // Not Template:tracking or a subpage of it.
                let title = title.into_inner();
                if namespace == TEMPLATE_NAMESPACE
                    && !(title.starts_with("tracking") || template_titles.contains(&title))
                    && entry_ids.contains(&from)
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
            .cmp(count2)
            .reverse()
            .then_with(|| title1.cmp(title2))
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
