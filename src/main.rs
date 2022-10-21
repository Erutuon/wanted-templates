use std::{
    collections::{BTreeMap as Map, HashSet as Set},
    convert::TryFrom,
    path::PathBuf,
};

use parse_mediawiki_sql::{
    field_types::{LinkTargetId, PageNamespace, PageTitle},
    iterate_sql_insertions,
    schemas::{LinkTarget, Page, TemplateLink},
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
    let link_target_sql = get_mmap_from_args(["-l", "--link-target"], "linktarget.sql")?;

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
    let template_link_targets: Map<LinkTargetId, String> = iterate_sql_insertions(&link_target_sql)
        .filter(
            |LinkTarget {
                 namespace,
                 title: PageTitle(title),
                 ..
             }| {
                // Requirements of the link targets that we are interested in:
                // 1. they are template titles,
                // 2. they aren't Template:tracking or a subpage of it,
                // 3. the page for the title doesn't exist.
                *namespace == TEMPLATE_NAMESPACE
                    && !((title.starts_with("tracking")
                        && (title.len() == "tracking".len()
                            || title.get("tracking".len().."tracking".len() + 1) == Some("/")))
                        || template_titles.contains(title))
            },
        )
        .map(|LinkTarget { id, title, .. }| (id, title.into_inner()))
        .collect();
    let mut wanted_template_counts = iterate_sql_insertions(&template_links_sql)
        .filter(|TemplateLink { from, .. }| entry_ids.contains(from))
        .filter_map(|TemplateLink { target_id, .. }| {
            template_link_targets
                .get(&target_id)
                .cloned()
                .map(UniCase::new)
        })
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
