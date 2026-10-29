# Contract regression corpus

Real plan documents and `plan_status` snapshots, each in its stored form
(`_id` and `_rev` included), with a declared expectation. Every document here
is generated: do not edit one by hand.

The corpus protects the published contract in two directions. A schema
change that breaks a real shape fails a test. A writer change that alters a
shape shows up as a diff to review.

## `current/`: what this release's writers produce

`tests/contract/corpus.py` builds `current/` from fixed inputs, through this
release's writers and producers:

- plan documents through `build_plan_document` and the finalizer's
  `build_finalized_document`, recording real engine attempts;
- snapshots through the ops consumer's `build_plan_snapshot`, over the spool
  those attempts published.

Clocks, generations, owners, run and execution IDs, and storage revisions
(`<n>-corpus`) are fixed, and the build reads no configuration, so a rebuild
reproduces every file byte for byte.
Files are JSON indented by two, with sorted keys, UTF-8 and a final newline.

To regenerate, from the repository root, in the `ygg` environment:

```bash
python -m tests.contract.corpus --write
```

Without `--write`, the command lists the files that differ and writes
nothing. It never writes outside `current/`. `tests/contract/test_corpus.py`
fails while `current/` differs from a rebuild. A regenerated file's diff is a
change to what Yggdrasil writes: review it as a contract change.

`current/manifest.json` has one entry per file:

| Key | Meaning |
| --- | --- |
| `file`, `kind` | The document, and its kind: `plan_document` or `plan_status` |
| `scenario` | What the document shows |
| `provenance` | Which writers and producers made it, and how |
| `expectation` | `valid`, or `invalid` for a document `validate` must reject |
| `problems` | For an invalid document, every problem `validate` reports, as `path` and `code` |
| `sequence`, `position` | For a step in one plan's history: the history, and the step's place in it |
| `derived_from` | For a document changed by hand from another entry's: that entry's file |
| `by_hand` | The top-level fields the corpus set or removed itself |

A few scenarios stand in for writers Yggdrasil does not have in this
release. These are the approval, the rerun request, foreign writers and hand
edits. `by_hand` names each field they set. A document with `derived_from`
differs from that entry only in its `by_hand` fields and its `_rev`.

## `released/`: frozen baselines

`released/<tag>/` holds the positive examples of a released version, in the
same layout as `current/`. Their manifest lists only those examples.

A released edition is never edited, regenerated or replaced. Later releases
add examples rather than redefine old ones. Within a family, an older
document's canonical output must still validate against the newer schema.
A baseline that stops validating signals an incompatible change, which needs
a new family. It is never a reason to edit the baseline.

Negative examples stay in `current/` only. A released entry may name a
`derived_from` file or a `sequence` whose other steps were not released:
those fields describe `current/` and are not checked in a released edition.

`released/` does not exist until the first baseline is prepared, while
preparing the first release with the contract.

## Releasing a baseline

A release's baseline is prepared before the release is tagged, so that the
tagged source contains its own frozen baseline. Whoever prepares the
release:

1. Chooses the release's tag name. The tag need not exist yet.
2. Checks that `current/` is up to date: `python -m tests.contract.corpus`
   reports no differences.
3. Creates `released/<tag>/`, named exactly as the tag.
4. Copies into it every file of `current/` whose manifest entry has
   `"expectation": "valid"`, at the same path.
5. Writes `released/<tag>/manifest.json` holding exactly those entries,
   unchanged, in their order, serialized like every corpus file.
6. Runs `pytest tests/contract/test_corpus.py`, then commits the edition
   with the release's changes.
7. Tags a commit that contains the edition: that commit, or a merge that
   includes it. A published tag is never moved to add a baseline later.
