"""The regression corpus: what this release writes, and what it must keep valid.

``tests/contract/documents/current/`` is what this release's writers produce
(:mod:`tests.contract.corpus`). Its manifest declares each document's
expectation, how it was made and its place in a plan's history; these tests
hold the files to those declarations and to a rebuild.

``released/<tag>/`` holds a released version's positive examples, frozen.
Each must keep validating against its family's current schema, and nothing
the corpus does changes them. No version has been released with the contract
yet, so those tests also run against an edition released, as the README
describes, from the current one.
"""

from __future__ import annotations

import functools
import io
import json
import shutil
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from tests.contract import corpus
from tests.contract.corpus import (
    CURRENT,
    DOCUMENTS,
    INVALID,
    MANIFEST,
    RELEASED,
    VALID,
    Differences,
    build,
    differences,
    edition_files,
    serialize,
    write,
)
from tests.contract.support import problems
from tests.execution_support import directory_tree
from yggdrasil.contract import PLAN_STATUS
from yggdrasil.contract.controls import token_pair
from yggdrasil.contract.schema import ContractError, validate

# The fields planning writes. A new generation carries nothing else: no actor
# stamps, no finalizer fields and no field another writer added.
PLANNED_FIELDS = frozenset(
    {
        "_id",
        "_rev",
        "schema_version",
        "realm",
        "scope",
        "status",
        "plan",
        "preview",
        "plan_generation",
        "run_token",
        "executed_run_token",
        "execution_authority",
        "execution_owner",
        "created_at",
        "updated_at",
        "source_doc_id",
        "source_doc_rev",
        "notes",
    }
)

# How to regenerate the current edition, for a failure message.
REGENERATE = (
    "If the change is intended, regenerate it with "
    "`python -m tests.contract.corpus --write` and review the diff as a "
    "contract change. Released editions (documents/released/) are never "
    "regenerated."
)


@functools.cache
def rebuilt() -> dict[str, bytes]:
    """Return the current edition as this release's writers build it, once."""
    return build()


def manifest_entries(files: dict[str, bytes]) -> list[dict[str, Any]]:
    """Return the entries of an edition's manifest, given the edition's files."""
    return json.loads(files[MANIFEST])["entries"]


def differing_files(first: dict[str, bytes], second: dict[str, bytes]) -> list[str]:
    """Return the paths at which two sets of files differ, sorted."""
    return sorted(
        name
        for name in first.keys() | second.keys()
        if first.get(name) != second.get(name)
    )


def differing_fields(first: dict[str, Any], second: dict[str, Any]) -> set[str]:
    """Return the top-level fields two documents hold differently, ``_rev`` aside.

    A field present in one only differs, and values are compared as stored, so
    ``1``, ``1.0`` and ``true`` are three values.
    """

    def stored(document: dict[str, Any], key: str) -> bytes | None:
        return serialize(document[key]) if key in document else None

    return {
        key
        for key in first.keys() | second.keys()
        if key != "_rev" and stored(first, key) != stored(second, key)
    }


def instant(timestamp: str) -> datetime:
    """Return the instant an RFC 3339 timestamp names, however it is spelled."""
    return datetime.fromisoformat(timestamp)


def release(current: Path, edition: Path) -> None:
    """Add a released edition made from current, as the README's release step does.

    Args:
        current: The current edition's directory.
        edition: The released edition's directory, which must not exist.
    """
    files = edition_files(current)
    entries = [
        entry for entry in manifest_entries(files) if entry["expectation"] == VALID
    ]
    edition.mkdir(parents=True)
    for entry in entries:
        path = edition / entry["file"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(files[entry["file"]])
    (edition / MANIFEST).write_bytes(serialize({"entries": entries}))


def edition_problems(edition: Path) -> list[str]:
    """Return why a released edition does not hold; nothing when it does.

    It holds when its manifest and its files agree, every entry is a positive
    example, and every document validates against its family's current
    schema. Fields that describe the current edition, such as
    ``derived_from`` and ``sequence``, are not checked.

    Args:
        edition: The released edition's directory.

    Returns:
        list[str]: One line per problem.
    """
    files = edition_files(edition)
    if MANIFEST not in files:
        return [f"{edition.name} has no {MANIFEST}"]
    entries = manifest_entries(files)
    listed = [entry["file"] for entry in entries]
    found = [
        f"{name} is listed twice"
        for name in sorted(set(listed))
        if listed.count(name) > 1
    ]
    found.extend(
        f"{name} has no manifest entry"
        for name in sorted(files.keys() - {MANIFEST} - set(listed))
    )
    for entry in entries:
        name = entry["file"]
        if name not in files:
            found.append(f"{name} is listed but missing")
        elif entry["expectation"] != VALID:
            found.append(f"{name} is not a positive example")
        else:
            try:
                validate(entry["kind"], json.loads(files[name]))
            except ContractError as error:
                found.append(f"{name} no longer validates: {error}")
    return found


class TestCurrentEdition(unittest.TestCase):
    """The current edition holds its declarations, and a rebuild reproduces it."""

    files: dict[str, bytes]
    entries: list[dict[str, Any]]

    @classmethod
    def setUpClass(cls) -> None:
        """Read the current edition once."""
        cls.files = edition_files(DOCUMENTS / CURRENT)
        cls.entries = manifest_entries(cls.files)

    def document(self, file: str) -> Any:
        """Return one of the edition's documents."""
        return json.loads(self.files[file])

    def test_the_manifest_and_the_directory_agree(self):
        listed = [entry["file"] for entry in self.entries]
        self.assertEqual(len(listed), len(set(listed)), "a file is listed twice")
        self.assertEqual(sorted(listed), sorted(self.files.keys() - {MANIFEST}))
        for entry in self.entries:
            with self.subTest(entry["file"]):
                self.assertEqual(entry["file"].split("/")[0], entry["kind"])

    def test_each_document_meets_its_expectation(self):
        for entry in self.entries:
            with self.subTest(entry["file"]):
                declared = [
                    (problem["path"], problem["code"])
                    for problem in entry.get("problems", [])
                ]
                self.assertEqual(entry["expectation"], INVALID if declared else VALID)
                self.assertEqual(
                    problems(entry["kind"], self.document(entry["file"])), declared
                )

    def test_a_rebuild_reproduces_every_file(self):
        found = differences(rebuilt(), DOCUMENTS / CURRENT)
        if found:
            self.fail(
                "The current edition differs from what this release's writers "
                f"produce:\n{found.describe()}\n{REGENERATE}"
            )

    def test_a_rebuild_does_not_depend_on_where_it_runs(self):
        with TemporaryDirectory() as directory:
            target = Path(directory, "target")
            target.mkdir()
            link = Path(directory, "link")
            link.symlink_to(target, target_is_directory=True)

            elsewhere = build(link)

        self.assertEqual(differing_files(elsewhere, rebuilt()), [])

    def test_a_document_changed_by_hand_differs_only_where_declared(self):
        entries = {entry["file"]: entry for entry in self.entries}
        derived = [entry for entry in self.entries if "derived_from" in entry]
        self.assertTrue(derived)
        for entry in derived:
            with self.subTest(entry["file"]):
                self.assertIn(entry["derived_from"], entries)
                base = entries[entry["derived_from"]]
                self.assertEqual(base["kind"], entry["kind"])
                self.assertEqual(
                    differing_fields(
                        self.document(entry["file"]), self.document(base["file"])
                    ),
                    set(entry["by_hand"]),
                )

    def test_each_plan_history_keeps_the_lifecycle_rules(self):
        histories: dict[str, list[dict[str, Any]]] = {}
        for entry in self.entries:
            if "sequence" in entry:
                histories.setdefault(entry["sequence"], []).append(entry)
        self.assertTrue(histories)
        for name, steps in histories.items():
            with self.subTest(name):
                steps.sort(key=lambda entry: entry["position"])
                positions = [entry["position"] for entry in steps]
                self.assertEqual(positions, list(range(1, len(steps) + 1)))
                self.assert_history_keeps_the_rules(steps)

    def assert_history_keeps_the_rules(self, steps: list[dict[str, Any]]) -> None:
        """Assert one plan's history keeps the rules of a document's lifetime.

        A document keeps its ID and creation instant. Within a generation its
        family, its definition and its generation never change, and its run
        token never decreases. A new generation starts over: tokens reset, and
        only what planning writes. A snapshot shows the request admitted from
        the plan document before it.
        """
        plan: dict[str, Any] | None = None
        for entry in steps:
            document = self.document(entry["file"])
            if entry["kind"] == PLAN_STATUS:
                assert plan is not None, f"{entry['file']} follows no plan document"
                attempt = document["attempt"]
                self.assertEqual(document["plan_id"], plan["_id"], entry["file"])
                self.assertEqual(
                    (attempt["plan_generation"], attempt["run_token"]),
                    (plan["plan_generation"], plan["run_token"]),
                    entry["file"],
                )
                continue
            if plan is not None:
                self.assertEqual(document["_id"], plan["_id"], entry["file"])
                self.assertEqual(
                    instant(document["created_at"]),
                    instant(plan["created_at"]),
                    entry["file"],
                )
                if document["plan_generation"] == plan["plan_generation"]:
                    self.assertEqual(
                        differing_fields(document, plan) & {"schema_version", "plan"},
                        set(),
                        entry["file"],
                    )
                    run, previous = token_pair(document).run, token_pair(plan).run
                    assert run is not None and previous is not None, entry["file"]
                    self.assertGreaterEqual(run, previous, entry["file"])
                else:
                    self.assertLessEqual(set(document), PLANNED_FIELDS, entry["file"])
                    self.assertEqual(
                        (document["run_token"], document["executed_run_token"]),
                        (0, -1),
                        entry["file"],
                    )
            plan = document


class TestReleasedEditions(unittest.TestCase):
    """Released editions hold, and nothing the corpus does changes them."""

    def setUp(self) -> None:
        temp_dir = TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)

    def released_copy(self, tag: str = "v0.0.0") -> Path:
        """Release the current edition into the temporary root, and return it."""
        edition = self.root / RELEASED / tag
        release(DOCUMENTS / CURRENT, edition)
        return edition

    def test_every_released_edition_still_holds(self):
        released = DOCUMENTS / RELEASED
        editions = sorted(path for path in released.glob("*") if path.is_dir())
        for edition in editions:
            with self.subTest(edition.name):
                self.assertEqual(edition_problems(edition), [])

    def test_an_edition_released_from_the_current_one_holds(self):
        edition = self.released_copy()

        self.assertEqual(edition_problems(edition), [])
        entries = manifest_entries(edition_files(edition))
        current = manifest_entries(edition_files(DOCUMENTS / CURRENT))
        self.assertEqual(
            entries, [entry for entry in current if entry["expectation"] == VALID]
        )

    def test_an_edition_that_does_not_hold_is_reported(self):
        current = edition_files(DOCUMENTS / CURRENT)
        negative = next(
            entry
            for entry in manifest_entries(current)
            if entry["expectation"] == INVALID
        )

        def list_a_negative(edition: Path) -> str:
            entries = [*manifest_entries(edition_files(edition)), negative]
            (edition / MANIFEST).write_bytes(serialize({"entries": entries}))
            (edition / negative["file"]).write_bytes(current[negative["file"]])
            return f"{negative['file']} is not a positive example"

        def add_an_unlisted_file(edition: Path) -> str:
            (edition / "plan_document" / "extra.json").write_text("{}\n")
            return "plan_document/extra.json has no manifest entry"

        def remove_a_listed_file(edition: Path) -> str:
            (edition / "plan_document" / "draft.json").unlink()
            return "plan_document/draft.json is listed but missing"

        def break_a_baseline(edition: Path) -> str:
            path = edition / "plan_document" / "draft.json"
            document = json.loads(path.read_bytes())
            del document["run_token"]
            path.write_bytes(serialize(document))
            return "plan_document/draft.json no longer validates"

        for change in (
            list_a_negative,
            add_an_unlisted_file,
            remove_a_listed_file,
            break_a_baseline,
        ):
            with self.subTest(change.__name__):
                edition = self.released_copy(change.__name__)
                expected = change(edition)

                found = edition_problems(edition)

                self.assertEqual(len(found), 1, found)
                self.assertTrue(found[0].startswith(expected), found)

    def test_writing_the_current_edition_leaves_released_editions_untouched(self):
        shutil.copytree(DOCUMENTS / CURRENT, self.root / CURRENT)
        edition = self.released_copy()
        current = self.root / CURRENT
        (current / "plan_document" / "draft.json").write_text("{}\n")
        (current / "plan_status" / "legacy.json").unlink()
        (current / "plan_document" / "retired.json").write_text("{}\n")
        # Even an edition that no longer holds is left as it is.
        (edition / "plan_document" / "draft.json").write_text("{}\n")
        released = directory_tree(self.root / RELEASED)

        found = write(self.root)

        self.assertEqual(directory_tree(self.root / RELEASED), released)
        self.assertEqual(differing_files(edition_files(current), rebuilt()), [])
        self.assertEqual(
            found,
            Differences(
                changed=("plan_document/draft.json",),
                missing=("plan_status/legacy.json",),
                unexpected=("plan_document/retired.json",),
            ),
        )

    def test_the_command_writes_only_when_asked(self):
        shutil.copytree(DOCUMENTS / CURRENT, self.root / CURRENT)
        stale = self.root / CURRENT / "plan_document" / "draft.json"
        stale.write_text("{}\n")
        output = io.StringIO()

        with patch.object(corpus, "DOCUMENTS", self.root), redirect_stdout(output):
            compared = corpus.main([])
            unchanged = stale.read_text()
            written = corpus.main(["--write"])

        self.assertEqual((compared, written), (1, 0))
        self.assertEqual(unchanged, "{}\n")
        self.assertIn("changed: plan_document/draft.json", output.getvalue())
        self.assertEqual(
            differing_files(edition_files(self.root / CURRENT), rebuilt()), []
        )


if __name__ == "__main__":
    unittest.main()
