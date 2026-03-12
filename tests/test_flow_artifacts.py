"""
Comprehensive tests for yggdrasil.flow.artifacts module.
Tests cover SimpleArtifactRef and ensure_artifact_ref functionality.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from yggdrasil.flow.artifacts import (
    ArtifactRefProtocol,
    SimpleArtifactRef,
    ensure_artifact_ref,
)


class TestSimpleArtifactRef(unittest.TestCase):
    """Test SimpleArtifactRef dataclass and its methods."""

    def test_initialization_with_filename(self):
        """SimpleArtifactRef should initialize with all parameters."""
        ref = SimpleArtifactRef(
            key_name="test_key", folder="test_folder", filename="test.txt"
        )

        self.assertEqual(ref.key_name, "test_key")
        self.assertEqual(ref.folder, "test_folder")
        self.assertEqual(ref.filename, "test.txt")

    def test_initialization_without_filename(self):
        """SimpleArtifactRef should initialize without filename (directory ref)."""
        ref = SimpleArtifactRef(key_name="dir_key", folder="output_dir")

        self.assertEqual(ref.key_name, "dir_key")
        self.assertEqual(ref.folder, "output_dir")
        self.assertIsNone(ref.filename)

    def test_initialization_explicit_none_filename(self):
        """SimpleArtifactRef should accept explicit None for filename."""
        ref = SimpleArtifactRef(key_name="dir_key", folder="output_dir", filename=None)

        self.assertEqual(ref.key_name, "dir_key")
        self.assertEqual(ref.folder, "output_dir")
        self.assertIsNone(ref.filename)

    def test_key_method_returns_key_name(self):
        """key() method should return the key_name."""
        ref = SimpleArtifactRef(
            key_name="my_artifact", folder="artifacts", filename="data.json"
        )

        self.assertEqual(ref.key(), "my_artifact")

    def test_key_method_different_values(self):
        """key() method should return different values for different refs."""
        ref1 = SimpleArtifactRef(key_name="key1", folder="folder1")
        ref2 = SimpleArtifactRef(key_name="key2", folder="folder2")

        self.assertEqual(ref1.key(), "key1")
        self.assertEqual(ref2.key(), "key2")
        self.assertNotEqual(ref1.key(), ref2.key())

    def test_resolve_path_with_filename(self):
        """resolve_path should return path to file when filename is set."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            ref = SimpleArtifactRef(
                key_name="file_key", folder="subdir", filename="output.txt"
            )

            path = ref.resolve_path(scope_dir)

            expected = scope_dir / "subdir" / "output.txt"
            self.assertEqual(path, expected)

    def test_resolve_path_without_filename(self):
        """resolve_path should return directory path when filename is None."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            ref = SimpleArtifactRef(key_name="dir_key", folder="subdir")

            path = ref.resolve_path(scope_dir)

            expected = scope_dir / "subdir"
            self.assertEqual(path, expected)

    def test_resolve_path_creates_directory(self):
        """resolve_path should create the directory if it doesn't exist."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            ref = SimpleArtifactRef(
                key_name="key", folder="new_folder", filename="file.txt"
            )

            # Directory should not exist yet
            dir_path = scope_dir / "new_folder"
            self.assertFalse(dir_path.exists())

            path = ref.resolve_path(scope_dir)

            # Directory should now exist
            self.assertTrue(dir_path.exists())
            self.assertTrue(dir_path.is_dir())
            self.assertEqual(path, dir_path / "file.txt")

    def test_resolve_path_creates_nested_directories(self):
        """resolve_path should create nested directories with parents=True."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            ref = SimpleArtifactRef(
                key_name="key", folder="level1/level2/level3", filename="deep.txt"
            )

            path = ref.resolve_path(scope_dir)

            expected_dir = scope_dir / "level1" / "level2" / "level3"
            self.assertTrue(expected_dir.exists())
            self.assertTrue(expected_dir.is_dir())
            self.assertEqual(path, expected_dir / "deep.txt")

    def test_resolve_path_idempotent(self):
        """resolve_path should be idempotent (calling twice is safe)."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            ref = SimpleArtifactRef(
                key_name="key", folder="folder", filename="file.txt"
            )

            path1 = ref.resolve_path(scope_dir)
            path2 = ref.resolve_path(scope_dir)

            self.assertEqual(path1, path2)
            self.assertTrue((scope_dir / "folder").exists())

    def test_resolve_path_different_scope_dirs(self):
        """resolve_path should produce different paths for different scope_dirs."""
        with TemporaryDirectory() as tmpdir1, TemporaryDirectory() as tmpdir2:
            scope1 = Path(tmpdir1)
            scope2 = Path(tmpdir2)
            ref = SimpleArtifactRef(
                key_name="key", folder="folder", filename="file.txt"
            )

            path1 = ref.resolve_path(scope1)
            path2 = ref.resolve_path(scope2)

            self.assertNotEqual(path1, path2)
            self.assertTrue(path1.is_relative_to(scope1))
            self.assertTrue(path2.is_relative_to(scope2))

    def test_frozen_dataclass(self):
        """SimpleArtifactRef should be frozen (immutable)."""
        ref = SimpleArtifactRef(key_name="key", folder="folder", filename="file.txt")

        with self.assertRaises(Exception):  # FrozenInstanceError or AttributeError
            ref.key_name = "new_key"  # type: ignore[misc]

    def test_equality(self):
        """Two SimpleArtifactRef instances with same values should be equal."""
        ref1 = SimpleArtifactRef(key_name="key", folder="folder", filename="file.txt")
        ref2 = SimpleArtifactRef(key_name="key", folder="folder", filename="file.txt")

        self.assertEqual(ref1, ref2)

    def test_inequality_different_key_name(self):
        """SimpleArtifactRef instances with different key_names should not be equal."""
        ref1 = SimpleArtifactRef(key_name="key1", folder="folder", filename="file.txt")
        ref2 = SimpleArtifactRef(key_name="key2", folder="folder", filename="file.txt")

        self.assertNotEqual(ref1, ref2)

    def test_inequality_different_folder(self):
        """SimpleArtifactRef instances with different folders should not be equal."""
        ref1 = SimpleArtifactRef(key_name="key", folder="folder1", filename="file.txt")
        ref2 = SimpleArtifactRef(key_name="key", folder="folder2", filename="file.txt")

        self.assertNotEqual(ref1, ref2)

    def test_inequality_different_filename(self):
        """SimpleArtifactRef instances with different filenames should not be equal."""
        ref1 = SimpleArtifactRef(key_name="key", folder="folder", filename="file1.txt")
        ref2 = SimpleArtifactRef(key_name="key", folder="folder", filename="file2.txt")

        self.assertNotEqual(ref1, ref2)

    def test_hashable(self):
        """SimpleArtifactRef should be hashable (can be used in sets/dicts)."""
        ref1 = SimpleArtifactRef(key_name="key", folder="folder", filename="file.txt")
        ref2 = SimpleArtifactRef(key_name="key", folder="folder", filename="file.txt")
        ref3 = SimpleArtifactRef(
            key_name="other_key", folder="folder", filename="file.txt"
        )

        # Should be able to create a set
        ref_set = {ref1, ref2, ref3}
        self.assertEqual(len(ref_set), 2)  # ref1 and ref2 are equal

        # Should be able to use as dict key
        ref_dict = {ref1: "value1", ref3: "value3"}
        self.assertEqual(len(ref_dict), 2)

    def test_repr(self):
        """SimpleArtifactRef should have a useful repr."""
        ref = SimpleArtifactRef(
            key_name="my_key", folder="my_folder", filename="my_file.txt"
        )

        repr_str = repr(ref)
        self.assertIn("SimpleArtifactRef", repr_str)
        self.assertIn("my_key", repr_str)
        self.assertIn("my_folder", repr_str)
        self.assertIn("my_file.txt", repr_str)

    def test_resolve_path_with_relative_folder(self):
        """resolve_path should work with relative folder paths."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            ref = SimpleArtifactRef(
                key_name="key", folder="./relative/path", filename="file.txt"
            )

            path = ref.resolve_path(scope_dir)

            self.assertTrue(path.is_relative_to(scope_dir))
            self.assertTrue((scope_dir / "relative" / "path").exists())

    def test_resolve_path_with_dot_in_folder(self):
        """resolve_path should handle dots in folder names."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            ref = SimpleArtifactRef(
                key_name="key", folder="folder.with.dots", filename="file.txt"
            )

            path = ref.resolve_path(scope_dir)

            expected_dir = scope_dir / "folder.with.dots"
            self.assertTrue(expected_dir.exists())
            self.assertEqual(path, expected_dir / "file.txt")

    def test_key_name_with_special_chars(self):
        """SimpleArtifactRef should accept key names with various characters."""
        ref = SimpleArtifactRef(
            key_name="key-with_special.chars", folder="folder", filename="file.txt"
        )

        self.assertEqual(ref.key(), "key-with_special.chars")

    def test_folder_with_backslashes(self):
        """resolve_path should handle backslashes in folder (Windows-style)."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            # Path will normalize the backslashes appropriately
            ref = SimpleArtifactRef(
                key_name="key", folder="folder\\subfolder", filename="file.txt"
            )

            path = ref.resolve_path(scope_dir)

            # Path should be created regardless of path separator
            self.assertTrue(path.parent.exists())

    def test_empty_folder_string(self):
        """resolve_path should handle empty folder string."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)
            ref = SimpleArtifactRef(key_name="key", folder="", filename="file.txt")

            path = ref.resolve_path(scope_dir)

            # Should resolve to scope_dir / "" / filename = scope_dir / filename
            self.assertEqual(path, scope_dir / "file.txt")


class TestEnsureArtifactRef(unittest.TestCase):
    """Test ensure_artifact_ref function for protocol validation."""

    def test_accepts_simple_artifact_ref(self):
        """ensure_artifact_ref should accept SimpleArtifactRef."""
        ref = SimpleArtifactRef(key_name="key", folder="folder", filename="file.txt")

        result = ensure_artifact_ref(ref)

        self.assertEqual(result, ref)

    def test_returns_same_instance(self):
        """ensure_artifact_ref should return the same instance (identity)."""
        ref = SimpleArtifactRef(key_name="key", folder="folder", filename="file.txt")

        result = ensure_artifact_ref(ref)

        self.assertIs(result, ref)

    def test_accepts_custom_class_with_protocol(self):
        """ensure_artifact_ref should accept custom class implementing protocol."""

        class CustomArtifactRef:
            def key(self) -> str:
                return "custom_key"

            def resolve_path(self, scope_dir: Path) -> Path:
                return scope_dir / "custom_path"

        custom_ref = CustomArtifactRef()
        result = ensure_artifact_ref(custom_ref)

        self.assertIsInstance(result, CustomArtifactRef)
        self.assertEqual(result.key(), "custom_key")

    def test_rejects_object_without_key_method(self):
        """ensure_artifact_ref should reject objects without key() method."""

        class BadRef:
            def resolve_path(self, scope_dir: Path) -> Path:
                return scope_dir / "path"

        bad_ref = BadRef()

        with self.assertRaises(TypeError) as cm:
            ensure_artifact_ref(bad_ref)

        self.assertIn("key", str(cm.exception))

    def test_rejects_object_without_resolve_path_method(self):
        """ensure_artifact_ref should reject objects without resolve_path() method."""

        class BadRef:
            def key(self) -> str:
                return "key"

        bad_ref = BadRef()

        with self.assertRaises(TypeError) as cm:
            ensure_artifact_ref(bad_ref)

        self.assertIn("resolve_path", str(cm.exception))

    def test_rejects_object_without_both_methods(self):
        """ensure_artifact_ref should reject objects without both methods."""

        class BadRef:
            pass

        bad_ref = BadRef()

        with self.assertRaises(TypeError) as cm:
            ensure_artifact_ref(bad_ref)

        self.assertIn("key", str(cm.exception))
        self.assertIn("resolve_path", str(cm.exception))

    def test_rejects_none(self):
        """ensure_artifact_ref should reject None."""
        with self.assertRaises(TypeError):
            ensure_artifact_ref(None)

    def test_rejects_string(self):
        """ensure_artifact_ref should reject plain strings."""
        with self.assertRaises(TypeError):
            ensure_artifact_ref("not_a_ref")

    def test_rejects_dict(self):
        """ensure_artifact_ref should reject dicts."""
        with self.assertRaises(TypeError):
            ensure_artifact_ref({"key": "value"})

    def test_rejects_path(self):
        """ensure_artifact_ref should reject Path objects."""
        with self.assertRaises(TypeError):
            ensure_artifact_ref(Path("/some/path"))

    def test_error_message_descriptive(self):
        """ensure_artifact_ref error message should be descriptive."""

        class BadRef:
            pass

        with self.assertRaises(TypeError) as cm:
            ensure_artifact_ref(BadRef())

        error_msg = str(cm.exception)
        self.assertIn("artifact ref", error_msg.lower())
        self.assertIn("key", error_msg)
        self.assertIn("resolve_path", error_msg)

    def test_accepts_object_with_callable_key(self):
        """ensure_artifact_ref should accept objects where key is callable."""

        class RefWithCallableKey:
            key = lambda self: "callable_key"

            def resolve_path(self, scope_dir: Path) -> Path:
                return scope_dir / "path"

        ref = RefWithCallableKey()
        result = ensure_artifact_ref(ref)

        self.assertEqual(result.key(), "callable_key")

    def test_accepts_object_with_method_attributes(self):
        """ensure_artifact_ref checks hasattr, which works with methods and properties."""

        class RefWithMethods:
            def key(self) -> str:
                return "method_key"

            def resolve_path(self, scope_dir: Path) -> Path:
                return scope_dir / "path"

        ref = RefWithMethods()

        # This should work because both methods exist
        result = ensure_artifact_ref(ref)
        self.assertEqual(result.key(), "method_key")


class TestArtifactRefProtocol(unittest.TestCase):
    """Test that ArtifactRefProtocol is defined correctly."""

    def test_protocol_has_key_method(self):
        """ArtifactRefProtocol should require key() method."""
        # This is more of a static type check, but we can verify the protocol exists
        self.assertTrue(hasattr(ArtifactRefProtocol, "key"))

    def test_protocol_has_resolve_path_method(self):
        """ArtifactRefProtocol should require resolve_path() method."""
        self.assertTrue(hasattr(ArtifactRefProtocol, "resolve_path"))

    def test_simple_artifact_ref_satisfies_protocol(self):
        """SimpleArtifactRef should satisfy ArtifactRefProtocol."""
        ref = SimpleArtifactRef(key_name="key", folder="folder")

        # Should have both required methods
        self.assertTrue(hasattr(ref, "key"))
        self.assertTrue(callable(ref.key))
        self.assertTrue(hasattr(ref, "resolve_path"))
        self.assertTrue(callable(ref.resolve_path))


class TestIntegrationScenarios(unittest.TestCase):
    """Integration tests for common artifact reference usage patterns."""

    def test_multiple_refs_same_folder(self):
        """Multiple refs can point to different files in same folder."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)

            ref1 = SimpleArtifactRef(
                key_name="file1", folder="shared", filename="a.txt"
            )
            ref2 = SimpleArtifactRef(
                key_name="file2", folder="shared", filename="b.txt"
            )

            path1 = ref1.resolve_path(scope_dir)
            path2 = ref2.resolve_path(scope_dir)

            self.assertEqual(path1.parent, path2.parent)
            self.assertNotEqual(path1, path2)
            self.assertTrue(path1.parent.exists())

    def test_ref_chain_different_levels(self):
        """Refs can create a hierarchy of directories."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)

            refs = [
                SimpleArtifactRef(
                    key_name="top", folder="level1", filename="file1.txt"
                ),
                SimpleArtifactRef(
                    key_name="mid", folder="level1/level2", filename="file2.txt"
                ),
                SimpleArtifactRef(
                    key_name="deep",
                    folder="level1/level2/level3",
                    filename="file3.txt",
                ),
            ]

            paths = [ref.resolve_path(scope_dir) for ref in refs]

            # All directories should exist
            for path in paths:
                self.assertTrue(path.parent.exists())

            # Verify hierarchy
            self.assertTrue(paths[1].is_relative_to(paths[0].parent))
            self.assertTrue(paths[2].is_relative_to(paths[1].parent))

    def test_directory_and_file_refs_coexist(self):
        """Directory refs and file refs can coexist in same folder."""
        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)

            dir_ref = SimpleArtifactRef(key_name="dir", folder="workspace")
            file_ref = SimpleArtifactRef(
                key_name="file", folder="workspace", filename="data.txt"
            )

            dir_path = dir_ref.resolve_path(scope_dir)
            file_path = file_ref.resolve_path(scope_dir)

            self.assertEqual(dir_path, file_path.parent)
            self.assertTrue(dir_path.exists())

    def test_ensure_artifact_ref_in_function(self):
        """ensure_artifact_ref can be used as a type guard in functions."""

        def process_artifact(ref: object) -> str:
            validated_ref = ensure_artifact_ref(ref)
            return validated_ref.key()

        ref = SimpleArtifactRef(key_name="test", folder="folder")
        result = process_artifact(ref)

        self.assertEqual(result, "test")

    def test_custom_implementation_interoperability(self):
        """Custom artifact ref implementation should work with SimpleArtifactRef."""

        class MockArtifactRef:
            def key(self) -> str:
                return "mock_key"

            def resolve_path(self, scope_dir: Path) -> Path:
                return scope_dir / "mock_folder" / "mock_file.txt"

        with TemporaryDirectory() as tmpdir:
            scope_dir = Path(tmpdir)

            simple_ref = SimpleArtifactRef(
                key_name="simple", folder="simple_folder", filename="simple.txt"
            )
            mock_ref = MockArtifactRef()

            # Both should work with ensure_artifact_ref
            validated_simple = ensure_artifact_ref(simple_ref)
            validated_mock = ensure_artifact_ref(mock_ref)

            # Both should be able to resolve paths
            simple_path = validated_simple.resolve_path(scope_dir)
            mock_path = validated_mock.resolve_path(scope_dir)

            self.assertIsInstance(simple_path, Path)
            self.assertIsInstance(mock_path, Path)


if __name__ == "__main__":
    unittest.main()
