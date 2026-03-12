import hashlib
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from yggdrasil.flow.utils.hash import dirhash_stats, sha256_file


class TestSha256File(unittest.TestCase):
    """
    Comprehensive tests for sha256_file function.

    Tests SHA-256 hash computation for files, including various file sizes,
    content types, and edge cases.
    """

    def setUp(self):
        """Set up temporary directory for test files."""
        self.temp_dir = TemporaryDirectory()
        self.test_dir = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    # =====================================================
    # BASIC FILE HASHING TESTS
    # =====================================================

    def test_sha256_empty_file(self):
        """Test hashing an empty file."""
        empty_file = self.test_dir / "empty.txt"
        empty_file.write_bytes(b"")

        result = sha256_file(empty_file)

        # SHA-256 of empty file is known value
        expected = hashlib.sha256(b"").hexdigest()
        self.assertEqual(result, expected)
        self.assertEqual(len(result), 64)  # SHA-256 is 64 hex chars

    def test_sha256_small_text_file(self):
        """Test hashing a small text file."""
        test_file = self.test_dir / "small.txt"
        content = b"Hello, World!"
        test_file.write_bytes(content)

        result = sha256_file(test_file)

        # Verify against known hash
        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    def test_sha256_binary_file(self):
        """Test hashing a binary file."""
        binary_file = self.test_dir / "binary.dat"
        content = bytes(range(256))
        binary_file.write_bytes(content)

        result = sha256_file(binary_file)

        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    def test_sha256_large_file(self):
        """Test hashing a file larger than chunk size (> 1MB)."""
        large_file = self.test_dir / "large.dat"
        # Create 2MB file
        content = b"x" * (2 * 1024 * 1024)
        large_file.write_bytes(content)

        result = sha256_file(large_file)

        # Verify matches standard hash
        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    def test_sha256_multiline_text(self):
        """Test hashing a multi-line text file."""
        text_file = self.test_dir / "multiline.txt"
        content = b"Line 1\nLine 2\nLine 3\n"
        text_file.write_bytes(content)

        result = sha256_file(text_file)

        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    # =====================================================
    # HASH FORMAT TESTS
    # =====================================================

    def test_sha256_returns_hex_string(self):
        """Test that result is a hexadecimal string."""
        test_file = self.test_dir / "test.txt"
        test_file.write_text("content", encoding="utf-8")

        result = sha256_file(test_file)

        # Should be hex string
        self.assertIsInstance(result, str)
        # Should only contain hex characters
        self.assertTrue(all(c in "0123456789abcdef" for c in result))

    def test_sha256_length_is_64(self):
        """Test that SHA-256 hash is always 64 characters."""
        test_file = self.test_dir / "test.txt"
        test_file.write_text("any content", encoding="utf-8")

        result = sha256_file(test_file)

        self.assertEqual(len(result), 64)

    def test_sha256_lowercase_hex(self):
        """Test that hash is returned in lowercase."""
        test_file = self.test_dir / "test.txt"
        test_file.write_text("content", encoding="utf-8")

        result = sha256_file(test_file)

        # Should be lowercase
        self.assertEqual(result, result.lower())

    # =====================================================
    # DETERMINISM TESTS
    # =====================================================

    def test_sha256_deterministic(self):
        """Test that hashing the same file twice gives same result."""
        test_file = self.test_dir / "test.txt"
        test_file.write_text("consistent content", encoding="utf-8")

        result1 = sha256_file(test_file)
        result2 = sha256_file(test_file)

        self.assertEqual(result1, result2)

    def test_sha256_different_content_different_hash(self):
        """Test that different content produces different hashes."""
        file1 = self.test_dir / "file1.txt"
        file2 = self.test_dir / "file2.txt"

        file1.write_text("content 1", encoding="utf-8")
        file2.write_text("content 2", encoding="utf-8")

        hash1 = sha256_file(file1)
        hash2 = sha256_file(file2)

        self.assertNotEqual(hash1, hash2)

    def test_sha256_identical_content_same_hash(self):
        """Test that identical content in different files produces same hash."""
        file1 = self.test_dir / "file1.txt"
        file2 = self.test_dir / "file2.txt"

        content = "identical content"
        file1.write_text(content, encoding="utf-8")
        file2.write_text(content, encoding="utf-8")

        hash1 = sha256_file(file1)
        hash2 = sha256_file(file2)

        self.assertEqual(hash1, hash2)

    # =====================================================
    # SPECIAL CONTENT TESTS
    # =====================================================

    def test_sha256_unicode_content(self):
        """Test hashing file with Unicode content."""
        unicode_file = self.test_dir / "unicode.txt"
        content = "Hello 世界 🌍 Ñoño"
        unicode_file.write_text(content, encoding="utf-8")

        result = sha256_file(unicode_file)

        # Should successfully hash UTF-8 encoded bytes
        expected = hashlib.sha256(content.encode("utf-8")).hexdigest()
        self.assertEqual(result, expected)

    def test_sha256_null_bytes(self):
        """Test hashing file containing null bytes."""
        null_file = self.test_dir / "null_bytes.dat"
        content = b"before\x00after\x00\x00end"
        null_file.write_bytes(content)

        result = sha256_file(null_file)

        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    def test_sha256_newline_variations(self):
        """Test that different newline styles affect hash."""
        unix_file = self.test_dir / "unix.txt"
        windows_file = self.test_dir / "windows.txt"

        unix_file.write_bytes(b"line1\nline2\n")
        windows_file.write_bytes(b"line1\r\nline2\r\n")

        hash_unix = sha256_file(unix_file)
        hash_windows = sha256_file(windows_file)

        # Different byte content should produce different hashes
        self.assertNotEqual(hash_unix, hash_windows)

    # =====================================================
    # CHUNKED READING TESTS
    # =====================================================

    def test_sha256_exactly_one_chunk(self):
        """Test file exactly 1MB (one chunk size)."""
        chunk_file = self.test_dir / "1mb.dat"
        content = b"x" * (1024 * 1024)
        chunk_file.write_bytes(content)

        result = sha256_file(chunk_file)

        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    def test_sha256_slightly_over_one_chunk(self):
        """Test file slightly larger than 1MB."""
        file = self.test_dir / "over1mb.dat"
        content = b"x" * (1024 * 1024 + 100)
        file.write_bytes(content)

        result = sha256_file(file)

        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    def test_sha256_multiple_chunks(self):
        """Test file requiring multiple chunks (5MB)."""
        large_file = self.test_dir / "5mb.dat"
        # Use different pattern per MB to ensure proper chunking
        content = b"".join(bytes([i % 256]) * (1024 * 1024) for i in range(5))
        large_file.write_bytes(content)

        result = sha256_file(large_file)

        expected = hashlib.sha256(content).hexdigest()
        self.assertEqual(result, expected)

    # =====================================================
    # PATH HANDLING TESTS
    # =====================================================

    def test_sha256_with_path_object(self):
        """Test that function accepts Path objects."""
        test_file = self.test_dir / "path_obj.txt"
        test_file.write_text("content", encoding="utf-8")

        # Should work with Path object
        result = sha256_file(test_file)

        self.assertIsInstance(result, str)
        self.assertEqual(len(result), 64)

    def test_sha256_nested_directory_path(self):
        """Test hashing file in nested directories."""
        nested_dir = self.test_dir / "level1" / "level2" / "level3"
        nested_dir.mkdir(parents=True)

        nested_file = nested_dir / "nested.txt"
        nested_file.write_text("nested content", encoding="utf-8")

        result = sha256_file(nested_file)

        self.assertEqual(len(result), 64)


class TestDirhashStats(unittest.TestCase):
    """
    Comprehensive tests for dirhash_stats function.

    Tests directory hashing based on file statistics (path, size, mtime),
    including various directory structures and edge cases.
    """

    def setUp(self):
        """Set up temporary directory for tests."""
        self.temp_dir = TemporaryDirectory()
        self.test_dir = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up temporary directory."""
        self.temp_dir.cleanup()

    # =====================================================
    # BASIC DIRECTORY HASHING TESTS
    # =====================================================

    def test_dirhash_empty_directory(self):
        """Test hashing an empty directory."""
        empty_dir = self.test_dir / "empty"
        empty_dir.mkdir()

        result = dirhash_stats(empty_dir)

        # Should return hash with 'dirhash:' prefix
        self.assertTrue(result.startswith("dirhash:"))
        # Hash part should be 64 chars
        hash_part = result.split(":", 1)[1]
        self.assertEqual(len(hash_part), 64)

    def test_dirhash_single_file(self):
        """Test hashing directory with single file."""
        test_dir = self.test_dir / "single"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content", encoding="utf-8")

        result = dirhash_stats(test_dir)

        self.assertTrue(result.startswith("dirhash:"))
        self.assertEqual(len(result), len("dirhash:") + 64)

    def test_dirhash_multiple_files(self):
        """Test hashing directory with multiple files."""
        test_dir = self.test_dir / "multiple"
        test_dir.mkdir()

        (test_dir / "file1.txt").write_text("content1", encoding="utf-8")
        (test_dir / "file2.txt").write_text("content2", encoding="utf-8")
        (test_dir / "file3.txt").write_text("content3", encoding="utf-8")

        result = dirhash_stats(test_dir)

        self.assertTrue(result.startswith("dirhash:"))

    def test_dirhash_nested_directories(self):
        """Test hashing directory with nested subdirectories."""
        test_dir = self.test_dir / "nested"
        test_dir.mkdir()

        (test_dir / "file1.txt").write_text("root", encoding="utf-8")

        sub1 = test_dir / "sub1"
        sub1.mkdir()
        (sub1 / "file2.txt").write_text("sub1", encoding="utf-8")

        sub2 = sub1 / "sub2"
        sub2.mkdir()
        (sub2 / "file3.txt").write_text("sub2", encoding="utf-8")

        result = dirhash_stats(test_dir)

        self.assertTrue(result.startswith("dirhash:"))

    # =====================================================
    # HASH FORMAT TESTS
    # =====================================================

    def test_dirhash_prefix_format(self):
        """Test that result has correct 'dirhash:' prefix."""
        test_dir = self.test_dir / "format"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content", encoding="utf-8")

        result = dirhash_stats(test_dir)

        # Must start with 'dirhash:'
        self.assertTrue(result.startswith("dirhash:"))
        # Should be exactly 'dirhash:' + 64 hex chars
        self.assertEqual(len(result), len("dirhash:") + 64)

    def test_dirhash_hex_string(self):
        """Test that hash part is hexadecimal."""
        test_dir = self.test_dir / "hex"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content", encoding="utf-8")

        result = dirhash_stats(test_dir)
        hash_part = result.split(":", 1)[1]

        # Should only contain hex characters
        self.assertTrue(all(c in "0123456789abcdef" for c in hash_part))

    def test_dirhash_lowercase(self):
        """Test that hash is lowercase."""
        test_dir = self.test_dir / "lowercase"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content", encoding="utf-8")

        result = dirhash_stats(test_dir)
        hash_part = result.split(":", 1)[1]

        self.assertEqual(hash_part, hash_part.lower())

    # =====================================================
    # DETERMINISM TESTS
    # =====================================================

    def test_dirhash_deterministic(self):
        """Test that hashing same directory twice gives same result."""
        test_dir = self.test_dir / "deterministic"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content", encoding="utf-8")

        result1 = dirhash_stats(test_dir)
        result2 = dirhash_stats(test_dir)

        self.assertEqual(result1, result2)

    def test_dirhash_file_order_independent(self):
        """Test that hash is based on sorted file order."""
        # Create two directories with same files in different creation order
        dir1 = self.test_dir / "dir1"
        dir2 = self.test_dir / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        # Create files in different order
        (dir1 / "a.txt").write_text("a", encoding="utf-8")
        (dir1 / "b.txt").write_text("b", encoding="utf-8")
        (dir1 / "c.txt").write_text("c", encoding="utf-8")

        (dir2 / "c.txt").write_text("c", encoding="utf-8")
        (dir2 / "a.txt").write_text("a", encoding="utf-8")
        (dir2 / "b.txt").write_text("b", encoding="utf-8")

        # Hashes should be identical (sorted order)
        hash1 = dirhash_stats(dir1)
        hash2 = dirhash_stats(dir2)

        self.assertEqual(hash1, hash2)

    # =====================================================
    # CHANGE DETECTION TESTS
    # =====================================================

    def test_dirhash_changes_on_new_file(self):
        """Test that adding a file changes the hash."""
        test_dir = self.test_dir / "change_file"
        test_dir.mkdir()
        (test_dir / "file1.txt").write_text("content1", encoding="utf-8")

        hash1 = dirhash_stats(test_dir)

        # Add new file
        (test_dir / "file2.txt").write_text("content2", encoding="utf-8")

        hash2 = dirhash_stats(test_dir)

        self.assertNotEqual(hash1, hash2)

    def test_dirhash_changes_on_file_modification(self):
        """Test that modifying a file changes the hash."""
        test_dir = self.test_dir / "change_content"
        test_dir.mkdir()
        test_file = test_dir / "file.txt"
        test_file.write_text("original", encoding="utf-8")

        hash1 = dirhash_stats(test_dir)

        # Modify file content (changes size and mtime)
        import time

        time.sleep(0.01)  # Ensure mtime changes
        test_file.write_text("modified content", encoding="utf-8")

        hash2 = dirhash_stats(test_dir)

        self.assertNotEqual(hash1, hash2)

    def test_dirhash_changes_on_file_size_change(self):
        """Test that changing file size changes the hash."""
        test_dir = self.test_dir / "change_size"
        test_dir.mkdir()
        test_file = test_dir / "file.txt"
        test_file.write_text("small", encoding="utf-8")

        hash1 = dirhash_stats(test_dir)

        # Change size while keeping same mtime if possible
        test_file.write_text("much larger content", encoding="utf-8")

        hash2 = dirhash_stats(test_dir)

        self.assertNotEqual(hash1, hash2)

    def test_dirhash_changes_on_mtime_change(self):
        """Test that changing mtime changes the hash."""
        test_dir = self.test_dir / "change_mtime"
        test_dir.mkdir()
        test_file = test_dir / "file.txt"
        test_file.write_text("content", encoding="utf-8")

        hash1 = dirhash_stats(test_dir)

        # Change mtime by modifying the file
        import time

        time.sleep(0.1)  # Ensure at least 1 second difference for mtime
        # Explicitly set a different mtime
        stat = test_file.stat()
        new_mtime = stat.st_mtime + 2  # Add 2 seconds
        os.utime(test_file, (stat.st_atime, new_mtime))

        hash2 = dirhash_stats(test_dir)

        # Should change because mtime changed (int cast means >= 1 second difference)
        self.assertNotEqual(hash1, hash2)

    def test_dirhash_changes_on_file_removal(self):
        """Test that removing a file changes the hash."""
        test_dir = self.test_dir / "remove_file"
        test_dir.mkdir()
        file1 = test_dir / "file1.txt"
        file2 = test_dir / "file2.txt"
        file1.write_text("content1", encoding="utf-8")
        file2.write_text("content2", encoding="utf-8")

        hash1 = dirhash_stats(test_dir)

        # Remove one file
        file2.unlink()

        hash2 = dirhash_stats(test_dir)

        self.assertNotEqual(hash1, hash2)

    # =====================================================
    # PATH HANDLING TESTS
    # =====================================================

    def test_dirhash_relative_path_encoding(self):
        """Test that relative paths within directory are properly encoded."""
        test_dir = self.test_dir / "relative"
        test_dir.mkdir()

        subdir = test_dir / "subdir"
        subdir.mkdir()

        (test_dir / "root.txt").write_text("root", encoding="utf-8")
        (subdir / "sub.txt").write_text("sub", encoding="utf-8")

        result = dirhash_stats(test_dir)

        # Should successfully hash with relative paths
        self.assertTrue(result.startswith("dirhash:"))

    def test_dirhash_unicode_filenames(self):
        """Test hashing directory with Unicode filenames."""
        test_dir = self.test_dir / "unicode"
        test_dir.mkdir()

        # Create files with Unicode names
        (test_dir / "файл.txt").write_text("content", encoding="utf-8")
        (test_dir / "文件.txt").write_text("content", encoding="utf-8")
        (test_dir / "αρχείο.txt").write_text("content", encoding="utf-8")

        result = dirhash_stats(test_dir)

        self.assertTrue(result.startswith("dirhash:"))

    def test_dirhash_special_characters_in_names(self):
        """Test files with special characters in names."""
        test_dir = self.test_dir / "special"
        test_dir.mkdir()

        # Create files with various special characters
        (test_dir / "file with spaces.txt").write_text("content", encoding="utf-8")
        (test_dir / "file-with-dashes.txt").write_text("content", encoding="utf-8")
        (test_dir / "file_with_underscores.txt").write_text("content", encoding="utf-8")

        result = dirhash_stats(test_dir)

        self.assertTrue(result.startswith("dirhash:"))

    # =====================================================
    # EDGE CASES
    # =====================================================

    def test_dirhash_deep_nesting(self):
        """Test deeply nested directory structure."""
        current = self.test_dir / "deep"
        current.mkdir()

        # Create 10 levels deep
        for i in range(10):
            current = current / f"level{i}"
            current.mkdir()
            (current / f"file{i}.txt").write_text(f"level{i}", encoding="utf-8")

        result = dirhash_stats(self.test_dir / "deep")

        self.assertTrue(result.startswith("dirhash:"))

    def test_dirhash_many_files(self):
        """Test directory with many files."""
        test_dir = self.test_dir / "many"
        test_dir.mkdir()

        # Create 100 files
        for i in range(100):
            (test_dir / f"file{i:03d}.txt").write_text(f"content{i}", encoding="utf-8")

        result = dirhash_stats(test_dir)

        self.assertTrue(result.startswith("dirhash:"))

    def test_dirhash_empty_files(self):
        """Test directory with empty files."""
        test_dir = self.test_dir / "empty_files"
        test_dir.mkdir()

        for i in range(5):
            (test_dir / f"empty{i}.txt").write_bytes(b"")

        result = dirhash_stats(test_dir)

        self.assertTrue(result.startswith("dirhash:"))


class TestHashIntegration(unittest.TestCase):
    """
    Integration tests showing real-world usage patterns for hash utilities.
    """

    def setUp(self):
        """Set up temporary directory."""
        self.temp_dir = TemporaryDirectory()
        self.test_dir = Path(self.temp_dir.name)

    def tearDown(self):
        """Clean up."""
        self.temp_dir.cleanup()

    # =====================================================
    # CACHE VALIDATION TESTS
    # =====================================================

    def test_file_cache_validation(self):
        """Test using file hash for cache validation."""
        data_file = self.test_dir / "data.txt"
        data_file.write_text("original data", encoding="utf-8")

        # Compute initial hash
        original_hash = sha256_file(data_file)

        # Simulate cache hit
        cached_hash = original_hash
        self.assertEqual(sha256_file(data_file), cached_hash)  # Valid

        # Modify file
        data_file.write_text("modified data", encoding="utf-8")

        # Cache should be invalid
        self.assertNotEqual(sha256_file(data_file), cached_hash)

    def test_directory_cache_validation(self):
        """Test using directory hash for cache validation."""
        output_dir = self.test_dir / "outputs"
        output_dir.mkdir()
        (output_dir / "result.txt").write_text("result", encoding="utf-8")

        # Initial hash
        original_hash = dirhash_stats(output_dir)

        # No changes - cache valid
        self.assertEqual(dirhash_stats(output_dir), original_hash)

        # Add file - cache invalid
        (output_dir / "result2.txt").write_text("result2", encoding="utf-8")
        self.assertNotEqual(dirhash_stats(output_dir), original_hash)

    # =====================================================
    # ARTIFACT TRACKING TESTS
    # =====================================================

    def test_artifact_fingerprinting(self):
        """Test fingerprinting artifacts for workflow tracking."""
        artifact_file = self.test_dir / "artifact.dat"
        artifact_file.write_bytes(b"artifact data")

        # Create artifact record
        artifact = {
            "path": str(artifact_file),
            "digest": f"sha256:{sha256_file(artifact_file)}",
        }

        # Verify artifact hasn't changed
        current_hash = sha256_file(artifact_file)
        stored_hash = artifact["digest"].split(":", 1)[1]

        self.assertEqual(current_hash, stored_hash)

    def test_directory_artifact_fingerprinting(self):
        """Test fingerprinting directory artifacts."""
        output_dir = self.test_dir / "outputs"
        output_dir.mkdir()
        (output_dir / "file1.txt").write_text("data1", encoding="utf-8")
        (output_dir / "file2.txt").write_text("data2", encoding="utf-8")

        # Create artifact record
        artifact = {"path": str(output_dir), "digest": dirhash_stats(output_dir)}

        # Verify format
        self.assertTrue(artifact["digest"].startswith("dirhash:"))

        # Verify stability
        self.assertEqual(dirhash_stats(output_dir), artifact["digest"])

    # =====================================================
    # DEDUPLICATION TESTS
    # =====================================================

    def test_file_deduplication(self):
        """Test identifying duplicate files by hash."""
        file1 = self.test_dir / "file1.txt"
        file2 = self.test_dir / "file2.txt"
        file3 = self.test_dir / "file3.txt"

        # Files with same content
        file1.write_text("identical", encoding="utf-8")
        file2.write_text("identical", encoding="utf-8")
        # Different content
        file3.write_text("different", encoding="utf-8")

        hash1 = sha256_file(file1)
        hash2 = sha256_file(file2)
        hash3 = sha256_file(file3)

        # Duplicates should have same hash
        self.assertEqual(hash1, hash2)
        # Different should have different hash
        self.assertNotEqual(hash1, hash3)

    # =====================================================
    # COMPARISON TESTS
    # =====================================================

    def test_compare_directories(self):
        """Test comparing two directories by hash."""
        dir1 = self.test_dir / "dir1"
        dir2 = self.test_dir / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        # Create identical content
        (dir1 / "file.txt").write_text("content", encoding="utf-8")
        (dir2 / "file.txt").write_text("content", encoding="utf-8")

        hash1 = dirhash_stats(dir1)
        hash2 = dirhash_stats(dir2)

        # Should be equal
        self.assertEqual(hash1, hash2)

        # Modify one
        (dir2 / "extra.txt").write_text("extra", encoding="utf-8")

        # Should now differ
        self.assertNotEqual(dirhash_stats(dir1), dirhash_stats(dir2))


if __name__ == "__main__":
    unittest.main()
