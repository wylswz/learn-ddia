package com.xmbsmdsj.ddia;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

public class TestSSTable {

    @TempDir
    Path tempDir;

    private Path getTestDataPath() {
        return tempDir.resolve("sstable-data");
    }

    @BeforeEach
    public void setUp() throws IOException {
        Files.createDirectories(getTestDataPath());
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Clean up test files
        if (Files.exists(getTestDataPath())) {
            Files.walk(getTestDataPath())
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        }
    }

    /**
     * Helper method that puts all entries from the map into the sstable and then
     * asserts all values match.
     */
    private void putAndAssertAll(SSTable sstable, HashMap<String, String> expected) throws IOException {
        for (var entry : expected.entrySet()) {
            sstable.put(entry.getKey(), entry.getValue());
        }
        for (var entry : expected.entrySet()) {
            assertEquals(entry.getValue(), sstable.get(entry.getKey()),
                    "Failed for key: " + entry.getKey());
        }
    }

    /**
     * Helper method that puts all entries from the map into the sstable and then
     * asserts all values match.
     * Also asserts that the given non-existent keys return null.
     */
    private void putAndAssertAll(SSTable sstable, HashMap<String, String> expected, String... nonExistentKeys)
            throws IOException {
        putAndAssertAll(sstable, expected);
        for (var key : nonExistentKeys) {
            assertNull(sstable.get(key), "Expected null for non-existent key: " + key);
        }
    }

    @Test
    public void testEmptySSTable() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        assertNull(sstable.get("nonexistent"));
    }

    @Test
    public void testSinglePutAndGet() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("key1", "value1");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testMultiplePutsAndGets() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("key1", "value1");
        expected.put("key2", "value2");
        expected.put("key3", "value3");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testOverwriteExistingKey() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("key1", "value1");
        expected.put("key1", "value1_updated");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testNonExistentKey() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("key1", "value1");

        putAndAssertAll(sstable, expected, "nonexistent", "", "key2");
    }

    @Test
    public void testEmptyKeyAndValue() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("", "empty_key_value");
        expected.put("empty_value_key", "");
        expected.put("", "");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testSpecialCharacters() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("key with spaces", "value with spaces");
        expected.put("key\nwith\nnewlines", "value\nwith\nnewlines");
        expected.put("key\twith\ttabs", "value\twith\ttabs");
        expected.put("key\"with\"quotes", "value\"with\"quotes");
        expected.put("key'with'quotes", "value'with'quotes");
        expected.put("key\\with\\backslashes", "value\\with\\backslashes");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testUnicodeCharacters() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("中文键", "中文值");
        expected.put("🔑", "💎");
        expected.put("ключ", "значение");
        expected.put("キー", "値");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testLargeValues() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("large_key", "x".repeat(10000));

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testLargeKeys() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("k".repeat(1000), "value");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testSegmentFlushing() throws IOException {
        var sstable = new SSTable(getTestDataPath(), 5);
        var expected = new HashMap<String, String>();
        for (int i = 0; i < 15; i++) {
            expected.put("key" + i, "value" + i);
        }
        putAndAssertAll(sstable, expected);
    }

    @Test
    public void debug() throws IOException {
        var sstable = new SSTable(getTestDataPath(), 5);
        var expected = new HashMap<String, String>();
        for (int i = 0; i < 15; i++) {
            expected.put("key" + i, "value" + i);
        }
        // Put data to trigger segment flushing
        for (var entry : expected.entrySet()) {
            sstable.put(entry.getKey(), entry.getValue());
        }
        System.out.println("=== Debugging On-Disk Segments ===");
        for (var s : sstable.listOnDiskSegments()) {
            s.debug();
            System.out.println("---");
        }
    }

    @Test
    public void testMultipleSegments() throws IOException {
        var sstable = new SSTable(getTestDataPath(), 3);
        var expected = new HashMap<String, String>();
        for (int i = 0; i < 10; i++) {
            expected.put("key" + i, "value" + i);
        }

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testOverwriteAcrossSegments() throws IOException {
        var sstable = new SSTable(getTestDataPath(), 3);
        var expected = new HashMap<String, String>();
        expected.put("key1", "value1_old");
        expected.put("key2", "value2");
        expected.put("key3", "value3");
        expected.put("key4", "value4");
        expected.put("key1", "value1_new");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testKeysAtBoundaries() throws IOException {
        var sstable = new SSTable(getTestDataPath(), 5);
        var expected = new HashMap<String, String>();
        expected.put("a", "value_a");
        expected.put("b", "value_b");
        expected.put("c", "value_c");
        expected.put("d", "value_d");
        expected.put("e", "value_e");
        expected.put("f", "value_f");
        expected.put("z", "value_z");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testSortedOrder() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("zebra", "value_zebra");
        expected.put("apple", "value_apple");
        expected.put("banana", "value_banana");
        expected.put("dog", "value_dog");
        expected.put("cat", "value_cat");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testNumericKeys() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        for (int i = 0; i < 100; i++) {
            expected.put(String.valueOf(i), "value_" + i);
        }

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testVeryLongKeyValuePairs() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("k".repeat(5000), "v".repeat(5000));

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testReopenSSTable() throws IOException {
        var dataPath = getTestDataPath();
        var expected = new HashMap<String, String>();
        for (int i = 0; i < 10; i++) {
            expected.put("key" + i, "value" + i);
        }

        var sstable1 = new SSTable(dataPath, 5);
        putAndAssertAll(sstable1, expected);

        var sstable2 = new SSTable(dataPath, 5);
        for (var entry : expected.entrySet()) {
            assertEquals(entry.getValue(), sstable2.get(entry.getKey()),
                    "Failed after reopen for key: " + entry.getKey());
        }
    }

    @Test
    public void testManyEntries() throws IOException {
        var sstable = new SSTable(getTestDataPath(), 10);
        var expected = new HashMap<String, String>();
        for (int i = 0; i < 100; i++) {
            expected.put("key" + String.format("%03d", i), "value" + i);
        }

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testKeysWithSamePrefix() throws IOException {
        var sstable = new SSTable(getTestDataPath());
        var expected = new HashMap<String, String>();
        expected.put("prefix_key1", "value1");
        expected.put("prefix_key2", "value2");
        expected.put("prefix_key3", "value3");
        expected.put("prefix", "value_prefix");
        expected.put("prefix_longer", "value_longer");

        putAndAssertAll(sstable, expected);
    }

    @Test
    public void testExactSegmentSizeLimit() throws IOException {
        var sstable = new SSTable(getTestDataPath(), 5);
        var expected = new HashMap<String, String>();
        for (int i = 0; i < 5; i++) {
            expected.put("key" + i, "value" + i);
        }
        expected.put("key5", "value5");

        putAndAssertAll(sstable, expected);
    }
}
