package com.xmbsmdsj.ddia;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.Comparator;
import java.util.HashSet;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Iterator;

public class SSTable {

    private static int strLen(String str) {
        var bytes = str.getBytes(StandardCharsets.UTF_8);
        return bytes.length;
    }
    private static final int SIZE_LEN = 4;
    private static final int OFFSET_LEN = 8;


    private volatile List<OnDiskSegment> segments;
    private InMemorySegment head;
    private int segmentSizeLimit = 1024;
    /**
     * datapath/
     *   seg-1.sst
     *   seg-2.sst
     *   ...
     * 
     * seg file is named as seg-<idx>.sst
     */
    private Path dataPath;
    private final Lock diskLock = new ReentrantLock();

    public SSTable(Path dataPath) throws IOException {
        this.dataPath = dataPath;
        this.segments = new ArrayList<>();
        this.head = new InMemorySegment();
        this.loadSegments();
    }

    public SSTable(Path dataPath, int segmentSizeLimit) throws IOException {
        this(dataPath);
        this.segmentSizeLimit = segmentSizeLimit;
    }

    private void loadSegments() throws IOException {
        var files = Stream.of(this.dataPath.toFile().listFiles())
        .filter(file -> file.getName().startsWith("seg-") && file.getName().endsWith(".sst"))
        .sorted(Comparator.comparingLong(file -> {
            var name = file.getName();
            var numberPart = name.substring(4, name.length() - 4); // Remove "seg-" prefix and ".sst" suffix
            return Long.parseLong(numberPart);
        }))
        .toList();

        for (var file : files) {
            this.segments.add(OnDiskSegment.fromPath(file.toPath()));
        }
    }

    private int nextSegIdx() {
        return this.segments.size() + 1;
    }

    synchronized public void put(String key, String value) throws IOException {
        this.head.put(key, value);
        if (this.head.size() >= this.segmentSizeLimit) {
            var onDiskSegment = this.head.dump(this.dataPath.resolve(String.format("seg-%d.sst", nextSegIdx())));
            this.segments.add(onDiskSegment);
            this.head = new InMemorySegment();
        }
    }

    synchronized public String get(String key) throws IOException {
        var inMemValue = this.head.get(key);
        if (inMemValue != null) {
            return inMemValue;
        }
        for (var segment : this.segments.reversed()) {
            var value = segment.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    List<OnDiskSegment> listOnDiskSegments() {
        return this.segments;
    }




    /**
     * on disk layout:
     * 
     * section 1 - sparse index
     * | index size: int32| # number of entries in the index
     * | keysize: int32 | key | offset: int64 |
     * | keysize: int32 | key | offset: int64 |
     * ...
     * section 2 - data
     * data entry format:
     * | data entries: int32| # number of data entries
     * | keysize: int32 | valuesize: int32 | key | value |
     * | keysize: int32 | valuesize: int32 | key | value |
     * ...
     * 
     * keys are sorted with each on-disk segment
     */
    static abstract class Segment {
        abstract String get(String key) throws IOException;
    }

    static class InMemorySegment extends Segment {
        private static final int SPARSE_INDEX_SAMPLE_FACTOR = 10;

        private TreeMap<String, String> data;

        InMemorySegment() {
            this.data = new TreeMap<>();
        }

        int size() {
            return this.data.size();
        }

        void put(String key, String value) {
            this.data.put(key, value);
        }

        @Override
        String get(String key) throws IOException {
            return this.data.get(key);
        }

        public OnDiskSegment dump(Path path) throws IOException {
            TreeMap<String, Long> sparseIndex = new TreeMap<>();
            Map<String, Long> sparseKeys = new TreeMap<>();
            int i = 0;
            long offset = 0;
            for (var entry : this.data.entrySet()) {
                var key = entry.getKey();
                var value = entry.getValue();
                if (i % SPARSE_INDEX_SAMPLE_FACTOR == 0) {
                    sparseKeys.put(key, offset);
                }

                offset += strLen(key) + strLen(value) + SIZE_LEN * 2;

                i++;
            }
            int indexSectionOffset = SIZE_LEN;
            for (var entry : sparseKeys.entrySet()) {
                indexSectionOffset += SIZE_LEN + strLen(entry.getKey()) + OFFSET_LEN;
            }

            for (var entry : sparseKeys.entrySet()) {
                sparseIndex.put(entry.getKey(), entry.getValue() + indexSectionOffset + SIZE_LEN);
            }

            try (var raf = new RandomAccessFile(path.toFile(), "rw")) {
                raf.writeInt(sparseIndex.size());
                for (var entry : sparseIndex.entrySet()) {
                    new IndexRecord(entry.getKey(), entry.getValue()).writeTo(raf);
                }
                raf.writeInt(this.data.size());
                for (var entry : this.data.entrySet()) {
                    new DataRecord(entry.getKey(), entry.getValue()).writeTo(raf);
                }
            }
            return OnDiskSegment.fromPath(path);
        }

    }

    static record IndexRecord(String key, long offset) {
        void writeTo(RandomAccessFile raf) throws IOException {
            raf.writeInt(strLen(key));
            raf.write(key.getBytes(StandardCharsets.UTF_8));
            raf.writeLong(offset);
        }
    }

    static record DataRecord(String key, String value) {
        void writeTo(RandomAccessFile raf) throws IOException {
            raf.writeInt(strLen(key));
            raf.writeInt(strLen(value));
            raf.write(key.getBytes(StandardCharsets.UTF_8));
            raf.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    static class OnDiskSegment extends Segment {
        private TreeMap<String, Long> sparseIndex;
        private Path path;
        private long dataSize;
        private long dataSectionOffset;

        static class OnDiskSegmentIterator implements Iterator<OnDiskSegment.Pair> {
            long offset;
            OnDiskSegment segment;
            RandomAccessFile raf;
    
            OnDiskSegmentIterator(OnDiskSegment segment) throws IOException {
                this.segment = segment;
                this.raf = new RandomAccessFile(segment.path.toFile(), "r");
                this.offset = segment.dataEntryOffset();
                this.raf.seek(this.offset);
            }
    
            @Override
            public boolean hasNext() {
                return this.segment.dataSize > this.offset;
            }
    
            @Override
            public OnDiskSegment.Pair next() {
                try {
                    var ret = this.segment.nextRecord(this.raf);
                    this.offset += ret.offset();
                    return ret;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        static class Pair {
            String key;
            String value;
            long offset;

            Pair(String key, String value) {
                this.key = key;
                this.value = value;
                this.offset = SIZE_LEN * 2 + strLen(key) + strLen(value);
            }

            String key() {
                return this.key;
            }

            String value() {
                return this.value;
            }

            long offset() {
                return this.offset;
            }
        }
        
        static OnDiskSegment fromPath(Path path) throws IOException {
            var ret = new OnDiskSegment();
            ret.path = path;
            ret.dataSize = Files.size(path);
            ret.sparseIndex = ret.loadSparseIndex();
            return ret;
        }

        private TreeMap<String, Long> loadSparseIndex() throws IOException {
            var ret =  new TreeMap<String, Long>();
            try (var raf = new RandomAccessFile(path.toFile(), "r")) {
                var indexSize = raf.readInt();
                for (int i = 0; i < indexSize; i++) {
                    var keySize = raf.readInt();
                    byte[] key = new byte[keySize];
                    raf.readFully(key);
                    var offset = raf.readLong();
                    ret.put(new String(key), offset);
                }
                // Store the file pointer position, which is the start of the data section
                this.dataSectionOffset = raf.getFilePointer();
            }
            return ret;
        }


        void debug() throws IOException {
            try (var raf = new RandomAccessFile(path.toFile(), "r")) {
                System.out.println("File size: " + this.dataSize);
                System.out.println("Data section offset: " + this.dataSectionOffset);
                System.out.println("indexSize: " + raf.readInt());
                for (var entry : sparseIndex.entrySet()) {
                    System.out.println("key: " + entry.getKey() + " offset: " + entry.getValue());
                }
                System.out.println("Seeking to: " + indexOffset());
                raf.seek(indexOffset());
                System.out.println("File pointer after seek: " + raf.getFilePointer());
                // Read the 4 bytes as raw bytes to see what we're actually reading
                byte[] dataSizeBytes = new byte[4];
                raf.readFully(dataSizeBytes);
                System.out.println("Data size bytes (hex): " + String.format("%02x %02x %02x %02x", 
                    dataSizeBytes[0] & 0xFF, dataSizeBytes[1] & 0xFF, dataSizeBytes[2] & 0xFF, dataSizeBytes[3] & 0xFF));
                raf.seek(indexOffset()); // Seek back
                var dataSize = raf.readInt();
                System.out.println("dataSize (as int): " + dataSize);
                if (dataSize > 0 && dataSize < 1000) { // Only try to read if dataSize looks reasonable
                    for (int i = 0; i < dataSize && raf.getFilePointer() < this.dataSize; i++) {
                        var record = nextRecord(raf);
                        System.out.println("key: " + record.key() + " value: " + record.value());
                    }
                }
            }
        }

        private String lookUpOnDisk(String key, long fromOffset, long toOffset) throws IOException {
            try (var raf = new RandomAccessFile(path.toFile(), "r")) {
                raf.seek(fromOffset);
                // Read until we've passed toOffset (which means we've read the record at toOffset)
                while (raf.getFilePointer() <= toOffset && raf.getFilePointer() < this.dataSize) {
                    var record = nextRecord(raf);
                    if (record.key().compareTo(key) == 0) {
                        return record.value();
                    }
                    // If we've passed the key (since data is sorted), we can stop
                    if (record.key().compareTo(key) > 0) {
                        return null;
                    }
                }
                return null;
            }
        }

        private Pair nextRecord(RandomAccessFile raf) throws IOException {
            var keySize = raf.readInt();
            var valueSize = raf.readInt();
            byte[] key = new byte[(int) keySize];
            raf.readFully(key);
            byte[] value = new byte[(int) valueSize];
            raf.readFully(value);
            return new Pair(new String(key), new String(value));
        }

        private String directLookUp(long offset) throws IOException {
            try (var raf = new RandomAccessFile(path.toFile(), "r")) {
                raf.seek(offset);
                var record = nextRecord(raf);
                return record.value();
            }
        }

        private long indexOffset() {
            return this.dataSectionOffset;
        }

        private long dataEntryOffset() {
            return indexOffset() + SIZE_LEN;
        }

        @Override
        String get(String key) throws IOException {
            var offset = sparseIndex.get(key);
            if (offset != null) {
                return directLookUp(offset);
            }
            var ceilingEntry = sparseIndex.ceilingEntry(key);
            var floorEntry = sparseIndex.floorEntry(key);

            if (ceilingEntry == null && floorEntry == null) {
                return null;
            }

            long fromOffset = indexOffset() + SIZE_LEN; // dataSectionOffset + data size field
            long toOffset = this.dataSize;
            if (floorEntry != null) {
                fromOffset = floorEntry.getValue();
            }
            if (ceilingEntry != null) {
                toOffset = ceilingEntry.getValue();
            }
            return lookUpOnDisk(key, fromOffset, toOffset);
        }

        OnDiskSegmentIterator iterator() throws IOException {
            return new OnDiskSegmentIterator(this);
        }
    }

    

    /**
     * merge on disk segments
     */
    synchronized void merge() throws IOException {
        var iterators = new ArrayList<OnDiskSegment.OnDiskSegmentIterator>();
        for (var seg : this.segments.reversed()) {
            iterators.add(seg.iterator());
        }
        Set<String> closeSet = new HashSet<>();
        var newSeg = new InMemorySegment();
        for (var iter : iterators) {
            while (iter.hasNext()) {
                var pair = iter.next();
                if (closeSet.contains(pair.key())) {
                    continue;
                }
                newSeg.put(pair.key(), pair.value());
            }
        }
        var newDiskSeg = newSeg.dump(dataPath.resolve("tmp.sst"));
        for (var seg : segments) {
            Files.delete(seg.path);
        }
        Files.move(newDiskSeg.path, dataPath.resolve("seg-1.sst"));
        this.segments.clear();
        this.segments.add(OnDiskSegment.fromPath(dataPath.resolve("seg-1.sst")));
    }
    
}
