/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.roaringbitmap;

import org.roaringbitmap.factory.KeyFactory;
import org.roaringbitmap.fs.SeekableInputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class RangeEncodeBitSliceBitmap<KEY> {

    public static final byte VERSION_1 = 1;
    public static final byte CURRENT_VERSION = VERSION_1;

    private final byte version;
    private final int rid;
    private final int cardinality;
    private final KEY min;
    private final KEY max;
    private final int blockOffset;
    private final int blockSerializedSizeInBytes;
    private final int blockSize;
    private final int entryOffset;
    private final int bitmapOffset;
    private final int nullBitmapSerializedSizeInBytes;
    private final int bsiSerializedInBytes;
    private final SeekableInputStream inputStream;
    private final Comparator<KEY> comparator;
    private final KeyFactory.KeyDeserializer<KEY> deserializer;
    private final int fixedSerializedSizeInBytes;

    private RoaringBitmap nullBitmap;
    private ByteBuffer blockByteBuffer;
    private List<DictionaryBlock<KEY>> blocks;
    private BitSliceIndexBitmap bsi;

    public RangeEncodeBitSliceBitmap(
            byte version,
            int rid,
            int cardinality,
            KEY min,
            KEY max,
            int blockOffset,
            int blockSerializedSizeInBytes,
            int blockSize,
            int entryOffset,
            int bitmapOffset,
            int nullBitmapSerializedSizeInBytes,
            int bsiSerializedInBytes,
            SeekableInputStream inputStream,
            KeyFactory.KeyDeserializer<KEY> deserializer,
            Comparator<KEY> comparator,
            int fixedSerializedSizeInBytes) {
        this.version = version;
        this.rid = rid;
        this.cardinality = cardinality;
        this.min = min;
        this.max = max;
        this.blockOffset = blockOffset;
        this.blockSerializedSizeInBytes = blockSerializedSizeInBytes;
        this.blockSize = blockSize;
        this.entryOffset = entryOffset;
        this.bitmapOffset = bitmapOffset;
        this.nullBitmapSerializedSizeInBytes = nullBitmapSerializedSizeInBytes;
        this.bsiSerializedInBytes = bsiSerializedInBytes;
        this.inputStream = inputStream;
        this.deserializer = deserializer;
        this.comparator = comparator;
        this.fixedSerializedSizeInBytes = fixedSerializedSizeInBytes;
    }

    public RoaringBitmap eq(KEY key) {
        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin == 0 && compareMax == 0) {
            return isNotNull();
        } else if (compareMin < 0 || compareMax > 0) {
            return new RoaringBitmap();
        }

        int code =
                findCode(key)
                        .orElseThrow(() -> new IllegalArgumentException("code should not be null"));
        if (code < 0) {
            return new RoaringBitmap();
        }

        return getBitSliceIndexBitmap().eq(code);
    }

    public RoaringBitmap lte(KEY key) {
        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMax >= 0) {
            return isNotNull();
        } else if (compareMin < 0) {
            return new RoaringBitmap();
        }

        int code =
                findCode(key)
                        .orElseThrow(() -> new IllegalArgumentException("code should not be null"));
        return code < 0 ? getBitSliceIndexBitmap().lte(-code) : getBitSliceIndexBitmap().lte(code);
    }

    public RoaringBitmap lt(KEY key) {
        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMax > 0) {
            return isNotNull();
        } else if (compareMin <= 0) {
            return new RoaringBitmap();
        }

        int code =
                findCode(key)
                        .orElseThrow(() -> new IllegalArgumentException("code should not be null"));
        return code < 0 ? getBitSliceIndexBitmap().lte(-code) : getBitSliceIndexBitmap().lt(code);
    }

    public RoaringBitmap gte(KEY key) {
        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin <= 0) {
            return isNotNull();
        } else if (compareMax > 0) {
            return new RoaringBitmap();
        }

        int code =
                findCode(key)
                        .orElseThrow(() -> new IllegalArgumentException("code should not be null"));
        return code < 0 ? getBitSliceIndexBitmap().gt(-code) : getBitSliceIndexBitmap().gte(code);
    }

    public RoaringBitmap gt(KEY key) {
        int compareMin = comparator.compare(key, min);
        int compareMax = comparator.compare(key, max);
        if (compareMin < 0) {
            return isNotNull();
        } else if (compareMax >= 0) {
            return new RoaringBitmap();
        }

        int code =
                findCode(key)
                        .orElseThrow(() -> new IllegalArgumentException("code should not be null"));
        return code < 0 ? getBitSliceIndexBitmap().gt(-code) : getBitSliceIndexBitmap().gt(code);
    }

    public RoaringBitmap isNull() {
        return getNullBitmap();
    }

    public RoaringBitmap isNotNull() {
        return RoaringBitmap.flip(getNullBitmap(), 0L, rid);
    }

    public RoaringBitmap topK(int k) {
        return getBitSliceIndexBitmap().topK(k);
    }

    public RoaringBitmap bottomK(int k) {
        return getBitSliceIndexBitmap().bottomK(k);
    }

    public KEY get(int position) {
        Long code = getBitSliceIndexBitmap().get(position);
        if (code == null) {
            return null;
        }
        List<DictionaryBlock<KEY>> blocks = getBlocks();
        int index =
                Collections.binarySearch(
                        blocks,
                        null,
                        (block, ignore) -> Integer.compare(block.code, (int) (long) code));
        if (index < 0) {
            index = -2 - index;
        }
        return blocks.get(index).findKey((int) (long) code).orElse(null);
    }

    public long count() {
        return rid;
    }

    public long countNotNull() {
        return count() - getNullBitmap().getCardinality();
    }

    public KEY min() {
        return min;
    }

    public KEY max() {
        return max;
    }

    public Optional<Integer> findCode(KEY key) {
        if (fixedSerializedSizeInBytes > 0) {
            // zero copy binary search
            ByteBuffer buf = getBlocksBuffer();
            DictionaryBlock<KEY> block = null;
            int size = DictionaryBlock.serializeSizeInBytes(fixedSerializedSizeInBytes);

            int low = 0;
            int high = blockSize - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                buf.position(mid * size);
                // the DictionaryBlock first field is the key
                KEY found = deserializer.deserialize(buf);
                int compareResult = comparator.compare(found, key);
                if (compareResult > 0) {
                    high = mid - 1;
                } else if (compareResult < 0) {
                    low = mid + 1;
                } else {
                    buf.position(mid * size);
                    block =
                            new DictionaryBlock<>(
                                    (ByteBuffer) buf.slice().limit(size),
                                    entryOffset,
                                    deserializer,
                                    comparator,
                                    fixedSerializedSizeInBytes,
                                    inputStream);
                    break;
                }
            }
            if (block == null) {
                buf.position((low - 1) * size);
                block =
                        new DictionaryBlock<>(
                                (ByteBuffer) buf.slice().limit(size),
                                entryOffset,
                                deserializer,
                                comparator,
                                fixedSerializedSizeInBytes,
                                inputStream);
            }

            return block.findCode(key);
        } else {
            List<DictionaryBlock<KEY>> blocks = getBlocks();
            int index =
                    Collections.binarySearch(
                            blocks, null, (block, ignore) -> comparator.compare(block.key, key));
            if (index < 0) {
                index = -2 - index;
            }
            return blocks.get(index).findCode(key);
        }
    }

    private List<DictionaryBlock<KEY>> getBlocks() {
        // deserialize the blocks
        if (blocks == null) {
            ByteBuffer buffer = getBlocksBuffer();
            blocks = new ArrayList<>(blockSize);
            for (int i = 0; i < blockSize; i++) {
                blocks.add(
                        new DictionaryBlock<>(
                                buffer,
                                entryOffset,
                                deserializer,
                                comparator,
                                fixedSerializedSizeInBytes,
                                inputStream));
            }
        }
        return blocks;
    }

    private ByteBuffer getBlocksBuffer() {
        if (blockByteBuffer == null) {
            try {
                inputStream.seek(blockOffset);
                byte[] bytes = new byte[blockSerializedSizeInBytes];
                inputStream.read(bytes);
                blockByteBuffer = ByteBuffer.wrap(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return blockByteBuffer;
    }

    private RoaringBitmap getNullBitmap() {
        if (nullBitmap == null) {
            try {
                inputStream.seek(bitmapOffset);
                byte[] bytes = new byte[nullBitmapSerializedSizeInBytes];
                inputStream.read(bytes);
                nullBitmap = new RoaringBitmap();
                nullBitmap.deserialize(ByteBuffer.wrap(bytes));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return nullBitmap;
    }

    private BitSliceIndexBitmap getBitSliceIndexBitmap() {
        if (bsi == null) {
            try {
                bsi =
                        BitSliceIndexBitmap.map(
                                inputStream,
                                bitmapOffset + nullBitmapSerializedSizeInBytes,
                                bsiSerializedInBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return bsi;
    }

    public static <KEY> RangeEncodeBitSliceBitmap<KEY> map(
            SeekableInputStream inputStream, int offset, KeyFactory<KEY> factory)
            throws IOException {
        KeyFactory.KeyDeserializer<KEY> deserializer = factory.createDeserializer();
        inputStream.seek(offset);

        byte[] bytes = new byte[Byte.BYTES + Integer.BYTES];
        inputStream.read(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte version = buffer.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException(
                    String.format(
                            "read index file fail, " + "your plugin version is lower than %d",
                            version));
        }
        int headerSerializedSizeInBytes = buffer.getInt();

        bytes = new byte[headerSerializedSizeInBytes];
        inputStream.read(bytes);
        buffer = ByteBuffer.wrap(bytes);

        int rid = buffer.getInt();
        int cardinality = buffer.getInt();
        KEY min = deserializer.deserialize(buffer);
        KEY max = deserializer.deserialize(buffer);
        int blockOffset = offset + buffer.getInt();
        int blockSerializedSizeInBytes = buffer.getInt();
        int blockSize = buffer.getInt();
        int entryOffset = offset + buffer.getInt();
        int bitmapOffset = offset + buffer.getInt();
        int nullBitmapSerializedSizeInBytes = buffer.getInt();
        int bsiSerializedInBytes = buffer.getInt();

        return new RangeEncodeBitSliceBitmap<>(
                version,
                rid,
                cardinality,
                min,
                max,
                blockOffset,
                blockSerializedSizeInBytes,
                blockSize,
                entryOffset,
                bitmapOffset,
                nullBitmapSerializedSizeInBytes,
                bsiSerializedInBytes,
                inputStream,
                deserializer,
                factory.createCompactor(),
                factory.fixedSerializedSizeInBytes());
    }

    public static class Appender<KEY> {

        private int rid;
        private final RoaringBitmap nullBitmaps;
        private final TreeMap<KEY, RoaringBitmap> bitmaps;
        private final int blockSizeLimit;
        private final KeyFactory.KeySerializer<KEY> serializer;
        private final int fixedSerializedSizeInBytes;

        public Appender(KeyFactory<KEY> factory, int blockSizeLimit) {
            this.rid = 0;
            this.nullBitmaps = new RoaringBitmap();
            this.bitmaps = new TreeMap<>(factory.createCompactor());
            this.serializer = factory.createSerializer();
            this.fixedSerializedSizeInBytes = factory.fixedSerializedSizeInBytes();
            this.blockSizeLimit = blockSizeLimit;
        }

        public void append(KEY key) {
            if (key == null) {
                nullBitmaps.add(rid++);
            } else {
                bitmaps.computeIfAbsent(key, (x) -> new RoaringBitmap()).add(rid++);
            }
        }

        public byte[] serialize() {
            if (rid == 0) {
                return new byte[] {};
            }

            nullBitmaps.runOptimize();

            BitSliceIndexBitmap bsi = new BitSliceIndexBitmap(0, bitmaps.size() - 1);
            int code = 0;
            int blockSize = 0;
            DictionaryBlock<KEY> currentBlock = null;

            KEY min = bitmaps.firstKey();
            KEY max = bitmaps.lastKey();

            Optional<Integer> estimatedBlockSerializedBytesInSize =
                    DictionaryBlock.estimatedBlockSerializedBytesInSize(
                            min,
                            serializer,
                            bitmaps.size(),
                            fixedSerializedSizeInBytes,
                            blockSizeLimit);
            Optional<Integer> estimatedEntrySerializedBytesInSize =
                    DictionaryBlock.estimatedEntrySerializedBytesInSize(
                            min,
                            serializer,
                            bitmaps.size(),
                            fixedSerializedSizeInBytes,
                            blockSizeLimit);
            ByteArrayOutputStream blockOutputStream =
                    new ByteArrayOutputStream(
                            estimatedBlockSerializedBytesInSize.orElse(16 * 1024));
            ByteArrayOutputStream entryOutputStream =
                    new ByteArrayOutputStream(
                            estimatedEntrySerializedBytesInSize.orElse(16 * 1024));
            DataOutputStream blockDataOutputStream = new DataOutputStream(blockOutputStream);
            DataOutputStream entryDataOutputStream = new DataOutputStream(entryOutputStream);
            for (Map.Entry<KEY, RoaringBitmap> entry : bitmaps.entrySet()) {
                KEY key = entry.getKey();
                RoaringBitmap bitmap = entry.getValue();

                // build the relationship between position and the dictionary code by the bsi
                for (Integer position : bitmap) {
                    bsi.set(position, code);
                }

                // build the dictionary
                if (currentBlock == null) {
                    currentBlock = new DictionaryBlock<>(key, code, 0, blockSizeLimit, serializer);
                    blockSize++;
                } else {
                    if (!currentBlock.tryAdd(key)) {
                        currentBlock.serializeBlock(blockDataOutputStream);
                        currentBlock.serializeEntry(entryDataOutputStream);

                        int offset = currentBlock.offset + currentBlock.entrySerializedSizeInBytes;
                        currentBlock =
                                new DictionaryBlock<>(
                                        key, code, offset, blockSizeLimit, serializer);
                        blockSize++;
                    }
                }
                code++;
            }

            if (currentBlock != null) {
                currentBlock.serializeBlock(blockDataOutputStream);
                currentBlock.serializeEntry(entryDataOutputStream);
            }

            int headerSerializeSizeInBytes = 0;
            headerSerializeSizeInBytes += Byte.BYTES; // version
            headerSerializeSizeInBytes += Integer.BYTES; // header length
            headerSerializeSizeInBytes += Integer.BYTES; // rid
            headerSerializeSizeInBytes += Integer.BYTES; // cardinality
            headerSerializeSizeInBytes += serializer.serializedSizeInBytes(min); // min
            headerSerializeSizeInBytes += serializer.serializedSizeInBytes(max); // max
            headerSerializeSizeInBytes += Integer.BYTES; // dictionary block offset
            headerSerializeSizeInBytes += Integer.BYTES; // dictionary block size
            headerSerializeSizeInBytes += Integer.BYTES; // dictionary block serialize size in bytes
            headerSerializeSizeInBytes += Integer.BYTES; // dictionary entry offset
            headerSerializeSizeInBytes += Integer.BYTES; // bitmap offsets
            headerSerializeSizeInBytes += Integer.BYTES; // null bitmap length
            headerSerializeSizeInBytes += Integer.BYTES; // bsi bitmap length

            // blockSize
            int blockSerializeSizeInBytes = blockDataOutputStream.size();
            int entrySerializeSizeInBytes = entryOutputStream.size();

            ByteBuffer bsiBuffer = bsi.serialize();
            int nullBitmapSerializedSizeInBytes = nullBitmaps.serializedSizeInBytes();
            int bsiSerializedSizeInBytes = bsiBuffer.array().length;

            ByteBuffer buffer =
                    ByteBuffer.allocate(
                            headerSerializeSizeInBytes
                                    + blockSerializeSizeInBytes
                                    + entrySerializeSizeInBytes
                                    + nullBitmapSerializedSizeInBytes
                                    + bsiSerializedSizeInBytes);

            // write header
            buffer.put(CURRENT_VERSION);
            buffer.putInt(headerSerializeSizeInBytes - Byte.BYTES - Integer.BYTES);
            buffer.putInt(rid);
            buffer.putInt(bitmaps.size());
            serializer.serialize(buffer, min);
            serializer.serialize(buffer, max);
            buffer.putInt(headerSerializeSizeInBytes); // block offset
            buffer.putInt(blockSerializeSizeInBytes); // block length
            buffer.putInt(blockSize); // block size
            buffer.putInt(
                    headerSerializeSizeInBytes
                            + blockSerializeSizeInBytes); // dictionary entry offset
            buffer.putInt(
                    headerSerializeSizeInBytes
                            + blockSerializeSizeInBytes
                            + entrySerializeSizeInBytes); // bitmap offsets
            buffer.putInt(nullBitmapSerializedSizeInBytes);
            buffer.putInt(bsiSerializedSizeInBytes);

            // write blocks
            buffer.put(blockOutputStream.toByteArray());

            // write entries
            buffer.put(entryOutputStream.toByteArray());

            // write null bitmap
            nullBitmaps.serialize(buffer);

            // write bsi
            buffer.put(bsiBuffer.array());

            return buffer.array();
        }
    }

    protected static class DictionaryBlock<KEY> {

        private final KEY key;
        private final int code;
        private final int offset;

        private KeyFactory.KeySerializer<KEY> serializer;
        private KeyFactory.KeyDeserializer<KEY> deserializer;
        private int blockSizeLimit;
        private List<KEY> entries;
        private int entrySize;
        private int entrySerializedSizeInBytes;
        private SeekableInputStream inputStream;
        private Comparator<KEY> comparator;
        private int fixedSerializedSizeInBytes;
        private ByteBuffer entryByteBuffer;

        public DictionaryBlock(
                KEY key,
                int code,
                int offset,
                int blockSizeLimit,
                KeyFactory.KeySerializer<KEY> serializer) {
            this.key = key;
            this.code = code;
            this.offset = offset;
            this.blockSizeLimit = blockSizeLimit;
            this.serializer = serializer;
            this.entrySerializedSizeInBytes = 0;
            this.entries = new LinkedList<>();
        }

        public DictionaryBlock(
                ByteBuffer buffer,
                int entryOffset,
                KeyFactory.KeyDeserializer<KEY> deserializer,
                Comparator<KEY> comparator,
                int fixedSerializedSizeInBytes,
                SeekableInputStream inputStream) {
            this.deserializer = deserializer;
            this.key = deserializer.deserialize(buffer);
            this.comparator = comparator;
            this.code = buffer.getInt();
            this.offset = entryOffset + buffer.getInt();
            this.entrySize = buffer.getInt();
            this.entrySerializedSizeInBytes = buffer.getInt();
            this.fixedSerializedSizeInBytes = fixedSerializedSizeInBytes;
            this.inputStream = inputStream;
        }

        public boolean tryAdd(KEY key) {
            int size = serializer.serializedSizeInBytes(key);
            if (entrySerializedSizeInBytes + size > blockSizeLimit) {
                return false;
            }
            entrySerializedSizeInBytes += size;
            entries.add(key);
            return true;
        }

        public Optional<KEY> findKey(int code) {
            if (this.code == code) {
                return Optional.of(key);
            }
            return Optional.of(getEntries().get(code - this.code - 1));
        }

        public Optional<Integer> findCode(KEY key) {
            if (comparator.compare(this.key, key) == 0) {
                return Optional.of(code);
            }

            // we need to return the negative code that tell the caller the code was not found
            if (fixedSerializedSizeInBytes > 0) {
                ByteBuffer buf = getEntryByteBuffer();
                int low = 0;
                int high = entrySize - 1;
                while (low <= high) {
                    int mid = (low + high) >>> 1;
                    buf.position(mid * fixedSerializedSizeInBytes);
                    KEY found = deserializer.deserialize(buf);
                    int compareResult = comparator.compare(found, key);
                    if (compareResult > 0) {
                        high = mid - 1;
                    } else if (compareResult < 0) {
                        low = mid + 1;
                    } else {
                        return Optional.of(code + mid + 1);
                    }
                }
                return Optional.of(-(code + low));
            } else {
                int index = Collections.binarySearch(getEntries(), key, comparator);
                return index < 0
                        ? Optional.of(-(code + (-2 - index) + 1))
                        : Optional.of(code + index + 1);
            }
        }

        public void serializeBlock(DataOutputStream outputStream) {
            if (serializer == null) {
                throw new IllegalArgumentException("serializer can not be null");
            }
            try {
                serializer.serialize(outputStream, key); // key
                outputStream.writeInt(code); // code
                outputStream.writeInt(offset); // offset
                outputStream.writeInt(entries.size()); // entry list size
                outputStream.writeInt(entrySerializedSizeInBytes); // entriesSerializedSizeInBytes
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void serializeEntry(DataOutputStream outputStream) {
            if (serializer == null) {
                throw new IllegalArgumentException("serializer can not be null");
            }
            try {
                for (KEY key : entries) {
                    serializer.serialize(outputStream, key);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private ByteBuffer getEntryByteBuffer() {
            if (entryByteBuffer == null) {
                try {
                    inputStream.seek(offset);
                    byte[] bytes = new byte[entrySerializedSizeInBytes];
                    inputStream.read(bytes);
                    entryByteBuffer = ByteBuffer.wrap(bytes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return entryByteBuffer;
        }

        private List<KEY> getEntries() {
            // deserialize the entries
            if (entries == null) {
                if (deserializer == null) {
                    throw new IllegalArgumentException("deserializer can not be null");
                }
                ByteBuffer buffer = getEntryByteBuffer();
                entries = new ArrayList<>(entrySize);
                for (int i = 0; i < entrySize; i++) {
                    entries.add(deserializer.deserialize(buffer));
                }
            }
            return entries;
        }

        public static int estimatedBlocks(int total, int blockSizeLimit) {
            return Math.floorDiv(total, blockSizeLimit) + 1;
        }

        public static <KEY> Optional<Integer> estimatedBlockSerializedBytesInSize(
                KEY key,
                KeyFactory.KeySerializer<KEY> serializer,
                int size,
                int fixedSerializedSizeInBytes,
                int blockSizeLimit) {
            if (fixedSerializedSizeInBytes <= 0) {
                return Optional.empty();
            }

            int keySerializedInBytes = serializer.serializedSizeInBytes(key);
            int blocks = estimatedBlocks(keySerializedInBytes * size, blockSizeLimit);
            return Optional.of(blocks * serializeSizeInBytes(keySerializedInBytes));
        }

        public static int serializeSizeInBytes(int keySerializedSizeInBytes) {
            int blockSerializedInBytes = 0;
            blockSerializedInBytes += keySerializedSizeInBytes;
            blockSerializedInBytes += Integer.BYTES;
            blockSerializedInBytes += Integer.BYTES;
            blockSerializedInBytes += Integer.BYTES;
            blockSerializedInBytes += Integer.BYTES;
            return blockSerializedInBytes;
        }

        public static <KEY> Optional<Integer> estimatedEntrySerializedBytesInSize(
                KEY key,
                KeyFactory.KeySerializer<KEY> serializer,
                int size,
                int fixedSerializedSizeInBytes,
                int blockSizeLimit) {
            if (fixedSerializedSizeInBytes <= 0) {
                return Optional.empty();
            }

            int keySerializedInBytes = serializer.serializedSizeInBytes(key);
            int blocks = estimatedBlocks(keySerializedInBytes * size, blockSizeLimit);
            return Optional.of(keySerializedInBytes * (size - blocks));
        }
    }
}
