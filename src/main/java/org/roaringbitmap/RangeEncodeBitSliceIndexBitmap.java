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

import org.assertj.core.util.VisibleForTesting;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

public class RangeEncodeBitSliceIndexBitmap implements BitSliceIndexBitmap {

    public static final byte VERSION_1 = 1;
    public static final byte CURRENT_VERSION = VERSION_1;

    private final byte version;
    private long mask;
    private long min;
    private long max;
    private long emptySliceMask;
    private int[] offsets;

    private ByteBuffer body;
    private RoaringBitmap ebm;
    private RoaringBitmap[] slices;

    public RangeEncodeBitSliceIndexBitmap() {
        this(Long.MIN_VALUE, Long.MIN_VALUE);
    }

    public RangeEncodeBitSliceIndexBitmap(long min, long max) {
        this.version = CURRENT_VERSION;
        this.min = min;
        this.max = max;
        this.ebm = new RoaringBitmap();
        this.slices =
                max == Long.MIN_VALUE
                        ? new RoaringBitmap[] {}
                        : new RoaringBitmap[64 - Long.numberOfLeadingZeros(max)];
        for (int i = 0; i < slices.length; i++) {
            slices[i] = new RoaringBitmap();
        }
        this.mask = generateMask(max);
        this.emptySliceMask = 0;
    }

    public RangeEncodeBitSliceIndexBitmap(ByteBuffer buffer) {
        version = buffer.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException(
                    String.format(
                            "deserialize bsi index fail, " + "current version is lower than %d",
                            version));
        }

        // deserialize min & max
        min = buffer.getLong();
        max = buffer.getLong();
        emptySliceMask = buffer.getLong();

        // read offsets
        byte length = buffer.get();
        offsets = new int[length];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = buffer.getInt();
        }

        // byte buffer
        body = buffer.slice();
        slices = new RoaringBitmap[offsets.length];

        // mask
        mask = generateMask(max);
    }

    @Override
    public void set(int rid, long value) {
        if (value < 0) {
            throw new UnsupportedOperationException("value can not be negative");
        }

        if (min < 0 || min > value) {
            min = value;
        }

        if (max < 0 || max < value) {
            max = value;
            mask = generateMask(max);

            // grow the slices
            int capacity = Long.toBinaryString(max).length();
            int old = slices.length;
            if (old < capacity) {
                RoaringBitmap[] newSlices = new RoaringBitmap[capacity];
                for (int i = 0; i < capacity; i++) {
                    if (i < old) {
                        newSlices[i] = slices[i];
                    } else {
                        // the mask changed, and range encoded always flip the binary
                        // so we need to fill it with ebm.clone
                        // but using bitmapOfRange instead can get more space saving
                        if (ebm.isEmpty()) {
                            newSlices[i] = new RoaringBitmap();
                        } else {
                            newSlices[i] = RoaringBitmap.bitmapOfRange(ebm.first(), ebm.last() + 1);
                            emptySliceMask |= (1L << i);
                        }
                    }
                }
                slices = newSlices;
            }
        }

        // range encode
        long bits = ~value & mask;

        // mark the empty slice
        emptySliceMask |= bits;

        // only bit=1 need to set
        while (bits != 0) {
            getSlice(Long.numberOfTrailingZeros(bits)).add(rid);
            bits &= (bits - 1);
        }
        getExistenceBitmap().add(rid);
    }

    @Override
    public Long get(int rid) {
        if (!exists(rid)) {
            return null;
        }
        long value = 0;
        for (int i = 0; i < slices.length; i++) {
            if (!getSlice(i).contains(rid)) {
                value |= (1L << i);
            }
        }
        return value;
    }

    @Override
    public boolean exists(int rid) {
        return getExistenceBitmap().contains(rid);
    }

    @Override
    public ByteBuffer serialize() {
        if (ebm.isEmpty()) {
            return ByteBuffer.allocate(0);
        }

        int header = 0;
        header += Byte.BYTES;
        header += Long.BYTES;
        header += Long.BYTES;
        header += Long.BYTES;
        header += Long.BYTES;
        header += Byte.BYTES;
        header += slices.length * Integer.BYTES;

        int body = 0;
        ebm.runOptimize();
        body += ebm.serializedSizeInBytes();

        int[] offsets = new int[slices.length];
        for (int i = 0; i < slices.length; i++) {
            slices[i].runOptimize();
            body += slices[i].serializedSizeInBytes();
            if (i == 0) {
                offsets[i] = ebm.serializedSizeInBytes();
            } else {
                offsets[i] = offsets[i - 1] + slices[i - 1].serializedSizeInBytes();
            }
        }

        int sizeInBytes = header + body;
        ByteBuffer buffer = ByteBuffer.allocate(sizeInBytes);

        buffer.put(CURRENT_VERSION);
        buffer.putLong(min);
        buffer.putLong(max);
        buffer.putLong(emptySliceMask);
        buffer.put((byte) slices.length);
        for (int offset : offsets) {
            buffer.putInt(offset);
        }
        ebm.serialize(buffer);
        for (RoaringBitmap slice : slices) {
            slice.serialize(buffer);
        }

        return buffer;
    }

    @Override
    public RoaringBitmap eq(long predicate, @Nullable RoaringBitmap foundSet) {
        if (min == max && min == predicate) {
            return foundSet == null
                    ? getExistenceBitmap()
                    : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        } else if (predicate < min || predicate > max) {
            return new RoaringBitmap();
        }

        RoaringBitmap state =
                foundSet == null
                        ? getExistenceBitmap()
                        : RoaringBitmap.and(foundSet, getExistenceBitmap());
        if (state.isEmpty()) {
            return new RoaringBitmap();
        }

        for (int i = slices.length - 1; i >= 0; i--) {
            long bit = (predicate >> i) & 1;
            if (bit == 1) {
                state = RoaringBitmap.andNot(state, getSlice(i));
            } else {
                state = RoaringBitmap.and(state, getSlice(i));
            }
        }
        return state;
    }

    @Override
    public RoaringBitmap lte(long predicate, @Nullable RoaringBitmap foundSet) {
        if (predicate >= max) {
            return foundSet == null
                    ? getExistenceBitmap()
                    : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        } else if (predicate < min) {
            return new RoaringBitmap();
        }

        RoaringBitmap fixedFoundSet =
                foundSet == null
                        ? getExistenceBitmap()
                        : RoaringBitmap.and(foundSet, getExistenceBitmap());
        if (fixedFoundSet.isEmpty()) {
            return new RoaringBitmap();
        }

        // the state is always start from the full bitmap
        RoaringBitmap state = RoaringBitmap.bitmapOfRange(fixedFoundSet.first(), fixedFoundSet.last() + 1);

        // if there is a run of k set bits starting from 0, all k operations can be eliminated.
        int start = Long.numberOfTrailingZeros(~predicate & mask);

        // if there is an empty slice at position k and the bit k is absent from the threshold x
        // skip k operations, and the state is an empty slice.
        if (emptySliceMask != mask) {
            start = Long.SIZE - Long.numberOfLeadingZeros(~(emptySliceMask | predicate) & mask);
            state = new RoaringBitmap();
        }

        for (int i = start; i < slices.length; i++) {
            long bit = (predicate >> i) & 1;
            if (bit == 1) {
                state.or(getSlice(i));
            } else {
                state.and(getSlice(i));
            }
        }
        return RoaringBitmap.and(state, fixedFoundSet);
    }

    @Override
    public RoaringBitmap gt(long predicate, @Nullable RoaringBitmap foundSet) {
        if (predicate < min) {
            return foundSet == null
                    ? getExistenceBitmap()
                    : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        } else if (predicate >= max) {
            return new RoaringBitmap();
        }

        RoaringBitmap state =
                foundSet == null
                        ? getExistenceBitmap()
                        : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        if (state.isEmpty()) {
            return new RoaringBitmap();
        }

        long start = state.first();
        long end = state.last() + 1;
        RoaringBitmap lte = lte(predicate, state);
        return RoaringBitmap.and(RoaringBitmap.flip(lte, start, end), state);
    }

    @Override
    public RoaringBitmap isNotNull(@Nullable RoaringBitmap foundSet) {
        return getExistenceBitmap();
    }

    @Override
    public RoaringBitmap topK(int k, @Nullable RoaringBitmap foundSet) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap();
        }

        if (k < 0) {
            throw new IllegalArgumentException(
                    "the k param can not be negative in bottomK, k=" + k);
        }

        RoaringBitmap g = new RoaringBitmap();
        RoaringBitmap e =
                foundSet == null
                        ? getExistenceBitmap()
                        : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        if (e.isEmpty()) {
            return new RoaringBitmap();
        }

        for (int i = slices.length - 1; i >= 0; i--) {
            RoaringBitmap x = RoaringBitmap.or(g, RoaringBitmap.andNot(e, getSlice(i)));
            long n = x.getCardinality();
            if (n > k) {
                e = RoaringBitmap.andNot(e, getSlice(i));
            } else if (n < k) {
                g = x;
                e = RoaringBitmap.and(e, getSlice(i));
            } else {
                e = RoaringBitmap.andNot(e, getSlice(i));
                break;
            }
        }

        // only k results should be returned
        RoaringBitmap f = RoaringBitmap.or(g, e);
        long n = f.getCardinality() - k;
        if (n > 0) {
            IntIterator iterator = e.getIntIterator();
            while (iterator.hasNext() && n > 0) {
                f.remove(iterator.next());
                n--;
            }
        }
        return f;
    }

    @Override
    public RoaringBitmap bottomK(int k, @Nullable RoaringBitmap foundSet) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap();
        }

        if (k < 0) {
            throw new IllegalArgumentException(
                    "the k param can not be negative in bottomK, k=" + k);
        }

        RoaringBitmap g = new RoaringBitmap();
        RoaringBitmap e =
                foundSet == null
                        ? getExistenceBitmap()
                        : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        if (e.isEmpty()) {
            return new RoaringBitmap();
        }

        for (int i = slices.length - 1; i >= 0; i--) {
            RoaringBitmap x = RoaringBitmap.or(g, RoaringBitmap.and(e, getSlice(i)));
            long n = x.getLongCardinality();
            if (n > k) {
                e = RoaringBitmap.and(e, getSlice(i));
            } else if (n < k) {
                g = x;
                e = RoaringBitmap.andNot(e, getSlice(i));
            } else {
                e = RoaringBitmap.and(e, getSlice(i));
                break;
            }
        }

        // only k results should be returned
        RoaringBitmap f = RoaringBitmap.or(g, e);
        long n = f.getCardinality() - k;
        if (n > 0) {
            IntIterator iterator = e.getIntIterator();
            while (iterator.hasNext() && n > 0) {
                f.remove(iterator.next());
                n--;
            }
        }
        return f;
    }

    @Override
    public Long min(@Nullable RoaringBitmap foundSet) {
        if (foundSet == null) {
            return getExistenceBitmap().isEmpty() ? null : min;
        }
        RoaringBitmap bottomK = bottomK(1, foundSet);
        if (bottomK.isEmpty()) {
            return null;
        }
        return get(bottomK.first());
    }

    @Override
    public Long max(@Nullable RoaringBitmap foundSet) {
        if (foundSet == null) {
            return getExistenceBitmap().isEmpty() ? null : max;
        }
        RoaringBitmap topK = topK(1, foundSet);
        if (topK.isEmpty()) {
            return null;
        }
        return get(topK.first());
    }

    @Override
    public Long sum(@Nullable RoaringBitmap foundSet) {
        if (foundSet != null && foundSet.isEmpty()) {
            return null;
        }

        RoaringBitmap ebm = getExistenceBitmap();
        if (ebm.isEmpty()) {
            return null;
        }

        RoaringBitmap state =
                foundSet == null
                        ? getExistenceBitmap()
                        : RoaringBitmap.and(getExistenceBitmap(), foundSet);

        return IntStream.range(0, slices.length)
                .mapToLong(
                        x -> (1L << x) * RoaringBitmap.andNot(state, getSlice(x)).getCardinality())
                .sum();
    }

    @Override
    public Long count(@Nullable RoaringBitmap foundSet) {
        if (foundSet != null && foundSet.isEmpty()) {
            return 0L;
        }

        RoaringBitmap ebm = getExistenceBitmap();
        if (ebm.isEmpty()) {
            return 0L;
        }

        RoaringBitmap state =
                foundSet == null
                        ? getExistenceBitmap()
                        : RoaringBitmap.and(getExistenceBitmap(), foundSet);
        return state.getLongCardinality();
    }

    @VisibleForTesting
    protected long getEmptySliceMask() {
        return emptySliceMask;
    }

    @VisibleForTesting
    protected RoaringBitmap[] getSlices() {
        RoaringBitmap[] result = new RoaringBitmap[slices.length];
        for (int i = 0; i < slices.length; i++) {
            result[i] = getSlice(i);
        }
        return result;
    }

    private RoaringBitmap getSlice(int index) {
        try {
            if (slices[index] == null) {
                body.position(offsets[index]);
                RoaringBitmap bitmap = new RoaringBitmap();
                bitmap.deserialize(body);
                slices[index] = bitmap;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return slices[index];
    }

    private RoaringBitmap getExistenceBitmap() {
        try {
            if (ebm == null) {
                body.position(0);
                RoaringBitmap bitmap = new RoaringBitmap();
                bitmap.deserialize(body);
                ebm = bitmap;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ebm;
    }

    private long generateMask(long value) {
        if (value < 0) {
            return 0;
        }
        return -1L >>> Long.numberOfLeadingZeros(value | 1);
    }
}
