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

import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;

public class ImmutableBitSliceIndexBitmap implements BitSliceIndex {

    public static final byte VERSION_1 = 1;
    public static final byte CURRENT_VERSION = VERSION_1;

    private final byte version;
    private long min;
    private long max;
    private int[] offsets;

    private ByteBuffer body;
    private RoaringBitmap ebm;
    private RoaringBitmap[] slices;

    private ImmutableBitSliceIndexBitmap(
            byte version, long min, long max, int[] offsets, ByteBuffer body) {
        this.version = version;
        this.min = min;
        this.max = max;
        this.offsets = offsets;
        this.slices = new RoaringBitmap[offsets.length];
        this.body = body;
    }

    @Override
    public void set(int key, long value) {
        throw new UnsupportedOperationException("current bsi is immutable.");
    }

    @Nullable
    @Override
    public Long get(int key) {
        if (!exists(key)) {
            return null;
        }
        long value = 0;
        for (int i = 0; i < bitCount(); i++) {
            if (getSlice(i).contains(key)) {
                value |= (1L << i);
            }
        }
        return value + min;
    }

    @Override
    public boolean exists(int key) {
        return getExistenceBitmap().contains(key);
    }

    @Override
    public int bitCount() {
        return slices.length;
    }

    @Override
    public void merge(BitSliceIndex other) {
        if (other == null) {
            return;
        }

        // other bsi is not support yet.
        if (!(other instanceof ImmutableBitSliceIndexBitmap)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%s bsi is not support to merge with the immutable yet.",
                            other.getClass().getName()));
        }

        ImmutableBitSliceIndexBitmap bsi = (ImmutableBitSliceIndexBitmap) other;
        RoaringBitmap otherEbm = bsi.getExistenceBitmap();
        if (otherEbm.isEmpty()) {
            return;
        }

        // merge two bsi into new slices
        RoaringBitmap currentEbm = getExistenceBitmap();
        int length = Integer.max(bitCount(), bsi.bitCount());
        RoaringBitmap[] newSlices = new RoaringBitmap[length];
        RoaringBitmap update = RoaringBitmap.and(otherEbm, currentEbm);
        for (int i = 0; i < length; i++) {
            RoaringBitmap left = i < bitCount() ? getSlice(i) : new RoaringBitmap();
            RoaringBitmap right = i < bsi.bitCount() ? bsi.getSlice(i) : new RoaringBitmap();
            newSlices[i] = RoaringBitmap.or(RoaringBitmap.andNot(left, update), right);
        }

        // we need to plus a value
        if (min != bsi.min) {
            long value = min > bsi.min ? min - bsi.min : bsi.min - min;
            RoaringBitmap foundSet =
                    min > bsi.min ? RoaringBitmap.andNot(currentEbm, otherEbm) : otherEbm;

            RoaringBitmap carrier = new RoaringBitmap();
            length = Math.max(Long.toBinaryString(value).length(), newSlices.length);
            for (int i = 0; i < length; i++) {
                long bit = (value >> i) & 1;
                if (i > newSlices.length - 1) {
                    newSlices = Arrays.copyOf(newSlices, newSlices.length + 1);
                    newSlices[i] = new RoaringBitmap();
                }
                RoaringBitmap matchedSlice = RoaringBitmap.and(newSlices[i], foundSet);
                // carry the slice by the last carrier
                RoaringBitmap carriedSlice = RoaringBitmap.xor(matchedSlice, carrier);
                carrier = RoaringBitmap.and(matchedSlice, carrier);
                if (bit == 1) {
                    // carry the slice
                    RoaringBitmap current = RoaringBitmap.xor(carriedSlice, foundSet);
                    carrier = RoaringBitmap.or(carrier, RoaringBitmap.and(carriedSlice, foundSet));
                    newSlices[i] =
                            RoaringBitmap.or(RoaringBitmap.andNot(newSlices[i], foundSet), current);
                } else {
                    newSlices[i] =
                            RoaringBitmap.or(
                                    RoaringBitmap.andNot(newSlices[i], foundSet), carriedSlice);
                }
            }

            if (!carrier.isEmpty()) {
                newSlices = Arrays.copyOf(newSlices, newSlices.length + 1);
                newSlices[newSlices.length - 1] = carrier;
            }
        }

        slices = newSlices;
        ebm = RoaringBitmap.or(currentEbm, otherEbm);
        min = Long.min(min, bsi.min);
        max = Long.max(max, bsi.max);
        body = null;
        offsets = null;
    }

    @Override
    public ByteBuffer serialize() {
        throw new UnsupportedOperationException("current bsi does not support serialize.");
    }

    @Override
    public RoaringBitmap eq(long predicate, @Nullable RoaringBitmap foundSet) {
        if (min == max && min == predicate) {
            return isNotNull(foundSet);
        } else if (predicate < min || predicate > max) {
            return new RoaringBitmap();
        }

        RoaringBitmap state = isNotNull(foundSet);
        if (state.isEmpty()) {
            return new RoaringBitmap();
        }

        long bits = predicate - min;
        for (int i = 0; i < bitCount(); i++) {
            long bit = (bits >> i) & 1;
            if (bit == 1) {
                state = RoaringBitmap.and(state, getSlice(i));
            } else {
                state = RoaringBitmap.andNot(state, getSlice(i));
            }
        }
        return state;
    }

    @Override
    public RoaringBitmap lte(long predicate, @Nullable RoaringBitmap foundSet) {
        if (predicate >= max) {
            return isNotNull(foundSet);
        } else if (predicate < min) {
            return new RoaringBitmap();
        }

        RoaringBitmap state = isNotNull(foundSet);
        if (state.isEmpty()) {
            return new RoaringBitmap();
        }

        long start = state.first();
        long end = state.last() + 1;
        RoaringBitmap gt = gt(predicate, state);
        return RoaringBitmap.and(RoaringBitmap.flip(gt, start, end), state);
    }

    @Override
    public RoaringBitmap gt(long predicate, @Nullable RoaringBitmap foundSet) {
        if (predicate < min) {
            return isNotNull(foundSet);
        } else if (predicate >= max) {
            return new RoaringBitmap();
        }

        RoaringBitmap mergedFoundSet = isNotNull(foundSet);
        if (mergedFoundSet.isEmpty()) {
            return new RoaringBitmap();
        }

        long bits = predicate - min;

        // the state is always start from the empty bitmap
        RoaringBitmap state = new RoaringBitmap();

        // if there is a run of k set bits starting from 0, [0, k] operations can be eliminated.
        int start = Long.numberOfTrailingZeros(~bits);
        for (int i = start; i < bitCount(); i++) {
            long bit = (bits >> i) & 1;
            if (bit == 1) {
                state.and(getSlice(i));
            } else {
                state.or(getSlice(i));
            }
        }

        state.and(mergedFoundSet);
        return state;
    }

    @Override
    public RoaringBitmap isNotNull(@Nullable RoaringBitmap foundSet) {
        return foundSet == null
                ? getExistenceBitmap()
                : RoaringBitmap.and(getExistenceBitmap(), foundSet);
    }

    @Override
    public RoaringBitmap topK(int k, @Nullable RoaringBitmap foundSet) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap();
        }

        if (k < 0) {
            throw new IllegalArgumentException("the k param can not be negative in topK, k=" + k);
        }

        RoaringBitmap g = new RoaringBitmap();
        RoaringBitmap e = isNotNull(foundSet);
        if (e.getCardinality() <= k) {
            return e;
        }

        for (int i = bitCount() - 1; i >= 0; i--) {
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
    public RoaringBitmap bottomK(int k, @Nullable RoaringBitmap foundSet) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap();
        }

        if (k < 0) {
            throw new IllegalArgumentException(
                    "the k param can not be negative in bottomK, k=" + k);
        }

        RoaringBitmap g = new RoaringBitmap();
        RoaringBitmap e = isNotNull(foundSet);
        if (e.getCardinality() <= k) {
            return e;
        }

        for (int i = bitCount() - 1; i >= 0; i--) {
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

    @Nullable
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

    @Nullable
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

    @Nullable
    @Override
    public Long sum(@Nullable RoaringBitmap foundSet) {
        if (foundSet != null && foundSet.isEmpty()) {
            return null;
        }

        RoaringBitmap ebm = getExistenceBitmap();
        if (ebm.isEmpty()) {
            return null;
        }

        RoaringBitmap state = isNotNull(foundSet);
        return IntStream.range(0, bitCount())
                        .mapToLong(
                                x -> (1L << x) * RoaringBitmap.andCardinality(state, getSlice(x)))
                        .sum()
                + (min * state.getCardinality());
    }

    @Nullable
    @Override
    public Long sumDistinct(@Nullable RoaringBitmap foundSet) {
        Long sum = null;
        if (foundSet != null && foundSet.isEmpty()) {
            return sum;
        }

        RoaringBitmap ebm = getExistenceBitmap();
        if (ebm.isEmpty()) {
            return sum;
        }

        RoaringBitmap fixedFoundSet = isNotNull(foundSet);
        Roaring64Bitmap values = new Roaring64Bitmap();
        for (Integer key : fixedFoundSet) {
            Long value = this.get(key);
            if (value == null || values.contains(value)) {
                continue;
            }
            values.add(value);
            sum = sum == null ? value : sum + value;
        }

        return sum;
    }

    @Override
    public long countNotNull(@Nullable RoaringBitmap foundSet) {
        if (foundSet != null && foundSet.isEmpty()) {
            return 0L;
        }

        RoaringBitmap ebm = getExistenceBitmap();
        if (ebm.isEmpty()) {
            return 0L;
        }

        return isNotNull(foundSet).getLongCardinality();
    }

    @Override
    public long countDistinct(@Nullable RoaringBitmap foundSet) {
        long count = 0L;
        if (foundSet != null && foundSet.isEmpty()) {
            return count;
        }

        RoaringBitmap ebm = getExistenceBitmap();
        if (ebm.isEmpty()) {
            return count;
        }

        RoaringBitmap fixedFoundSet = isNotNull(foundSet);
        Roaring64Bitmap values = new Roaring64Bitmap();
        for (Integer key : fixedFoundSet) {
            Long value = this.get(key);
            if (value == null || values.contains(value)) {
                continue;
            }
            values.add(value);
            count++;
        }

        return count;
    }

    @Override
    public RoaringBitmap distinct(@Nullable RoaringBitmap foundSet) {
        if (foundSet != null && foundSet.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap ebm = getExistenceBitmap();
        if (ebm.isEmpty()) {
            return new RoaringBitmap();
        }

        RoaringBitmap fixedFoundSet = isNotNull(foundSet);
        RoaringBitmap position = new RoaringBitmap();
        Roaring64Bitmap values = new Roaring64Bitmap();
        for (Integer key : fixedFoundSet) {
            Long value = this.get(key);
            if (value == null || values.contains(value)) {
                continue;
            }
            values.add(value);
            position.add(key);
        }

        return position;
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

    public static ImmutableBitSliceIndexBitmap map(ByteBuffer buffer) {
        byte version = buffer.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException(
                    String.format(
                            "deserialize bsi index fail, " + "current version is lower than %d",
                            version));
        }

        // deserialize min & max
        long min = buffer.getLong();
        long max = buffer.getLong();

        // read offsets
        byte length = buffer.get();
        int[] offsets = new int[length];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = buffer.getInt();
        }

        // byte buffer
        ByteBuffer body = buffer.slice();
        return new ImmutableBitSliceIndexBitmap(version, min, max, offsets, body);
    }

    public static final class Appender {

        private long min;
        private long max;
        private RoaringBitmap ebm;
        private RoaringBitmap[] slices;

        public Appender() {
            this.min = Long.MIN_VALUE;
            this.max = Long.MIN_VALUE;
            this.ebm = new RoaringBitmap();
            this.slices = new RoaringBitmap[] {};
        }

        public void append(int key, long value) {
            if (value < 0) {
                throw new UnsupportedOperationException("value can not be negative");
            }

            if (min < 0 || min > value) {
                min = value;
            }

            if (max < 0 || max < value) {
                max = value;

                // grow the slices
                int capacity = Long.toBinaryString(max).length();
                int old = slices.length;
                if (old < capacity) {
                    RoaringBitmap[] newSlices = new RoaringBitmap[capacity];
                    for (int i = 0; i < capacity; i++) {
                        if (i < old) {
                            newSlices[i] = slices[i];
                        } else {
                            newSlices[i] = new RoaringBitmap();
                        }
                    }
                    slices = newSlices;
                }
            }

            long bits = value;

            // only bit=1 need to set
            while (bits != 0) {
                slices[Long.numberOfTrailingZeros(bits)].add(key);
                bits &= (bits - 1);
            }
            ebm.add(key);
        }

        public ByteBuffer serialize() {
            if (ebm.isEmpty()) {
                return ByteBuffer.allocate(0);
            }

            runOptimize();

            int header = 0;
            header += Byte.BYTES;
            header += Long.BYTES;
            header += Long.BYTES;
            header += Byte.BYTES;
            header += slices.length * Integer.BYTES;

            int body = 0;
            body += ebm.serializedSizeInBytes();

            int[] offsets = new int[slices.length];
            for (int i = 0; i < slices.length; i++) {
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

        private void runOptimize() {
            // minus the minimum to reduce the number of slices
            RoaringBitmap borrow = new RoaringBitmap();
            for (int i = 0; i < slices.length; i++) {
                long bit = (min >> i) & 1;
                RoaringBitmap slice = slices[i];
                // after borrowed by the last bit slice
                RoaringBitmap borrowedSlice = RoaringBitmap.andNot(slice, borrow);
                // elimination the state
                RoaringBitmap eliminated = RoaringBitmap.andNot(borrow, slice);
                if (bit == 1) {
                    RoaringBitmap current = RoaringBitmap.andNot(ebm, borrowedSlice);
                    borrow = RoaringBitmap.or(eliminated, current);
                    slices[i] = RoaringBitmap.andNot(current, eliminated);
                } else {
                    borrow = eliminated;
                    slices[i] = RoaringBitmap.or(borrowedSlice, borrow);
                }
            }

            // find the first not empty slice start from the end
            int end = slices.length - 1;
            for (; end > 0; end--) {
                if (!slices[end].isEmpty()) {
                    break;
                }
            }
            slices = Arrays.copyOfRange(slices, 0, end + 1);

            // bitmap runOptimize
            ebm.runOptimize();
            Arrays.stream(slices).forEach(RoaringBitmap::runOptimize);
        }
    }
}
