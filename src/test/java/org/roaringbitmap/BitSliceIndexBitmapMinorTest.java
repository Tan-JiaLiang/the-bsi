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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class BitSliceIndexBitmapMinorTest {

    @Test
    public void testTimeMillis() {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap();
        range.set(0, 1736907560000L);
        range.set(1, 1736907460000L);
        range.set(5, 1736908460000L);
        range.set(6, 1736902450000L);
        range.set(9, 1736902410000L);
        range.set(10, 1736903480000L);
        assertThat(range.gt(1736903410000L)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 5, 10));
        assertThat(range.lte(1736903410000L)).isEqualTo(RoaringBitmap.bitmapOf(6, 9));
    }

    @Test
    public void testGT() {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap();
        range.set(0, 1);
        range.set(1, 3);
        range.set(2, 0);
        range.set(10, 8);
        range.set(15, 30);
        range.set(18, 2);
        range.set(25, 12);

        assertThat(range.gt(3)).isEqualTo(RoaringBitmap.bitmapOf(10, 15, 25));
        assertThat(range.gt(3, RoaringBitmap.bitmapOf(0, 1, 2, 18, 25)))
                .isEqualTo(RoaringBitmap.bitmapOf(25));
    }

    @Test
    public void testLTE() throws IOException {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap();
        range.set(0, 1);
        range.set(1, 3);
        range.set(2, 0);
        range.set(10, 8);
        range.set(15, 30);
        range.set(18, 2);
        range.set(25, 12);

        assertThat(range.lte(3)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 18));
        assertThat(range.lte(10, RoaringBitmap.bitmapOf(0, 1, 2, 18, 25)))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 18));
    }

    @Test
    public void testEmptyMask() {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap();
        range.set(0, 9);
        range.set(2, 10);
        range.set(5, 11);
        range.set(18, 1);
        range.set(19, 2);
        range.set(21, 3);

        ByteBuffer serialize = range.serialize();
        BitSliceIndexBitmap bsi = new BitSliceIndexBitmap(ByteBuffer.wrap(serialize.array()));

        RoaringBitmap[] slices = bsi.getSlices();
        long emptySliceMask = bsi.getEmptySliceMask();

        for (int i = 0; i < slices.length; i++) {
            long emptySliceBit = (emptySliceMask >> i) & 1;
            if (emptySliceBit == 0) {
                assertThat(slices[i].isEmpty()).isTrue();
            }
        }

        assertThat(range.gt(6)).isEqualTo(RoaringBitmap.bitmapOf(0, 2, 5));
    }

    @Test
    public void testEQ() throws IOException {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap(0, 30);
        range.set(0, 1);
        range.set(1, 3);
        range.set(2, 0);
        range.set(10, 8);
        range.set(15, 30);
        range.set(18, 2);
        range.set(25, 12);

        assertThat(range.eq(10, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf());
        assertThat(range.eq(0, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(2));
        assertThat(range.eq(1, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(0));
        assertThat(range.eq(3, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(1));
        assertThat(range.eq(2, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(18));
        assertThat(range.eq(8, RoaringBitmap.bitmapOf(0, 1, 12, 10)))
                .isEqualTo(RoaringBitmap.bitmapOf(10));
        assertThat(range.eq(15, RoaringBitmap.bitmapOf(0, 1, 12, 10)))
                .isEqualTo(RoaringBitmap.bitmapOf());
        assertThat(range.eq(3, RoaringBitmap.bitmapOf(0, 1, 12, 10)))
                .isEqualTo(RoaringBitmap.bitmapOf(1));
    }

    @Test
    public void testBottomK() throws IOException {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap(0, 30);
        range.set(0, 1);
        range.set(1, 3);
        range.set(2, 0);
        range.set(10, 8);
        range.set(15, 30);
        range.set(18, 2);
        range.set(25, 12);

        assertThat(range.bottomK(1, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(2));
        assertThat(range.bottomK(2, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(0, 2));
        assertThat(range.bottomK(3, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(0, 2, 18));
        assertThat(range.bottomK(4, range.isNotNull()))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 18));
        assertThat(range.bottomK(5, range.isNotNull()))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 10, 18));
        assertThat(range.bottomK(6, range.isNotNull()))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 10, 18, 25));
        assertThat(range.bottomK(7, range.isNotNull()))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 10, 15, 18, 25));

        assertThat(range.bottomK(1, RoaringBitmap.bitmapOf(0, 15)))
                .isEqualTo(RoaringBitmap.bitmapOf(0));
        assertThat(range.bottomK(1, RoaringBitmap.bitmapOf(0, 10, 25)))
                .isEqualTo(RoaringBitmap.bitmapOf(0));
        assertThat(range.bottomK(3, RoaringBitmap.bitmapOf(0, 10, 25, 15)))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 10, 25));
    }

    @Test
    public void testTopK() throws IOException {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap(1, 30);
        range.set(0, 1);
        range.set(1, 3);
        range.set(2, 0);
        range.set(10, 8);
        range.set(15, 30);
        range.set(18, 2);
        range.set(25, 12);

        assertThat(range.topK(1, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(15));
        assertThat(range.topK(2, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(15, 25));
        assertThat(range.topK(3, range.isNotNull())).isEqualTo(RoaringBitmap.bitmapOf(10, 15, 25));
        assertThat(range.topK(4, range.isNotNull()))
                .isEqualTo(RoaringBitmap.bitmapOf(1, 10, 15, 25));
        assertThat(range.topK(5, range.isNotNull()))
                .isEqualTo(RoaringBitmap.bitmapOf(1, 10, 15, 18, 25));
        assertThat(range.topK(6, range.isNotNull()))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 1, 10, 15, 18, 25));
        assertThat(range.topK(7, range.isNotNull()))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 10, 15, 18, 25));

        assertThat(range.topK(1, RoaringBitmap.bitmapOf(0, 15)))
                .isEqualTo(RoaringBitmap.bitmapOf(15));
        assertThat(range.topK(1, RoaringBitmap.bitmapOf(0, 10, 25)))
                .isEqualTo(RoaringBitmap.bitmapOf(25));
        assertThat(range.topK(2, RoaringBitmap.bitmapOf(0, 2)))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 2));
        assertThat(range.topK(3, RoaringBitmap.bitmapOf(0, 10, 25, 15)))
                .isEqualTo(RoaringBitmap.bitmapOf(10, 15, 25));
    }

    @Test
    public void testSum() throws IOException {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap(1, 30);
        range.set(0, 1);
        range.set(1, 3);
        range.set(10, 8);
        range.set(15, 30);
        range.set(18, 2);
        range.set(25, 12);

        assertThat(range.sum(range.isNotNull())).isEqualTo(56);
        assertThat(range.sum(RoaringBitmap.bitmapOf(0))).isEqualTo(1);
        assertThat(range.sum(RoaringBitmap.bitmapOf(0, 12, 15))).isEqualTo(31);
        assertThat(range.sum(RoaringBitmap.bitmapOf(0, 15, 18))).isEqualTo(33);
        assertThat(range.sum(RoaringBitmap.bitmapOf(2, 18, 25))).isEqualTo(14);
    }

    @Test
    public void testGet() throws IOException {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap(1, 30);
        range.set(0, 1);
        range.set(1, 3);
        range.set(10, 8);
        range.set(15, 30);
        range.set(18, 2);
        range.set(25, 12);

        assertThat(range.get(0)).isEqualTo(1);
        assertThat(range.get(1)).isEqualTo(3);
        assertThat(range.get(2)).isEqualTo(null);
        assertThat(range.get(10)).isEqualTo(8);
        assertThat(range.get(15)).isEqualTo(30);
        assertThat(range.get(18)).isEqualTo(2);
        assertThat(range.get(25)).isEqualTo(12);
    }

    @Test
    public void testMinMax() throws IOException {
        BitSliceIndexBitmap range = new BitSliceIndexBitmap(1, 30);
        range.set(0, 1);
        range.set(1, 3);
        range.set(10, 8);
        range.set(15, 30);
        range.set(18, 2);
        range.set(25, 12);

        assertThat(range.min(range.isNotNull())).isEqualTo(1);
        assertThat(range.max(range.isNotNull())).isEqualTo(30);

        assertThat(range.min(RoaringBitmap.bitmapOf(2, 3, 18))).isEqualTo(2);
        assertThat(range.max(RoaringBitmap.bitmapOf(1, 2, 10, 18))).isEqualTo(8);
    }

    @Test
    public void test() {
        System.out.println(Long.toBinaryString(10));
        System.out.println(Long.numberOfLeadingZeros(10));
        System.out.println(Long.SIZE - Long.numberOfLeadingZeros(10));
    }
}
