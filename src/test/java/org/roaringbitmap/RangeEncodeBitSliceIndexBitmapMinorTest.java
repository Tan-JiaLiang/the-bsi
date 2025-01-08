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

public class RangeEncodeBitSliceIndexBitmapMinorTest {

    @Test
    public void testLTE() throws IOException {
        RangeEncodeBitSliceIndexBitmap range = new RangeEncodeBitSliceIndexBitmap();
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
        RangeEncodeBitSliceIndexBitmap range = new RangeEncodeBitSliceIndexBitmap();
        range.set(0, 5);
        range.set(2, 7);
        range.set(5, 12);
        range.set(18, 13);

        ByteBuffer serialize = range.serialize();
        RangeEncodeBitSliceIndexBitmap bsi =
                new RangeEncodeBitSliceIndexBitmap(ByteBuffer.wrap(serialize.array()));

        RoaringBitmap[] slices = bsi.getSlices();
        long emptySliceMask = bsi.getEmptySliceMask();

        for (int i = 0; i < slices.length; i++) {
            long emptySliceBit = (emptySliceMask >> i) & 1;
            if (emptySliceBit == 0) {
                assertThat(slices[i].isEmpty()).isTrue();
            }
        }

        assertThat(range.lte(10)).isEqualTo(RoaringBitmap.bitmapOf(0, 2));
    }

    @Test
    public void testEQ() throws IOException {
        RangeEncodeBitSliceIndexBitmap range = new RangeEncodeBitSliceIndexBitmap(0, 30);
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
        RangeEncodeBitSliceIndexBitmap range = new RangeEncodeBitSliceIndexBitmap(0, 30);
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
        RangeEncodeBitSliceIndexBitmap range = new RangeEncodeBitSliceIndexBitmap(1, 30);
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
        RangeEncodeBitSliceIndexBitmap range = new RangeEncodeBitSliceIndexBitmap(1, 30);
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
    public void testValueAt() throws IOException {
        RangeEncodeBitSliceIndexBitmap range = new RangeEncodeBitSliceIndexBitmap(1, 30);
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
        RangeEncodeBitSliceIndexBitmap range = new RangeEncodeBitSliceIndexBitmap(1, 30);
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
