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

public class ImmutableBitSliceIndexBitmapMinorTest {

    @Test
    public void testTimeMillis() {
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1736907560000L);
        appender.append(1, 1736907460000L);
        appender.append(5, 1736908460000L);
        appender.append(6, 1736902450000L);
        appender.append(9, 1736902410000L);
        appender.append(10, 1736903480000L);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap bsi = ImmutableBitSliceIndexBitmap.map(buffer);

        assertThat(bsi.get(0)).isEqualTo(1736907560000L);
        assertThat(bsi.get(1)).isEqualTo(1736907460000L);
        assertThat(bsi.get(5)).isEqualTo(1736908460000L);
        assertThat(bsi.get(6)).isEqualTo(1736902450000L);
        assertThat(bsi.get(9)).isEqualTo(1736902410000L);
        assertThat(bsi.get(10)).isEqualTo(1736903480000L);

        assertThat(bsi.eq(1736907560000L)).isEqualTo(RoaringBitmap.bitmapOf(0));
        assertThat(bsi.eq(1736902410000L)).isEqualTo(RoaringBitmap.bitmapOf(9));
        assertThat(bsi.eq(1736902410001L)).isEqualTo(RoaringBitmap.bitmapOf());
        assertThat(bsi.gt(1736903410000L)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 5, 10));
        assertThat(bsi.lte(1736903410000L)).isEqualTo(RoaringBitmap.bitmapOf(6, 9));
    }

    @Test
    public void testGT() {
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1);
        appender.append(1, 3);
        appender.append(2, 0);
        appender.append(10, 8);
        appender.append(15, 30);
        appender.append(18, 2);
        appender.append(25, 12);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap range = ImmutableBitSliceIndexBitmap.map(buffer);

        assertThat(range.gt(3)).isEqualTo(RoaringBitmap.bitmapOf(10, 15, 25));
        assertThat(range.gt(3, RoaringBitmap.bitmapOf(0, 1, 2, 18, 25)))
                .isEqualTo(RoaringBitmap.bitmapOf(25));
    }

    @Test
    public void testLTE() throws IOException {
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1);
        appender.append(1, 3);
        appender.append(2, 0);
        appender.append(10, 8);
        appender.append(15, 30);
        appender.append(18, 2);
        appender.append(25, 12);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap range = ImmutableBitSliceIndexBitmap.map(buffer);

        assertThat(range.lte(3)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 18));
        assertThat(range.lte(10, RoaringBitmap.bitmapOf(0, 1, 2, 18, 25)))
                .isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 18));
    }

    @Test
    public void testEQ() throws IOException {
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1);
        appender.append(1, 3);
        appender.append(2, 0);
        appender.append(10, 8);
        appender.append(15, 30);
        appender.append(18, 2);
        appender.append(25, 12);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap range = ImmutableBitSliceIndexBitmap.map(buffer);

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
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1);
        appender.append(1, 3);
        appender.append(2, 0);
        appender.append(10, 8);
        appender.append(15, 30);
        appender.append(18, 2);
        appender.append(25, 12);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap range = ImmutableBitSliceIndexBitmap.map(buffer);

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
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1);
        appender.append(1, 3);
        appender.append(2, 0);
        appender.append(10, 8);
        appender.append(15, 30);
        appender.append(18, 2);
        appender.append(25, 12);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap range = ImmutableBitSliceIndexBitmap.map(buffer);

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
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1);
        appender.append(1, 3);
        appender.append(2, 0);
        appender.append(10, 8);
        appender.append(15, 30);
        appender.append(18, 2);
        appender.append(25, 12);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap range = ImmutableBitSliceIndexBitmap.map(buffer);

        assertThat(range.sum(range.isNotNull())).isEqualTo(56);
        assertThat(range.sum(RoaringBitmap.bitmapOf(0))).isEqualTo(1);
        assertThat(range.sum(RoaringBitmap.bitmapOf(0, 12, 15))).isEqualTo(31);
        assertThat(range.sum(RoaringBitmap.bitmapOf(0, 15, 18))).isEqualTo(33);
        assertThat(range.sum(RoaringBitmap.bitmapOf(2, 18, 25))).isEqualTo(14);
    }

    @Test
    public void testGet() throws IOException {
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1);
        appender.append(1, 3);
        appender.append(2, 0);
        appender.append(10, 8);
        appender.append(15, 30);
        appender.append(18, 2);
        appender.append(25, 12);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap range = ImmutableBitSliceIndexBitmap.map(buffer);

        assertThat(range.get(0)).isEqualTo(1);
        assertThat(range.get(1)).isEqualTo(3);
        assertThat(range.get(2)).isEqualTo(0);
        assertThat(range.get(3)).isEqualTo(null);
        assertThat(range.get(10)).isEqualTo(8);
        assertThat(range.get(15)).isEqualTo(30);
        assertThat(range.get(18)).isEqualTo(2);
        assertThat(range.get(25)).isEqualTo(12);
    }

    @Test
    public void testMinMax() throws IOException {
        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        appender.append(0, 1);
        appender.append(1, 3);
        appender.append(2, 0);
        appender.append(10, 8);
        appender.append(15, 30);
        appender.append(18, 2);
        appender.append(25, 12);

        ByteBuffer buffer = appender.serialize();
        buffer.flip();
        ImmutableBitSliceIndexBitmap range = ImmutableBitSliceIndexBitmap.map(buffer);

        assertThat(range.min(range.isNotNull())).isEqualTo(0);
        assertThat(range.max(range.isNotNull())).isEqualTo(30);

        assertThat(range.min(RoaringBitmap.bitmapOf(2, 3, 18))).isEqualTo(0);
        assertThat(range.max(RoaringBitmap.bitmapOf(1, 2, 10, 18))).isEqualTo(8);
    }

    @Test
    public void test() {
        System.out.println(Long.toBinaryString(10));
        System.out.println(Long.numberOfLeadingZeros(10));
        System.out.println(Long.SIZE - Long.numberOfLeadingZeros(10));
    }

    @Test
    public void testMerge() {
        ImmutableBitSliceIndexBitmap.Appender ap1 = new ImmutableBitSliceIndexBitmap.Appender();
        ap1.append(15, 30);
        ap1.append(18, 2);
        ap1.append(25, 12);

        ByteBuffer buf1 = ap1.serialize();
        buf1.flip();
        ImmutableBitSliceIndexBitmap bsi1 = ImmutableBitSliceIndexBitmap.map(buf1);

        ImmutableBitSliceIndexBitmap.Appender ap2 = new ImmutableBitSliceIndexBitmap.Appender();
        ap2.append(0, 1);
        ap2.append(1, 3);
        ap2.append(2, 0);
        ap2.append(10, 8);

        ByteBuffer buf2 = ap2.serialize();
        buf2.flip();
        ImmutableBitSliceIndexBitmap bsi2 = ImmutableBitSliceIndexBitmap.map(buf2);

        bsi1.merge(bsi2);

        assertThat(bsi1.get(15)).isEqualTo(30);
        assertThat(bsi1.get(18)).isEqualTo(2);
        assertThat(bsi1.get(25)).isEqualTo(12);
        assertThat(bsi1.get(0)).isEqualTo(1);
        assertThat(bsi1.get(1)).isEqualTo(3);
        assertThat(bsi1.get(2)).isEqualTo(0);
        assertThat(bsi1.get(10)).isEqualTo(8);
    }

    @Test
    public void testMerge2() {
        ImmutableBitSliceIndexBitmap.Appender ap1 = new ImmutableBitSliceIndexBitmap.Appender();
        ap1.append(0, 1);
        ap1.append(1, 15);
        ap1.append(10, 5);
        ap1.append(15, 30);
        ap1.append(18, 2);
        ap1.append(25, 12);

        ByteBuffer buf1 = ap1.serialize();
        buf1.flip();
        ImmutableBitSliceIndexBitmap bsi1 = ImmutableBitSliceIndexBitmap.map(buf1);

        ImmutableBitSliceIndexBitmap.Appender ap2 = new ImmutableBitSliceIndexBitmap.Appender();
        ap2.append(0, 1);
        ap2.append(1, 3);
        ap2.append(2, 0);
        ap2.append(10, 8);

        ByteBuffer buf2 = ap2.serialize();
        buf2.flip();
        ImmutableBitSliceIndexBitmap bsi2 = ImmutableBitSliceIndexBitmap.map(buf2);

        bsi2.merge(bsi1);

        assertThat(bsi2.get(15)).isEqualTo(30);
        assertThat(bsi2.get(18)).isEqualTo(2);
        assertThat(bsi2.get(25)).isEqualTo(12);
        assertThat(bsi2.get(0)).isEqualTo(1);
        assertThat(bsi2.get(1)).isEqualTo(15);
        assertThat(bsi2.get(2)).isEqualTo(0);
        assertThat(bsi2.get(10)).isEqualTo(5);
    }
}
