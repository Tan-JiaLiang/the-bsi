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
import org.roaringbitmap.factory.IntegerKeyFactory;
import org.roaringbitmap.fs.ByteArraySeekableStream;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class RangeEncodeBitSliceBitmapMinorTest {

    @Test
    public void testAll() throws IOException {
        IntegerKeyFactory factory = new IntegerKeyFactory();
        RangeEncodeBitSliceBitmap.Appender<Integer> appender =
                new RangeEncodeBitSliceBitmap.Appender<>(factory, 16);
        appender.append(1);
        appender.append(3);
        appender.append(1);
        appender.append(null);
        appender.append(2);
        appender.append(null);
        appender.append(null);
        appender.append(2);
        appender.append(5);

        byte[] serialize = appender.serialize();
        RangeEncodeBitSliceBitmap<Integer> range =
                RangeEncodeBitSliceBitmap.map(new ByteArraySeekableStream(serialize), 0, factory);

        assertThat(range.eq(1)).isEqualTo(RoaringBitmap.bitmapOf(0, 2));
        assertThat(range.eq(3)).isEqualTo(RoaringBitmap.bitmapOf(1));
        assertThat(range.eq(2)).isEqualTo(RoaringBitmap.bitmapOf(4, 7));
        assertThat(range.eq(5)).isEqualTo(RoaringBitmap.bitmapOf(8));

        assertThat(range.lt(1)).isEqualTo(RoaringBitmap.bitmapOf());
        assertThat(range.lt(2)).isEqualTo(RoaringBitmap.bitmapOf(0, 2));
        assertThat(range.lt(3)).isEqualTo(RoaringBitmap.bitmapOf(0, 2, 4, 7));
        assertThat(range.lt(5)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7));
        assertThat(range.lt(6)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));

        assertThat(range.lte(1)).isEqualTo(RoaringBitmap.bitmapOf(0, 2));
        assertThat(range.lte(2)).isEqualTo(RoaringBitmap.bitmapOf(0, 2, 4, 7));
        assertThat(range.lte(3)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7));
        assertThat(range.lte(5)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));

        assertThat(range.gt(0)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));
        assertThat(range.gt(1)).isEqualTo(RoaringBitmap.bitmapOf(1, 4, 7, 8));
        assertThat(range.gt(2)).isEqualTo(RoaringBitmap.bitmapOf(1, 8));
        assertThat(range.gt(3)).isEqualTo(RoaringBitmap.bitmapOf(8));
        assertThat(range.gt(5)).isEqualTo(RoaringBitmap.bitmapOf());

        assertThat(range.gte(1)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));
        assertThat(range.gte(2)).isEqualTo(RoaringBitmap.bitmapOf(1, 4, 7, 8));
        assertThat(range.gte(3)).isEqualTo(RoaringBitmap.bitmapOf(1, 8));
        assertThat(range.gte(5)).isEqualTo(RoaringBitmap.bitmapOf(8));

        assertThat(range.isNull()).isEqualTo(RoaringBitmap.bitmapOf(3, 5, 6));
        assertThat(range.isNotNull()).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));

        assertThat(range.topK(1)).isEqualTo(RoaringBitmap.bitmapOf(8));
        assertThat(range.topK(2)).isEqualTo(RoaringBitmap.bitmapOf(1, 8));
        assertThat(range.topK(3)).isEqualTo(RoaringBitmap.bitmapOf(1, 7, 8));
        assertThat(range.topK(4)).isEqualTo(RoaringBitmap.bitmapOf(1, 4, 7, 8));
        assertThat(range.topK(5)).isEqualTo(RoaringBitmap.bitmapOf(1, 2, 4, 7, 8));
        assertThat(range.topK(6)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));
        assertThat(range.topK(7)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));

        assertThat(range.bottomK(7)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));
        assertThat(range.bottomK(6)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7, 8));
        assertThat(range.bottomK(5)).isEqualTo(RoaringBitmap.bitmapOf(0, 1, 2, 4, 7));
        assertThat(range.bottomK(4)).isEqualTo(RoaringBitmap.bitmapOf(0, 2, 4, 7));
        assertThat(range.bottomK(3)).isEqualTo(RoaringBitmap.bitmapOf(0, 2, 7));
        assertThat(range.bottomK(2)).isEqualTo(RoaringBitmap.bitmapOf(0, 2));
        assertThat(range.bottomK(1)).isEqualTo(RoaringBitmap.bitmapOf(2));

        assertThat(range.max()).isEqualTo(5);
        assertThat(range.min()).isEqualTo(1);

        assertThat(range.count()).isEqualTo(9);
        assertThat(range.countNotNull()).isEqualTo(6);
    }

    @Test
    public void testEstimatedBlocks() {
        assertThat(RangeEncodeBitSliceBitmap.DictionaryBlock.estimatedBlocks(4 * 1024, 16 * 1024))
                .isEqualTo(1);
        assertThat(RangeEncodeBitSliceBitmap.DictionaryBlock.estimatedBlocks(16 * 1024, 16 * 1024))
                .isEqualTo(2);
        assertThat(RangeEncodeBitSliceBitmap.DictionaryBlock.estimatedBlocks(4 * 100000, 16 * 1024))
                .isEqualTo(25);
    }
}
