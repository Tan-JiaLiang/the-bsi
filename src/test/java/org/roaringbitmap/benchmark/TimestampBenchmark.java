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

package org.roaringbitmap.benchmark;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.roaringbitmap.BitSliceIndexBitmap;
import org.roaringbitmap.ImmutableBitSliceIndexBitmap;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RangeEncodeBitSliceBitmap;
import org.roaringbitmap.RangeEncodeBitSliceIndexBitmap;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.factory.LongKeyFactory;
import org.roaringbitmap.fs.LocalSeekableInputStream;
import org.roaringbitmap.fs.SeekableInputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TimestampBenchmark {

    private static final int ROW_COUNT = 1000000;
    private static final long BASE = System.currentTimeMillis() / 1000 * 1000;

    private static final String BSI_PATH = "src/test/resources/data/bsi.txt";
    private static final String IMMUTABLE_BSI_PATH = "src/test/resources/data/immutable-bsi.txt";
    private static final String RE_BSI_PATH = "src/test/resources/data/re-bsi.txt";
    private static final String ROARING_BSI_PATH = "src/test/resources/data/roaring-bsi.txt";
    private static final String ROARING_RE_BSI_PATH = "src/test/resources/data/roaring-re-bsi.txt";
    private static final String RANGE_BITMAP_PATH = "src/test/resources/data/range-bitmap.txt";

    @BeforeAll
    public static void setup() throws IOException {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        List<Long> values = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < ROW_COUNT; i++) {
            long value = BASE + (random.nextInt(180 * 1000) / 1000 * 1000);
            min = Math.min(min, value);
            max = Math.max(max, value);
            values.add(value);
        }

        RangeEncodeBitSliceIndexBitmap reBsi = new RangeEncodeBitSliceIndexBitmap(min, max);
        for (int i = 0; i < values.size(); i++) {
            reBsi.set(i, values.get(i));
        }
        Files.write(new File(RE_BSI_PATH).toPath(), reBsi.serialize().array());

        BitSliceIndexBitmap bitmap = new BitSliceIndexBitmap(min, max);
        for (int i = 0; i < values.size(); i++) {
            bitmap.set(i, values.get(i));
        }
        Files.write(new File(BSI_PATH).toPath(), bitmap.serialize().array());

        ImmutableBitSliceIndexBitmap.Appender app = new ImmutableBitSliceIndexBitmap.Appender();
        for (int i = 0; i < values.size(); i++) {
            app.append(i, values.get(i));
        }
        Files.write(new File(IMMUTABLE_BSI_PATH).toPath(), app.serialize().array());

        RangeBitmap.Appender appender = RangeBitmap.appender(max);
        for (Long value : values) {
            appender.add(value);
        }
        ByteBuffer buffer = ByteBuffer.allocate(appender.serializedSizeInBytes());
        appender.serialize(buffer);
        Files.write(new File(ROARING_RE_BSI_PATH).toPath(), buffer.array());

        RangeEncodeBitSliceBitmap.Appender<Long> rangeAppender =
                new RangeEncodeBitSliceBitmap.Appender<>(new LongKeyFactory(), Long.BYTES * 1024);
        for (Long value : values) {
            rangeAppender.append(value);
        }
        Files.write(new File(RANGE_BITMAP_PATH).toPath(), rangeAppender.serialize());
    }

    @AfterAll
    public static void after() throws IOException {
        System.out.printf("%s size is %s%n", BSI_PATH, new File(BSI_PATH).length());
        System.out.printf(
                "%s size is %s%n", IMMUTABLE_BSI_PATH, new File(IMMUTABLE_BSI_PATH).length());
        System.out.printf("%s size is %s%n", RE_BSI_PATH, new File(RE_BSI_PATH).length());
        System.out.printf("%s size is %s%n", ROARING_BSI_PATH, new File(ROARING_BSI_PATH).length());
        System.out.printf(
                "%s size is %s%n", ROARING_RE_BSI_PATH, new File(ROARING_RE_BSI_PATH).length());
        System.out.printf(
                "%s size is %s%n", RANGE_BITMAP_PATH, new File(RANGE_BITMAP_PATH).length());

        Files.delete(new File(BSI_PATH).toPath());
        Files.delete(new File(IMMUTABLE_BSI_PATH).toPath());
        Files.delete(new File(RE_BSI_PATH).toPath());
        Files.delete(new File(ROARING_RE_BSI_PATH).toPath());
        Files.delete(new File(RANGE_BITMAP_PATH).toPath());
    }

    @RepeatedTest(10)
    public void testRangeQuery() {
        int length = 1;
        Random random = new Random();
        long[] values = new long[length + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = BASE + (random.nextInt(180 * 1000) / 1000 * 1000);
        }

        Benchmark benchmark =
                new Benchmark("benchmark", length)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(false);

        benchmark.addCase(
                "roaring-re-bsi",
                30,
                () -> {
                    for (long value : values) {
                        File file = new File(ROARING_RE_BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            RangeBitmap range = RangeBitmap.map(ByteBuffer.wrap(bytes));
                            range.lte(value, RoaringBitmap.bitmapOfRange(0, ROW_COUNT + 1));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "bsi",
                30,
                () -> {
                    for (long value : values) {
                        File file = new File(BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            BitSliceIndexBitmap bsi =
                                    new BitSliceIndexBitmap(ByteBuffer.wrap(bytes));
                            bsi.gt(value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "re-bsi",
                30,
                () -> {
                    for (long value : values) {
                        File file = new File(RE_BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            RangeEncodeBitSliceIndexBitmap bsi =
                                    new RangeEncodeBitSliceIndexBitmap(ByteBuffer.wrap(bytes));
                            bsi.lte(value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "immutable-bsi",
                30,
                () -> {
                    for (long value : values) {
                        File file = new File(IMMUTABLE_BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            ByteBuffer buffer = ByteBuffer.wrap(bytes);
                            ImmutableBitSliceIndexBitmap bsi =
                                    ImmutableBitSliceIndexBitmap.map(buffer);
                            bsi.gt(value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "range-bitmap",
                10,
                () -> {
                    for (long value : values) {
                        File file = new File(RANGE_BITMAP_PATH);

                        try (SeekableInputStream stream = new LocalSeekableInputStream(file)) {
                            RangeEncodeBitSliceBitmap<Long> bitmap =
                                    RangeEncodeBitSliceBitmap.map(stream, 0, new LongKeyFactory());
                            bitmap.gt(value);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.run();
    }
}
