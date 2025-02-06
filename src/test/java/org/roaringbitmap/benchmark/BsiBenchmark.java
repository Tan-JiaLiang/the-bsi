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
import org.junit.jupiter.api.Test;
import org.roaringbitmap.BitSliceIndexBitmap;
import org.roaringbitmap.ImmutableBitSliceIndexBitmap;
import org.roaringbitmap.RangeBitmap;
import org.roaringbitmap.RangeEncodeBitSliceIndexBitmap;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.bsi.buffer.MutableBitSliceIndex;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class BsiBenchmark {

    private static final int ROW_COUNT = 1000000;
    private static final int MAX = 1000000000;

    private static final String BSI_PATH = "src/test/resources/data/bsi.txt";
    private static final String IMMUTABLE_BSI_PATH = "src/test/resources/data/immutable-bsi.txt";
    private static final String RE_BSI_PATH = "src/test/resources/data/re-bsi.txt";
    private static final String ROARING_BSI_PATH = "src/test/resources/data/roaring-bsi.txt";
    private static final String ROARING_RE_BSI_PATH = "src/test/resources/data/roaring-re-bsi.txt";

    @BeforeAll
    public static void setup() throws IOException {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        List<Integer> values = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < ROW_COUNT; i++) {
            int value = random.nextInt(100) + MAX;
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

        MutableBitSliceIndex bsi = new MutableBitSliceIndex(min, max);
        for (int i = 0; i < values.size(); i++) {
            bsi.setValue(i, values.get(i));
        }
        ByteBuffer b = ByteBuffer.allocate(bsi.serializedSizeInBytes());
        bsi.serialize(b);
        Files.write(new File(ROARING_BSI_PATH).toPath(), b.array());

        RangeBitmap.Appender appender = RangeBitmap.appender(max);
        for (Integer value : values) {
            appender.add(value);
        }
        ByteBuffer buffer = ByteBuffer.allocate(appender.serializedSizeInBytes());
        appender.serialize(buffer);
        RangeBitmap range = appender.build();
        Files.write(new File(ROARING_RE_BSI_PATH).toPath(), buffer.array());

        // testing
        for (int i = 0; i < 10; i++) {
            int next = random.nextInt(MAX);
            assertThat(reBsi.lte(next)).isEqualTo(range.lte(next));
            assertThat(reBsi.lte(next)).isEqualTo(bsi.rangeLE(null, next).toRoaringBitmap());
            assertThat(reBsi.lte(next, RoaringBitmap.bitmapOfRange(0, ROW_COUNT)))
                    .isEqualTo(range.lte(next));
        }
    }

    @AfterAll
    public static void after() throws IOException {
        Files.delete(new File(BSI_PATH).toPath());
        Files.delete(new File(IMMUTABLE_BSI_PATH).toPath());
        Files.delete(new File(RE_BSI_PATH).toPath());
        Files.delete(new File(ROARING_BSI_PATH).toPath());
        Files.delete(new File(ROARING_RE_BSI_PATH).toPath());
    }

    @Test
    public void testEQQuery() {
        int length = 30;
        Random random = new Random();
        int[] values = new int[length + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(100) + MAX;
        }
        values[length] = MAX + 101;

        Benchmark benchmark =
                new Benchmark("benchmark", length)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(false);

        benchmark.addCase(
                "roaring-bsi",
                10,
                () -> {
                    for (int value : values) {
                        File file = new File(ROARING_BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            ByteBuffer buffer = ByteBuffer.wrap(bytes);
                            MutableBitSliceIndex bsi = new MutableBitSliceIndex();
                            bsi.deserialize(buffer);
                            bsi.rangeEQ(null, value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "roaring-re-bsi",
                10,
                () -> {
                    for (int value : values) {
                        File file = new File(ROARING_RE_BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            RangeBitmap range = RangeBitmap.map(ByteBuffer.wrap(bytes));
                            range.eq(value, RoaringBitmap.bitmapOfRange(0, ROW_COUNT + 1));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "bsi",
                10,
                () -> {
                    for (int value : values) {
                        File file = new File(BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            BitSliceIndexBitmap bsi =
                                    new BitSliceIndexBitmap(ByteBuffer.wrap(bytes));
                            bsi.eq(value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "re-bsi",
                10,
                () -> {
                    for (int value : values) {
                        File file = new File(RE_BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            RangeEncodeBitSliceIndexBitmap bsi =
                                    new RangeEncodeBitSliceIndexBitmap(ByteBuffer.wrap(bytes));
                            bsi.eq(value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "immutable-bsi",
                10,
                () -> {
                    for (int value : values) {
                        File file = new File(IMMUTABLE_BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            ByteBuffer buffer = ByteBuffer.wrap(bytes);
                            ImmutableBitSliceIndexBitmap bsi =
                                    ImmutableBitSliceIndexBitmap.map(buffer);
                            bsi.eq(value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.run();
    }

    @Test
    public void testRangeQuery() {
        int length = 30;
        Random random = new Random();
        int[] values = new int[length + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(100) + MAX;
        }
        values[length] = MAX + 101;

        Benchmark benchmark =
                new Benchmark("benchmark", length)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(false);

        benchmark.addCase(
                "roaring-bsi",
                10,
                () -> {
                    for (int value : values) {
                        File file = new File(ROARING_BSI_PATH);
                        try (BufferedInputStream stream =
                                new BufferedInputStream(Files.newInputStream(file.toPath()))) {
                            byte[] bytes = new byte[(int) file.length()];
                            stream.read(bytes);
                            ByteBuffer buffer = ByteBuffer.wrap(bytes);
                            MutableBitSliceIndex bsi = new MutableBitSliceIndex();
                            bsi.deserialize(buffer);
                            bsi.rangeGE(null, value);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

        benchmark.addCase(
                "roaring-re-bsi",
                10,
                () -> {
                    for (int value : values) {
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
                10,
                () -> {
                    for (int value : values) {
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
                10,
                () -> {
                    for (int value : values) {
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
                10,
                () -> {
                    for (int value : values) {
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

        benchmark.run();
    }
}
