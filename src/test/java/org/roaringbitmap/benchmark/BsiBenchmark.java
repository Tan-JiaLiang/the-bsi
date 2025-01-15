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

    private static final int ROW_COUNT = 3000000;
    private static final int MIN = 0;
    private static final int MAX = 300000;

    private static final String RE_BSI_PATH = "src/test/resources/data/re_bsi.txt";
    private static final String BSI_PATH = "src/test/resources/data/bsi.txt";
    private static final String RANGE_PATH = "src/test/resources/data/range.txt";

    @BeforeAll
    public static void setup() throws IOException {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        List<Integer> values = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < ROW_COUNT; i++) {
            int value = random.nextInt(MAX);
            min = Math.min(min, value);
            max = Math.max(max, value);
            values.add(value);
        }

        RangeEncodeBitSliceIndexBitmap reBsi = new RangeEncodeBitSliceIndexBitmap(MIN, MAX);
        for (int i = 0; i < values.size(); i++) {
            reBsi.set(i, values.get(i));
        }
        Files.write(new File(RE_BSI_PATH).toPath(), reBsi.serialize().array());

        MutableBitSliceIndex bsi = new MutableBitSliceIndex(MIN, MAX);
        for (int i = 0; i < values.size(); i++) {
            bsi.setValue(i, values.get(i));
        }
        ByteBuffer b = ByteBuffer.allocate(bsi.serializedSizeInBytes());
        bsi.serialize(b);
        Files.write(new File(BSI_PATH).toPath(), b.array());

        RangeBitmap.Appender appender = RangeBitmap.appender(MAX);
        for (Integer value : values) {
            appender.add(value);
        }
        ByteBuffer buffer = ByteBuffer.allocate(appender.serializedSizeInBytes());
        appender.serialize(buffer);
        RangeBitmap range = appender.build();
        Files.write(new File(RANGE_PATH).toPath(), buffer.array());

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
        Files.delete(new File(RE_BSI_PATH).toPath());
        Files.delete(new File(BSI_PATH).toPath());
        Files.delete(new File(RANGE_PATH).toPath());
    }

    @Test
    public void testLTE() {
        int read = 100;
        int length = 30;
        Random random = new Random();
        int[] values = new int[length + 1];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(MAX);
        }
        values[length] = MAX + 1;

        for (int value : values) {
            Benchmark benchmark =
                    new Benchmark("benchmark", read * length)
                            .setNumWarmupIters(1)
                            .setOutputPerIteration(false);

            benchmark.addCase(
                    "bsi-" + value,
                    3,
                    () -> {
                        for (int i = 0; i < read; i++) {
                            File file = new File(BSI_PATH);
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
                    "re-bsi-" + value,
                    3,
                    () -> {
                        for (int i = 0; i < read; i++) {
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
                    "range-" + value,
                    3,
                    () -> {
                        for (int i = 0; i < read; i++) {
                            File file = new File(RANGE_PATH);
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

            benchmark.run();
        }
    }
}
