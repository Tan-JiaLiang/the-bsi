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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.factory.StringKeyFactory;
import org.roaringbitmap.fs.ByteArraySeekableStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class RangeEncodeBitSliceBitmapStringTypeTest {

    public static final int NUM_OF_ROWS = 1000000;
    public static final int VALUE_BOUND = 100000;
    public static final String VALUE_LT_MIN = "/";
    public static final String VALUE_GT_MAX = "|";
    private final String BASE = "skdjfslfi-";

    private Random random;
    private List<Pair> pairs;
    private RangeEncodeBitSliceBitmap<String> range;
    private Comparator<String> comparator;

    @BeforeEach
    public void setup() throws IOException {
        this.random = new Random();
        List<Pair> pairs = new ArrayList<>();
        long min = 0;
        long max = 0;
        for (int i = 0; i < NUM_OF_ROWS; i++) {
            if (i % 5 == 0) {
                pairs.add(new Pair(i, null));
                continue;
            }
            long next = generateNextValue();
            min = Math.min(min == 0 ? next : min, next);
            max = Math.max(max == 0 ? next : max, next);
            pairs.add(new Pair(i, BASE + next));
        }

        StringKeyFactory factory = new StringKeyFactory();
        RangeEncodeBitSliceBitmap.Appender<String> appender =
                new RangeEncodeBitSliceBitmap.Appender<>(factory, 16 * 1024);
        for (Pair pair : pairs) {
            appender.append(pair.value);
        }
        this.comparator = factory.createCompactor();
        this.range =
                RangeEncodeBitSliceBitmap.map(
                        new ByteArraySeekableStream(appender.serialize()), 0, factory);
        this.pairs = Collections.unmodifiableList(pairs);
    }

    @Test
    public void testEQ() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            String predicate = BASE + generateNextValue();
            assertThat(range.eq(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> Objects.equals(x.value, predicate))
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        //        // test predicate with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            String predicate = BASE + generateNextValue();
        //            assertThat(range.eq(predicate, foundSet))
        //                    .isEqualTo(
        //                            pairs.stream()
        //                                    .filter(x -> foundSet.contains(x.index))
        //                                    .filter(x -> Objects.equals(x.value, predicate))
        //                                    .map(x -> x.index)
        //                                    .collect(
        //                                            RoaringBitmap::new,
        //                                            RoaringBitmap::add,
        //                                            (x1, x2) -> x1.or(x2)));
        //        }
        //
        // test predicate out of the value bound
        assertThat(range.eq(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap());
        assertThat(range.eq(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap());
    }

    @Test
    public void testLT() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            String predicate = BASE + generateNextValue();
            assertThat(range.lt(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> comparator.compare(x.value, predicate) < 0)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        //        // test predicate with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            long predicate = generateNextValue();
        //            assertThat(range.lt(predicate, foundSet))
        //                    .isEqualTo(
        //                            pairs.stream()
        //                                    .filter(x -> foundSet.contains(x.index))
        //                                    .filter(x -> x.value != null)
        //                                    .filter(x -> x.value < predicate)
        //                                    .map(x -> x.index)
        //                                    .collect(
        //                                            RoaringBitmap::new,
        //                                            RoaringBitmap::add,
        //                                            (x1, x2) -> x1.or(x2)));
        //        }
        //
        // test predicate out of the value bound
        assertThat(range.lt(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap());
        assertThat(range.lt(VALUE_GT_MAX))
                .isEqualTo(
                        pairs.stream()
                                .filter(x -> x.value != null)
                                .map(x -> x.index)
                                .collect(
                                        RoaringBitmap::new,
                                        RoaringBitmap::add,
                                        (x1, x2) -> x1.or(x2)));
    }

    @Test
    public void testLTE() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            String predicate = BASE + generateNextValue();
            assertThat(range.lte(predicate).getCardinality())
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> comparator.compare(x.value, predicate) <= 0)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2))
                                    .getCardinality());
        }

        //        // test predicate with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            long predicate = generateNextValue();
        //            assertThat(range.lte(predicate, foundSet))
        //                    .isEqualTo(
        //                            pairs.stream()
        //                                    .filter(x -> foundSet.contains(x.index))
        //                                    .filter(x -> x.value != null)
        //                                    .filter(x -> x.value <= predicate)
        //                                    .map(x -> x.index)
        //                                    .collect(
        //                                            RoaringBitmap::new,
        //                                            RoaringBitmap::add,
        //                                            (x1, x2) -> x1.or(x2)));
        //        }
        //
        // test predicate out of the value bound
        assertThat(range.lte(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap());
        assertThat(range.lte(VALUE_GT_MAX))
                .isEqualTo(
                        pairs.stream()
                                .filter(x -> x.value != null)
                                .map(x -> x.index)
                                .collect(
                                        RoaringBitmap::new,
                                        RoaringBitmap::add,
                                        (x1, x2) -> x1.or(x2)));
    }

    @Test
    public void testGT() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            String predicate = BASE + generateNextValue();
            assertThat(range.gt(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> comparator.compare(x.value, predicate) > 0)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        //        // test predicate with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            long predicate = generateNextValue();
        //            assertThat(range.gt(predicate, foundSet))
        //                    .isEqualTo(
        //                            pairs.stream()
        //                                    .filter(x -> foundSet.contains(x.index))
        //                                    .filter(x -> x.value != null)
        //                                    .filter(x -> x.value > predicate)
        //                                    .map(x -> x.index)
        //                                    .collect(
        //                                            RoaringBitmap::new,
        //                                            RoaringBitmap::add,
        //                                            (x1, x2) -> x1.or(x2)));
        //        }
        //
        // test predicate out of the value bound
        assertThat(range.gt(VALUE_LT_MIN))
                .isEqualTo(
                        pairs.stream()
                                .filter(x -> x.value != null)
                                .map(x -> x.index)
                                .collect(
                                        RoaringBitmap::new,
                                        RoaringBitmap::add,
                                        (x1, x2) -> x1.or(x2)));
        assertThat(range.gt(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap());
    }

    @Test
    public void testGTE() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            String predicate = BASE + generateNextValue();
            assertThat(range.gte(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> comparator.compare(x.value, predicate) >= 0)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        //        // test predicate with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            long predicate = generateNextValue();
        //            assertThat(range.gte(predicate, foundSet))
        //                    .isEqualTo(
        //                            pairs.stream()
        //                                    .filter(x -> foundSet.contains(x.index))
        //                                    .filter(x -> x.value != null)
        //                                    .filter(x -> x.value >= predicate)
        //                                    .map(x -> x.index)
        //                                    .collect(
        //                                            RoaringBitmap::new,
        //                                            RoaringBitmap::add,
        //                                            (x1, x2) -> x1.or(x2)));
        //        }
        //
        // test predicate out of the value bound
        assertThat(range.gte(VALUE_LT_MIN))
                .isEqualTo(
                        pairs.stream()
                                .filter(x -> x.value != null)
                                .map(x -> x.index)
                                .collect(
                                        RoaringBitmap::new,
                                        RoaringBitmap::add,
                                        (x1, x2) -> x1.or(x2)));
        assertThat(range.gte(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap());
    }

    @Test
    public void testIsNotNull() {
        assertThat(range.isNotNull())
                .isEqualTo(
                        pairs.stream()
                                .filter(x -> x.value != null)
                                .map(x -> x.index)
                                .collect(
                                        RoaringBitmap::new,
                                        RoaringBitmap::add,
                                        (x1, x2) -> x1.or(x2)));
    }

    @Test
    public void testMin() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            Optional<Pair> opt =
                    pairs.stream()
                            .filter(x -> x.value != null)
                            .min(Comparator.comparing(x -> x.value, comparator));
            assertThat(opt).isPresent();
            String min = opt.get().value;
            assertThat(min).isNotNull();
            assertThat(range.min()).isEqualTo(min);
        }

        //        // test with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            Optional<Pair> opt =
        //                    pairs.stream()
        //                            .filter(x -> foundSet.contains(x.index))
        //                            .filter(x -> x.value != null)
        //                            .min(Comparator.comparing(x -> x.value));
        //            assertThat(opt).isPresent();
        //            Long min = opt.get().value;
        //            assertThat(min).isNotNull();
        //            assertThat(range.min(foundSet)).isEqualTo(min);
        //        }
    }

    @Test
    public void testMax() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            Optional<Pair> opt =
                    pairs.stream()
                            .filter(x -> x.value != null)
                            .max(Comparator.comparing(x -> x.value, comparator));
            assertThat(opt).isPresent();
            String max = opt.get().value;
            assertThat(max).isNotNull();
            assertThat(range.max()).isEqualTo(max);
        }

        //        // test with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            Optional<Pair> opt =
        //                    pairs.stream()
        //                            .filter(x -> foundSet.contains(x.index))
        //                            .filter(x -> x.value != null)
        //                            .max(Comparator.comparing(x -> x.value));
        //            assertThat(opt).isPresent();
        //            Long max = opt.get().value;
        //            assertThat(max).isNotNull();
        //            assertThat(range.max(foundSet)).isEqualTo(max);
        //        }
    }

    @Test
    public void testCountNotNull() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            long count = pairs.stream().filter(x -> x.value != null).count();
            assertThat(range.countNotNull()).isEqualTo(count);
        }

        //        // test with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            long count =
        //                    pairs.stream()
        //                            .filter(x -> foundSet.contains(x.index))
        //                            .filter(x -> x.value != null)
        //                            .count();
        //            assertThat(range.countNotNull(foundSet)).isEqualTo(count);
        //        }
    }

    @Test
    public void testTopK() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            int k = random.nextInt(10);
            Set<String> topK =
                    pairs.stream()
                            .filter(x -> x.value != null)
                            .sorted((x, y) -> -comparator.compare(x.value, y.value))
                            .limit(k)
                            .map(x -> x.value)
                            .collect(Collectors.toSet());
            RoaringBitmap actual = range.topK(k);
            for (Integer index : actual) {
                assertThat(topK).contains(range.get(index));
            }
        }

        //        // test with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            int k = random.nextInt(10000);
        //            Set<Long> topK =
        //                    pairs.stream()
        //                            .filter(x -> foundSet.contains(x.index))
        //                            .filter(x -> x.value != null)
        //                            .sorted((x, y) -> -Long.compare(x.value, y.value))
        //                            .limit(k)
        //                            .map(x -> x.value)
        //                            .collect(Collectors.toSet());
        //            RoaringBitmap actual = range.topK(k, foundSet);
        //            for (Integer index : actual) {
        //                assertThat(topK).contains(range.get(index));
        //            }
        //        }
    }

    @Test
    public void testBottomK() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            int k = random.nextInt(10000);
            Set<String> bottomK =
                    pairs.stream()
                            .filter(x -> x.value != null)
                            .sorted((x, y) -> comparator.compare(x.value, y.value))
                            .limit(k)
                            .map(x -> x.value)
                            .collect(Collectors.toSet());
            RoaringBitmap actual = range.bottomK(k);
            for (Integer index : actual) {
                assertThat(bottomK).contains(range.get(index));
            }
        }

        //         test with found set
        //        RoaringBitmap foundSet = new RoaringBitmap();
        //        for (int i = 0; i < 10000; i++) {
        //            foundSet.add(random.nextInt(NUM_OF_ROWS));
        //        }
        //        for (int i = 0; i < 10; i++) {
        //            int k = random.nextInt(10000);
        //            Set<Long> bottomK =
        //                    pairs.stream()
        //                            .filter(x -> foundSet.contains(x.index))
        //                            .filter(x -> x.value != null)
        //                            .sorted(Comparator.comparingLong(x -> x.value))
        //                            .limit(k)
        //                            .map(x -> x.value)
        //                            .collect(Collectors.toSet());
        //            RoaringBitmap actual = range.bottomK(k, foundSet);
        //            for (Integer index : actual) {
        //                assertThat(bottomK).contains(range.get(index));
        //            }
        //        }
    }

    private int generateNextValue() {
        // return a value in the range [1, VALUE_BOUND)
        return random.nextInt(VALUE_BOUND) + 1;
    }

    private static class Pair {
        int index;
        String value;

        public Pair(int index, String value) {
            this.index = index;
            this.value = value;
        }
    }
}
