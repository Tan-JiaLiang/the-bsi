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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ImmutableBitSliceIndexBitmapTest {

    public static final int NUM_OF_ROWS = 1000000;
    public static final int VALUE_BOUND = 100000;
    public static final long VALUE_LT_MIN = 0;
    public static final long VALUE_GT_MAX = VALUE_BOUND + 100;

    private Random random;
    private List<Pair> pairs;
    private BitSliceIndex bsi;

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
            pairs.add(new Pair(i, next));
        }

        ImmutableBitSliceIndexBitmap.Appender appender =
                new ImmutableBitSliceIndexBitmap.Appender();
        for (Pair pair : pairs) {
            if (pair.value == null) {
                continue;
            }
            appender.append(pair.index, pair.value);
        }
        ByteBuffer serialize = appender.serialize();
        this.bsi = ImmutableBitSliceIndexBitmap.map(ByteBuffer.wrap(serialize.array()));
        this.pairs = Collections.unmodifiableList(pairs);
    }

    @Test
    public void testEQ() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.eq(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> Objects.equals(x.value, predicate))
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.eq(predicate, foundSet))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> foundSet.contains(x.index))
                                    .filter(x -> Objects.equals(x.value, predicate))
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate out of the value bound
        assertThat(bsi.eq(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap());
        assertThat(bsi.eq(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap());
    }

    @Test
    public void testLT() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.lt(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value < predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.lt(predicate, foundSet))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> foundSet.contains(x.index))
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value < predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate out of the value bound
        assertThat(bsi.lt(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap());
        assertThat(bsi.lt(VALUE_GT_MAX)).isEqualTo(bsi.isNotNull());
    }

    @Test
    public void testLTE() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.lte(predicate).getCardinality())
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value <= predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2))
                                    .getCardinality());
        }

        // test predicate with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.lte(predicate, foundSet))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> foundSet.contains(x.index))
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value <= predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate out of the value bound
        assertThat(bsi.lte(VALUE_LT_MIN)).isEqualTo(new RoaringBitmap());
        assertThat(bsi.lte(VALUE_GT_MAX)).isEqualTo(bsi.isNotNull());
    }

    @Test
    public void testGT() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.gt(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value > predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.gt(predicate, foundSet))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> foundSet.contains(x.index))
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value > predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate out of the value bound
        assertThat(bsi.gt(VALUE_LT_MIN)).isEqualTo(bsi.isNotNull());
        assertThat(bsi.gt(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap());
    }

    @Test
    public void testGTE() {
        // test predicate in the value bound
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.gte(predicate))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value >= predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            long predicate = generateNextValue();
            assertThat(bsi.gte(predicate, foundSet))
                    .isEqualTo(
                            pairs.stream()
                                    .filter(x -> foundSet.contains(x.index))
                                    .filter(x -> x.value != null)
                                    .filter(x -> x.value >= predicate)
                                    .map(x -> x.index)
                                    .collect(
                                            RoaringBitmap::new,
                                            RoaringBitmap::add,
                                            (x1, x2) -> x1.or(x2)));
        }

        // test predicate out of the value bound
        assertThat(bsi.gte(VALUE_LT_MIN)).isEqualTo(bsi.isNotNull());
        assertThat(bsi.gte(VALUE_GT_MAX)).isEqualTo(new RoaringBitmap());
    }

    @Test
    public void testIsNotNull() {
        assertThat(bsi.isNotNull())
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
                            .min(Comparator.comparing(x -> x.value));
            assertThat(opt).isPresent();
            Long min = opt.get().value;
            assertThat(min).isNotNull();
            assertThat(bsi.min()).isEqualTo(min);
        }

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            Optional<Pair> opt =
                    pairs.stream()
                            .filter(x -> foundSet.contains(x.index))
                            .filter(x -> x.value != null)
                            .min(Comparator.comparing(x -> x.value));
            assertThat(opt).isPresent();
            Long min = opt.get().value;
            assertThat(min).isNotNull();
            assertThat(bsi.min(foundSet)).isEqualTo(min);
        }
    }

    @Test
    public void testMax() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            Optional<Pair> opt =
                    pairs.stream()
                            .filter(x -> x.value != null)
                            .max(Comparator.comparing(x -> x.value));
            assertThat(opt).isPresent();
            Long max = opt.get().value;
            assertThat(max).isNotNull();
            assertThat(bsi.max()).isEqualTo(max);
        }

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            Optional<Pair> opt =
                    pairs.stream()
                            .filter(x -> foundSet.contains(x.index))
                            .filter(x -> x.value != null)
                            .max(Comparator.comparing(x -> x.value));
            assertThat(opt).isPresent();
            Long max = opt.get().value;
            assertThat(max).isNotNull();
            assertThat(bsi.max(foundSet)).isEqualTo(max);
        }
    }

    @Test
    public void testSum() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            Optional<Long> sum =
                    pairs.stream().filter(x -> x.value != null).map(x -> x.value).reduce(Long::sum);
            assertThat(sum).isPresent();
            assertThat(bsi.sum()).isEqualTo(sum.get());
        }

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            Optional<Long> sum =
                    pairs.stream()
                            .filter(x -> foundSet.contains(x.index))
                            .filter(x -> x.value != null)
                            .map(x -> x.value)
                            .reduce(Long::sum);
            if (sum.isPresent()) {
                assertThat(bsi.sum(foundSet)).isEqualTo(sum.get());
            } else {
                assertThat(bsi.sum(foundSet)).isNull();
            }
        }
    }

    @Test
    public void testCount() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            long count = pairs.stream().filter(x -> x.value != null).count();
            assertThat(bsi.count()).isEqualTo(count);
        }

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            long count =
                    pairs.stream()
                            .filter(x -> foundSet.contains(x.index))
                            .filter(x -> x.value != null)
                            .count();
            assertThat(bsi.count(foundSet)).isEqualTo(count);
        }
    }

    @Test
    public void testTopK() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            int k = random.nextInt(10000);
            Set<Long> topK =
                    pairs.stream()
                            .filter(x -> x.value != null)
                            .sorted((x, y) -> -Long.compare(x.value, y.value))
                            .limit(k)
                            .map(x -> x.value)
                            .collect(Collectors.toSet());
            RoaringBitmap actual = bsi.topK(k);
            for (Integer index : actual) {
                assertThat(topK).contains(bsi.get(index));
            }
        }

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            int k = random.nextInt(10000);
            Set<Long> topK =
                    pairs.stream()
                            .filter(x -> foundSet.contains(x.index))
                            .filter(x -> x.value != null)
                            .sorted((x, y) -> -Long.compare(x.value, y.value))
                            .limit(k)
                            .map(x -> x.value)
                            .collect(Collectors.toSet());
            RoaringBitmap actual = bsi.topK(k, foundSet);
            for (Integer index : actual) {
                assertThat(topK).contains(bsi.get(index));
            }
        }
    }

    @Test
    public void testBottomK() {
        // test without found set
        for (int i = 0; i < 10; i++) {
            int k = random.nextInt(10000);
            Set<Long> bottomK =
                    pairs.stream()
                            .filter(x -> x.value != null)
                            .sorted(Comparator.comparingLong(x -> x.value))
                            .limit(k)
                            .map(x -> x.value)
                            .collect(Collectors.toSet());
            RoaringBitmap actual = bsi.bottomK(k);
            for (Integer index : actual) {
                assertThat(bottomK).contains(bsi.get(index));
            }
        }

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            int k = random.nextInt(10000);
            Set<Long> bottomK =
                    pairs.stream()
                            .filter(x -> foundSet.contains(x.index))
                            .filter(x -> x.value != null)
                            .sorted(Comparator.comparingLong(x -> x.value))
                            .limit(k)
                            .map(x -> x.value)
                            .collect(Collectors.toSet());
            RoaringBitmap actual = bsi.bottomK(k, foundSet);
            for (Integer index : actual) {
                assertThat(bottomK).contains(bsi.get(index));
            }
        }
    }

    @Test
    public void testCountDistinct() {
        // test without found set
        long cnt1 =
                pairs.stream().filter(x -> x.value != null).map(x -> x.value).distinct().count();
        assertThat(bsi.countDistinct()).isEqualTo(cnt1);

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        for (int i = 0; i < 10; i++) {
            long cnt2 =
                    pairs.stream()
                            .filter(x -> foundSet.contains(x.index))
                            .filter(x -> x.value != null)
                            .map(x -> x.value)
                            .distinct()
                            .count();
            assertThat(bsi.countDistinct(foundSet)).isEqualTo(cnt2);
        }
    }

    @Test
    public void testSumDistinct() {
        // test without found set
        long sum1 =
                pairs.stream()
                        .filter(x -> x.value != null)
                        .map(x -> x.value)
                        .distinct()
                        .reduce(0L, Long::sum);
        assertThat(bsi.sumDistinct()).isEqualTo(sum1);

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        long cnt2 =
                pairs.stream()
                        .filter(x -> foundSet.contains(x.index))
                        .filter(x -> x.value != null)
                        .map(x -> x.value)
                        .distinct()
                        .reduce(0L, Long::sum);
        assertThat(bsi.sumDistinct(foundSet)).isEqualTo(cnt2);
    }

    @Test
    public void testDistinct() {
        // test without found set
        RoaringBitmap dst1 = new RoaringBitmap();
        Set<Long> v1 = new HashSet<>();
        for (Pair pair : pairs) {
            if (pair.value == null || v1.contains(pair.value)) {
                continue;
            }
            v1.add(pair.value);
            dst1.add(pair.index);
        }
        assertThat(bsi.distinct()).isEqualTo(dst1);

        // test with found set
        RoaringBitmap foundSet = new RoaringBitmap();
        for (int i = 0; i < 10000; i++) {
            foundSet.add(random.nextInt(NUM_OF_ROWS));
        }
        RoaringBitmap dst2 = new RoaringBitmap();
        Set<Long> v2 = new HashSet<>();
        for (Pair pair : pairs) {
            if (!foundSet.contains(pair.index)) {
                continue;
            }
            if (pair.value == null || v2.contains(pair.value)) {
                continue;
            }
            v2.add(pair.value);
            dst2.add(pair.index);
        }
        assertThat(bsi.distinct(foundSet)).isEqualTo(dst2);
    }

    private int generateNextValue() {
        // return a value in the range [1, VALUE_BOUND)
        return random.nextInt(VALUE_BOUND) + 1;
    }

    private static class Pair {
        int index;
        Long value;

        public Pair(int index, Long value) {
            this.index = index;
            this.value = value;
        }
    }
}
