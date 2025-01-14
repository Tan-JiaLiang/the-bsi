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

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

public interface BitSliceIndexBitmap {

    void set(int key, long value);

    @Nullable
    Long get(int key);

    boolean exists(int key);

    int bitCount();

    void merge(BitSliceIndexBitmap other);

    ByteBuffer serialize();

    RoaringBitmap eq(long predicate, @Nullable RoaringBitmap foundSet);

    RoaringBitmap lte(long predicate, @Nullable RoaringBitmap foundSet);

    RoaringBitmap gt(long predicate, @Nullable RoaringBitmap foundSet);

    RoaringBitmap isNotNull(@Nullable RoaringBitmap foundSet);

    RoaringBitmap topK(int k, @Nullable RoaringBitmap foundSet);

    RoaringBitmap bottomK(int k, @Nullable RoaringBitmap foundSet);

    @Nullable
    Long min(@Nullable RoaringBitmap foundSet);

    @Nullable
    Long max(@Nullable RoaringBitmap foundSet);

    @Nullable
    Long sum(@Nullable RoaringBitmap foundSet);

    Long count(@Nullable RoaringBitmap foundSet);

    default RoaringBitmap lt(long predicate, @Nullable RoaringBitmap foundSet) {
        return lte(predicate - 1, foundSet);
    }

    default RoaringBitmap gte(long predicate, @Nullable RoaringBitmap foundSet) {
        return gt(predicate - 1, foundSet);
    }

    default RoaringBitmap lte(long predicate) {
        return lte(predicate, null);
    }

    default RoaringBitmap gte(long predicate) {
        return gte(predicate, null);
    }

    default RoaringBitmap eq(long predicate) {
        return eq(predicate, null);
    }

    default RoaringBitmap lt(long predicate) {
        return lt(predicate, null);
    }

    default RoaringBitmap gt(long predicate) {
        return gt(predicate, null);
    }

    default RoaringBitmap isNotNull() {
        return isNotNull(null);
    }

    default RoaringBitmap topK(int k) {
        return topK(k, null);
    }

    default RoaringBitmap bottomK(int k) {
        return bottomK(k, null);
    }

    @Nullable
    default Long min() {
        return min(null);
    }

    @Nullable
    default Long max() {
        return max(null);
    }

    @Nullable
    default Long sum() {
        return sum(null);
    }

    @Nullable
    default Long count() {
        return count(null);
    }

    void subtract(long input);
}
