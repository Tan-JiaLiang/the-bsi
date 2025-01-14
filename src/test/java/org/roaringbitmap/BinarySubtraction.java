package org.roaringbitmap;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Objects;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class BinarySubtraction {

    @RepeatedTest(1000)
    public void test() {
        Random random = new Random();
        long left = random.nextLong();
        long right = random.nextLong();
        right = right > left ? right - left : right;
        assertThat(subtractBinary(left, right)).isEqualTo(left - right);
    }

    public static long subtractBinary(long num1, long num2) {
        long borrow;
        while (num2 != 0) {
            // 计算无进位减法结果
            borrow = (~num1) & num2;
            num1 = num1 ^ num2;
            // 计算进位
            num2 = borrow << 1;
        }
        return num1;
    }

    @Test
    public void test0() {
        System.out.println(0b11011);
        System.out.println(0b01110);
        System.out.println(0b01101);
    }

    @RepeatedTest(10)
    public void test1() {
        Random random = new Random();
        Integer[] values = new Integer[1000];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextInt(1000000);
        }

        System.out.println(Arrays.toString(values));

        Integer max = Arrays.stream(values).filter(Objects::nonNull).max(Integer::compare).get();
        Integer min = Arrays.stream(values).filter(Objects::nonNull).min(Integer::compare).get();
        RangeEncodeBitSliceIndexBitmap bsi = new RangeEncodeBitSliceIndexBitmap(min, max);
        for (int i = 0; i < values.length; i++) {
            Integer value = values[i];
            if (value != null) {
                bsi.set(i, value);
            }
        }

        bsi.subtract(min);

        for (int i = 0; i < values.length; i++) {
            Integer value = values[i];
            if (value == null) {
                continue;
            }
            assertThat(bsi.get(i)).isEqualTo(value - min);
        }
    }
}
