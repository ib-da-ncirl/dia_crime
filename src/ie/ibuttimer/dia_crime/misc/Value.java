/*
 * The MIT License (MIT)
 * Copyright (c) 2020 Ian Buttimer
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ie.ibuttimer.dia_crime.misc;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.function.Consumer;

public class Value extends Object {

    public static <Value> Optional<Value> empty() {
        return Optional.empty();
    }

    private Object value;

    private Value(Object value) {
        this.value = value;
    }

    public static Value of(Object value) {
        return new Value(value);
    }

    public static Optional<Value> ofOptional(Object value) {
        return Optional.of(new Value(value));
    }

    public static boolean isNumber(Object value)  {
        return value instanceof Number;
    }

    public static boolean isBigNumber(Object value) {
        return isBigInteger(value) || isBigDecimal(value);
    }

    public static boolean isBigInteger(Object value) {
        return (value instanceof BigInteger);
    }

    public static boolean isBigDecimal(Object value) {
        return (value instanceof BigDecimal);
    }

    public static boolean isString(Object value) {
        return (value instanceof String);
    }

    public static boolean isDouble(Object value) {
        return (value instanceof Double);
    }

    public static boolean isFloat(Object value) {
        return (value instanceof Float);
    }

    public static boolean isLong(Object value) {
        return (value instanceof Long);
    }

    public static boolean isInteger(Object value) {
        return (value instanceof Integer);
    }

    public static boolean isShort(Object value) {
        return (value instanceof Short);
    }

    public static boolean isByte(Object value) {
        return (value instanceof Byte);
    }

    public static boolean isCharacter(Object value) {
        return (value instanceof Character);
    }

    public static boolean isLocalDateTime(Object value) {
        return (value instanceof LocalDateTime);
    }

    public static boolean isLocalDate(Object value) {
        return (value instanceof LocalDate);
    }

    public static void ifNumber(Object value, Consumer<? super Number> action) {
        if (isNumber(value)) {
            action.accept((Number)value);
        }
    }

    public static void ifBigNumber(Object value, Consumer<? super Number> action) {
        if (isBigNumber(value)) {
            action.accept((Number)value);
        }
    }

    public static void ifBigInteger(Object value, Consumer<? super BigInteger> action) {
        if (isBigInteger(value)) {
            action.accept((BigInteger)value);
        }
    }

    public static void ifBigDecimal(Object value, Consumer<? super BigDecimal> action) {
        if (isBigDecimal(value)) {
            action.accept((BigDecimal)value);
        }
    }

    public static void ifString(Object value, Consumer<? super String> action) {
        if (isString(value)) {
            action.accept((String)value);
        }
    }

    public static void ifDouble(Object value, Consumer<? super Double> action) {
        if (isDouble(value)) {
            action.accept((Double)value);
        }
    }

    public static void ifFloat(Object value, Consumer<? super Float> action) {
        if (isFloat(value)) {
            action.accept((Float)value);
        }
    }

    public static void ifLong(Object value, Consumer<? super Long> action) {
        if (isLong(value)) {
            action.accept((Long)value);
        }
    }

    public static void ifInteger(Object value, Consumer<? super Integer> action) {
        if (isInteger(value)) {
            action.accept((Integer)value);
        }
    }

    public static void ifShort(Object value, Consumer<? super Short> action) {
        if (isShort(value)) {
            action.accept((Short)value);
        }
    }

    public static void ifByte(Object value, Consumer<? super Byte> action) {
        if (isByte(value)) {
            action.accept((Byte)value);
        }
    }

    public static void ifCharacter(Object value, Consumer<? super Character> action) {
        if (isCharacter(value)) {
            action.accept((Character)value);
        }
    }

    public static void ifLocalDateTime(Object value, Consumer<? super LocalDateTime> action) {
        if (isLocalDateTime(value)) {
            action.accept((LocalDateTime)value);
        }
    }

    public static void ifLocalDate(Object value, Consumer<? super LocalDate> action) {
        if (isLocalDate(value)) {
            action.accept((LocalDate)value);
        }
    }

    public boolean isNumber() {
        return isNumber(value);
    }

    public boolean isBigNumber() {
        return isBigNumber(value);
    }

    public boolean isBigInteger() {
        return isBigInteger(value);
    }

    public boolean isBigDecimal() {
        return isBigDecimal(value);
    }

    public Class getValueClass() {
        return value.getClass();
    }

    public void ifNumber(Consumer<? super Number> action) {
        if (isNumber()) {
            action.accept((Number)this.value);
        }
    }

    private void numericCheck() {
        if (!isNumber()) {
            throw new IllegalStateException("Value is not a Number");
        }
    }

    public Object value() {
        return value;
    }

    public double doubleValue() {
        numericCheck();
        return ((Number)this.value).doubleValue();
    }

    public float floatValue() {
        numericCheck();
        return ((Number)this.value).floatValue();
    }

    public long longValue() {
        numericCheck();
        return ((Number)this.value).longValue();
    }

    public int intValue() {
        numericCheck();
        return ((Number)this.value).intValue();
    }

    public BigInteger bigIntegerValue() {
        numericCheck();
        BigInteger result;
        if (this.value instanceof BigInteger) {
            result = (BigInteger) this.value;
        } else if (this.value instanceof BigDecimal) {
            result = ((BigDecimal) this.value).toBigInteger();
        } else {
            result = new BigInteger(String.valueOf(longValue()));
        }
        return result;
    }

    public BigDecimal bigDecimalValue() {
        numericCheck();
        BigDecimal result;
        if (this.value instanceof BigInteger) {
            result = new BigDecimal((BigInteger)this.value);
        } else if (this.value instanceof BigDecimal) {
            result = (BigDecimal) this.value;
        } else {
            result = new BigDecimal(String.valueOf(doubleValue()));
        }
        return result;
    }

    public void asDouble(Consumer<? super Double> action) {
        if (isNumber()) {
            action.accept(((Number)this.value).doubleValue());
        }
    }


}
