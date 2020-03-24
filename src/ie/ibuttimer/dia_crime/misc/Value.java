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

    public boolean isNumber() {
        return value instanceof Number;
    }

    public boolean isBigNumber() {
        return isBigInteger() || isBigDecimal();
    }

    public boolean isBigInteger() {
        return (value instanceof BigInteger);
    }

    public boolean isBigDecimal() {
        return (value instanceof BigDecimal);
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
