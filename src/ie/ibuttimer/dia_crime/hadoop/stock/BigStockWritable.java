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

package ie.ibuttimer.dia_crime.hadoop.stock;

import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Customer stock writable utilising 'Big' number classes
 */
@Deprecated
public class BigStockWritable extends AbstractStockWritable<BigStockWritable> implements Writable {

    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private BigDecimal adjClose;
    private BigInteger volume;

    private UUID uuid = null;   // only set for symbolic MIN_VALUE & MAX_VALUE

    public static final BigStockWritable MIN_VALUE;
    private static final UUID MIN_UUID;
    public static final BigStockWritable MAX_VALUE;
    private static final UUID MAX_UUID;
    static {
        /* BigDecimal & BigInteger min/max values are *big*, there are no statics in jdk since they are basically only
            limited by device memory. For the purposes of this application these are symbolic min & max which are
            sufficient.
            Note: This implementation is not portable!
         */
        MIN_VALUE = new BigStockWritable(null, null, null, null, null, null, "");
        MIN_UUID = UUID.randomUUID();
        MIN_VALUE.uuid = MIN_UUID;
        MAX_VALUE = new BigStockWritable(null, null, null, null, null, null, "");
        MAX_UUID = UUID.randomUUID();
        MAX_VALUE.uuid = MAX_UUID;
    }

    /* Date,Open,High,Low,Close,Adj Close,Volume
     */

    // Default constructor to allow (de)serialization
    public BigStockWritable() {
        super("");
        zero();
    }

    public BigStockWritable(BigDecimal open, BigDecimal high, BigDecimal low,
                            BigDecimal close, BigDecimal adjClose, BigInteger volume, String id) {
        super(id);
        init(open, high, low, close, adjClose, volume);
    }

    public BigStockWritable(BigStockWritable toCopy) {
        this(toCopy.open, toCopy.high, toCopy.low, toCopy.close, toCopy.adjClose, toCopy.volume, toCopy.getId());
    }

    private void zero() {
        init(BigDecimal.valueOf(0), BigDecimal.valueOf(0), BigDecimal.valueOf(0),
                BigDecimal.valueOf(0), BigDecimal.valueOf(0), BigInteger.valueOf(0));
    }

    private void init(BigDecimal open, BigDecimal high, BigDecimal low,
                      BigDecimal close, BigDecimal adjClose, BigInteger volume) {
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjClose = adjClose;
        this.volume = volume;
    }

    @Override
    public BigStockWritable getInstance() {
        return new BigStockWritable();
    }

    public static BigStockWritable read(DataInput dataInput) throws IOException {
        BigStockWritable writable = new BigStockWritable();
        writable.readFields(dataInput);
        return writable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (isSymbolicMinOrMax(this)) {
            throw new UnsupportedOperationException(getClass().getSimpleName() + " MIN_VALUE/MAX_VALUE is not Writable");
        } else {
            super.write(dataOutput);
            dataOutput.writeUTF(open.toPlainString());
            dataOutput.writeUTF(high.toPlainString());
            dataOutput.writeUTF(low.toPlainString());
            dataOutput.writeUTF(close.toPlainString());
            dataOutput.writeUTF(adjClose.toPlainString());
            dataOutput.writeUTF(volume.toString());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        this.open = new BigDecimal(dataInput.readUTF());
        this.high = new BigDecimal(dataInput.readUTF());
        this.low = new BigDecimal(dataInput.readUTF());
        this.close = new BigDecimal(dataInput.readUTF());
        this.adjClose = new BigDecimal(dataInput.readUTF());
        this.volume = new BigInteger(dataInput.readUTF());
    }

    public static BigStockWritable readWritable(DataInput dataInput, BigStockWritable obj) throws IOException {
         obj.readFields(dataInput);
         return obj;
    }

    public static BigStockWritable readWritable(DataInput dataInput) throws IOException {
        return BigStockWritable.readWritable(dataInput, new BigStockWritable());
    }

    public BigDecimal getOpen() {
        isSymbolicMinOrMaxThrow();
        return open;
    }

    public void setOpen(BigDecimal open) {
        isSymbolicMinOrMaxThrow();
        this.open = open;
    }

    public void setOpen(String open) {
        setOpen(new BigDecimal(open));
    }

    public BigDecimal getHigh() {
        isSymbolicMinOrMaxThrow();
        return high;
    }

    public void setHigh(BigDecimal high) {
        isSymbolicMinOrMaxThrow();
        this.high = high;
    }

    public void setHigh(String high) {
        setHigh(new BigDecimal(high));
    }

    public BigDecimal getLow() {
        isSymbolicMinOrMaxThrow();
        return low;
    }

    public void setLow(BigDecimal low) {
        isSymbolicMinOrMaxThrow();
        this.low = low;
    }

    public void setLow(String low) {
        setLow(new BigDecimal(low));
    }

    public BigDecimal getClose() {
        isSymbolicMinOrMaxThrow();
        return close;
    }

    public void setClose(BigDecimal close) {
        isSymbolicMinOrMaxThrow();
        this.close = close;
    }

    public void setClose(String close) {
        setClose(new BigDecimal(close));
    }

    public BigDecimal getAdjClose() {
        isSymbolicMinOrMaxThrow();
        return adjClose;
    }

    public void setAdjClose(BigDecimal adjClose) {
        isSymbolicMinOrMaxThrow();
        this.adjClose = adjClose;
    }

    public void setAdjClose(String adjClose) {
        setAdjClose(new BigDecimal(adjClose));
    }

    public BigInteger getVolume() {
        isSymbolicMinOrMaxThrow();
        return volume;
    }

    public void setVolume(BigInteger volume) {
        isSymbolicMinOrMaxThrow();
        this.volume = volume;
    }

    public void setVolume(String volume) {
        setVolume(new BigInteger(volume));
    }

    @Override
    public Optional<Value> getField(String field) {
        Optional<Value> value = super.getField(field);
        if (value.isEmpty()) {
            switch (field) {
                case OPEN_PROP:     value = Value.ofOptional(open);     break;
                case HIGH_PROP:     value = Value.ofOptional(high);     break;
                case LOW_PROP:      value = Value.ofOptional(low);      break;
                case CLOSE_PROP:    value = Value.ofOptional(close);    break;
                case ADJCLOSE_PROP: value = Value.ofOptional(adjClose); break;
                case VOLUME_PROP:   value = Value.ofOptional(volume);   break;
                default:            value = Value.empty();              break;
            }
        }
        return value;
    }

    @Override
    public boolean setField(String field, Object value) {
        AtomicBoolean set = new AtomicBoolean(super.setField(field, value));
        if (!set.get()) {
            Value.ifBigDecimal(value, v -> {
                set.set(true);
                switch (field) {
                    case OPEN_PROP:     setOpen(v);     break;
                    case HIGH_PROP:     setHigh(v);     break;
                    case LOW_PROP:      setLow(v);      break;
                    case CLOSE_PROP:    setClose(v);    break;
                    case ADJCLOSE_PROP: setAdjClose(v); break;
                    default:            set.set(false); break;
                }
            });
            if (!set.get()) {
                Value.ifBigInteger(value, v -> {
                    set.set(true);
                    switch (field) {
                        case VOLUME_PROP:   setVolume(v);   break;
                        default:            set.set(false); break;
                    }
                });
            }
        }
        return set.get();
    }

    @Override
    public BigStockWritable copyOf() {
        return new BigStockWritable(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                super.toString() +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", close=" + close +
                ", adjClose=" + adjClose +
                ", volume=" + volume +
                '}';
    }

    private static boolean isSymbolicMinOrMax(BigStockWritable obj) {
        return obj.uuid != null;
    }

    private static boolean isSymbolicMin(BigStockWritable obj) {
        return obj.uuid == MIN_UUID;
    }

    private static boolean isSymbolicMax(BigStockWritable obj) {
        return obj.uuid == MAX_UUID;
    }

    private void isSymbolicMinOrMaxThrow() {
        if (isSymbolicMinOrMax(this)) {
            throw new UnsupportedOperationException("Not supported on " + getClass().getSimpleName() + " MIN_VALUE/MAX_VALUE");
        }
    }

    @Override
    public void add(BigStockWritable other) {
        if ((isSymbolicMin(this) && isSymbolicMax(other)) || (isSymbolicMax(this) && isSymbolicMin(other))) {
            this.zero();
        } else if (isSymbolicMinOrMax(this) || isSymbolicMinOrMax(other)) {
            throw new UnsupportedOperationException("Cannot do arithmetic on " + getClass().getSimpleName() + " MIN_VALUE/MAX_VALUE");
        } else {
            open = open.add(other.open);
            high = high.add(other.high);
            low = low.add(other.low);
            close = close.add(other.close);
            adjClose = adjClose.add(other.adjClose);
            volume = volume.add(other.volume);
        }
    }

    @Override
    public void set(BigStockWritable other) {
        super.set(other);
        this.open = other.open;
        this.high = other.high;
        this.low = other.low;
        this.close = other.close;
        this.adjClose = other.adjClose;
        this.volume = other.volume;
    }

    @Override
    public void min(BigStockWritable other) {
        if (isSymbolicMin(this)) {
            // no op
        } else if (isSymbolicMinOrMax(this) || isSymbolicMinOrMax(other)) {
            throw new UnsupportedOperationException("Cannot do min on " + getClass().getSimpleName() + " MIN_VALUE/MAX_VALUE");
        } else {
            open = open.min(other.open);
            high = high.min(other.high);
            low = low.min(other.low);
            close = close.min(other.close);
            adjClose = adjClose.min(other.adjClose);
            volume = volume.min(other.volume);
        }
    }

    @Override
    public void max(BigStockWritable other) {
        if (isSymbolicMax(this)) {
            // no op
        } else if (isSymbolicMinOrMax(this) || isSymbolicMinOrMax(other)) {
            throw new UnsupportedOperationException("Cannot do min on " + getClass().getSimpleName() + " MIN_VALUE/MAX_VALUE");
        } else {
            open = open.max(other.open);
            high = high.max(other.high);
            low = low.max(other.low);
            close = close.max(other.close);
            adjClose = adjClose.max(other.adjClose);
            volume = volume.max(other.volume);
        }
    }

    /**
     * Base class for
     * @param <B>
     * @param <W>
     */
    public abstract static class AbstractBigStockEntryWritableBuilder<B extends AbstractBaseWritableBuilder<?,?>, W extends BigStockWritable>
            extends AbstractBaseWritableBuilder<B, W> implements StockWritable.IStockWritableBuilder<B, W>  {

        protected AbstractBigStockEntryWritableBuilder(Logger logger) {
            super(logger);
        }

        @Override
        public B setOpen(String open) {
            getWritable().setOpen(getBigDecimal(open));
            return getThis();
        }

        @Override
        public B setHigh(String high) {
            getWritable().setHigh(getBigDecimal(high));
            return getThis();
        }

        @Override
        public B setLow(String low) {
            getWritable().setLow(getBigDecimal(low));
            return getThis();
        }

        @Override
        public B setClose(String close) {
            getWritable().setClose(getBigDecimal(close));
            return getThis();
        }

        @Override
        public B setAdjClose(String adjClose) {
            getWritable().setAdjClose(getBigDecimal(adjClose));
            return getThis();
        }

        @Override
        public B setVolume(String volume) {
            getWritable().setVolume(getBigInteger(volume));
            return getThis();
        }

        @Override
        public B setId(String id) {
            getWritable().setId(id);
            return getThis();
        }
    }

    public static class BigStockEntryWritableBuilder
            extends AbstractBigStockEntryWritableBuilder<BigStockEntryWritableBuilder, BigStockWritable> {

        // TODO add factor support

        private static final Logger logger = Logger.getLogger(BigStockEntryWritableBuilder.class);

        public static BigStockEntryWritableBuilder getInstance() {
            return new BigStockEntryWritableBuilder();
        }

        protected BigStockEntryWritableBuilder() {
            super(logger);
        }


        @Override
        public BigStockEntryWritableBuilder getThis() {
            return this;
        }

        @Override
        public BigStockWritable getNewWritable() {
            return new BigStockWritable();
        }

        @Override
        public BigStockWritable build() {
            return getWritable();
        }
    }
}
