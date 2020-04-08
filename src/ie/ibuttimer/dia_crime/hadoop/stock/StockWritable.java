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

import ie.ibuttimer.dia_crime.hadoop.AbstractBaseWritable;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Custom writable for stocks
 */
public class StockWritable extends AbstractStockWritable<StockWritable> implements Writable {

    private double open;
    private double high;
    private double low;
    private double close;
    private double adjClose;
    private double volume;

    public static final StockWritable MIN_VALUE;
    public static final StockWritable MAX_VALUE;
    static {
        MIN_VALUE = new StockWritable(Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE,
                Double.MIN_VALUE, Double.MIN_VALUE, "");
        MAX_VALUE = new StockWritable(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                Double.MAX_VALUE, Double.MAX_VALUE, "");
    }

    /* Date,Open,High,Low,Close,Adj Close,Volume
     */

    // Default constructor to allow (de)serialization
    public StockWritable() {
        this(0, 0, 0, 0, 0, 0, "");
    }

    public StockWritable(double open, double high, double low, double close, double adjClose, double volume, String id) {
        super(id);
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjClose = adjClose;
        this.volume = volume;
    }

    public StockWritable(StockWritable toCopy) {
        this(toCopy.open, toCopy.high, toCopy.low, toCopy.close, toCopy.adjClose, toCopy.volume, toCopy.getId());
    }

    @Override
    public StockWritable getInstance() {
        return new StockWritable();
    }

    public static StockWritable read(DataInput dataInput) throws IOException {
        StockWritable writable = new StockWritable();
        writable.readFields(dataInput);
        return writable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeDouble(open);
        dataOutput.writeDouble(high);
        dataOutput.writeDouble(low);
        dataOutput.writeDouble(close);
        dataOutput.writeDouble(adjClose);
        dataOutput.writeDouble(volume);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        this.open = dataInput.readDouble();
        this.high = dataInput.readDouble();
        this.low = dataInput.readDouble();
        this.close = dataInput.readDouble();
        this.adjClose = dataInput.readDouble();
        this.volume = dataInput.readDouble();
    }

    public static StockWritable readWritable(DataInput dataInput, StockWritable obj) throws IOException {
         obj.readFields(dataInput);
         return obj;
    }

    public static StockWritable readWritable(DataInput dataInput) throws IOException {
        return StockWritable.readWritable(dataInput, new StockWritable());
    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }

    public double getAdjClose() {
        return adjClose;
    }

    public void setAdjClose(double adjClose) {
        this.adjClose = adjClose;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
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
            Value.ifDouble(value, v -> {
                set.set(true);
                switch (field) {
                    case OPEN_PROP:     setOpen(v);     break;
                    case HIGH_PROP:     setHigh(v);     break;
                    case LOW_PROP:      setLow(v);      break;
                    case CLOSE_PROP:    setClose(v);    break;
                    case ADJCLOSE_PROP: setAdjClose(v); break;
                    case VOLUME_PROP:   setVolume(v);   break;
                    default:            set.set(false); break;
                }
            });
        }
        return set.get();
    }

    @Override
    public void add(StockWritable other) {
        this.open += other.open;
        this.high += other.high;
        this.low += other.low;
        this.close += other.close;
        this.adjClose += other.adjClose;
        this.volume += other.volume;
    }

    @Override
    public void set(StockWritable other) {
        super.set(other);
        this.open = other.open;
        this.high = other.high;
        this.low = other.low;
        this.close = other.close;
        this.adjClose = other.adjClose;
        this.volume = other.volume;
        setId(other.getId());
    }

    @Override
    public void min(StockWritable other) {
        this.open = Double.min(this.open, other.open);
        this.high = Double.min(this.high, other.high);
        this.low = Double.min(this.low, other.low);
        this.close = Double.min(this.close, other.close);
        this.adjClose = Double.min(this.adjClose, other.adjClose);
        this.volume = Double.min(this.volume, other.volume);
    }

    @Override
    public void max(StockWritable other) {
        this.open = Double.max(this.open, other.open);
        this.high = Double.max(this.high, other.high);
        this.low = Double.max(this.low, other.low);
        this.close = Double.max(this.close, other.close);
        this.adjClose = Double.max(this.adjClose, other.adjClose);
        this.volume = Double.max(this.volume, other.volume);
    }

    @Override
    public StockWritable copyOf() {
        return new StockWritable(this);
    }

    public static void ifInstance(Object value, Consumer<StockWritable> action) {
        if (isInstance(value)) {
            action.accept((StockWritable)value);
        }
    }

    public static boolean isInstance(Object value) {
        return (value instanceof StockWritable);
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


    public interface IStockWritableBuilder<B extends AbstractBaseWritableBuilder<?,?>, W extends AbstractBaseWritable<?>>
                        extends IBaseWritableBuilder<B, W> {

        B setOpen(String open);

        B setHigh(String high);

        B setLow(String low);

        B setClose(String close);

        B setAdjClose(String adjClose);

        B setVolume(String volume);

        B setId(String id);
    }

    /**
     * Base class for stock writable builders
     * @param <B>   Builder class
     * @param <W>   Writable class
     */
    public abstract static class AbstractStockWritableBuilder<B extends AbstractBaseWritableBuilder<?,?>, W extends StockWritable>
            extends AbstractBaseWritableBuilder<B, W> implements IStockWritableBuilder<B, W> {

        protected AbstractStockWritableBuilder(Logger logger) {
            super(logger);
        }

        public AbstractStockWritableBuilder(Logger logger, Map<String, Double> factors) {
            super(logger, factors);
        }

        @Override
        public B setOpen(String open) {
            getWritable().setOpen(getDouble(open, OPEN_PROP));
            return getThis();
        }

        @Override
        public B setHigh(String high) {
            getWritable().setHigh(getDouble(high, HIGH_PROP));
            return getThis();
        }

        @Override
        public B setLow(String low) {
            getWritable().setLow(getDouble(low, LOW_PROP));
            return getThis();
        }

        @Override
        public B setClose(String close) {
            getWritable().setClose(getDouble(close, CLOSE_PROP));
            return getThis();
        }

        @Override
        public B setAdjClose(String adjClose) {
            getWritable().setAdjClose(getDouble(adjClose, ADJCLOSE_PROP));
            return getThis();
        }

        @Override
        public B setVolume(String volume) {
            getWritable().setVolume(getDouble(volume, VOLUME_PROP));
            return getThis();
        }

        @Override
        public B setId(String id) {
            getWritable().setId(id);
            return getThis();
        }
    }

    /**
     * Stock writable builder
     */
    public static class StockWritableBuilder
            extends AbstractStockWritableBuilder<StockWritableBuilder, StockWritable> {

        private static final Logger logger = Logger.getLogger(StockWritableBuilder.class);

        public static StockWritableBuilder getInstance() {
            return new StockWritableBuilder();
        }

        public static StockWritableBuilder getInstance(Map<String, Double> factors) {
            return new StockWritableBuilder(factors);
        }

        protected StockWritableBuilder() {
            super(logger);
        }

        public StockWritableBuilder(Map<String, Double> factors) {
            super(logger, factors);
        }

        @Override
        public StockWritableBuilder getThis() {
            return this;
        }

        @Override
        public StockWritable getNewWritable() {
            return new StockWritable();
        }

        @Override
        public StockWritable build() {
            return getWritable();
        }
    }
}
