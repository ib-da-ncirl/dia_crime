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
import java.util.Optional;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.Constants.VOLUME_PROP;

public class StockEntryWritable extends AbstractStockEntryWritable<StockEntryWritable> implements Writable {

    private double open;
    private double high;
    private double low;
    private double close;
    private double adjClose;
    private long volume;

    public static final StockEntryWritable MIN_VALUE;
    public static final StockEntryWritable MAX_VALUE;
    static {
        MIN_VALUE = new StockEntryWritable(Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE, Double.MIN_VALUE,
                Double.MIN_VALUE, Long.MIN_VALUE);
        MAX_VALUE = new StockEntryWritable(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                Double.MAX_VALUE, Long.MAX_VALUE);
    }

    /* Date,Open,High,Low,Close,Adj Close,Volume
     */

    // Default constructor to allow (de)serialization
    public StockEntryWritable() {
        this(0, 0, 0, 0, 0, 0);
    }

    public StockEntryWritable(double open, double high, double low, double close, double adjClose, long volume) {
        super();
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjClose = adjClose;
        this.volume = volume;
    }

    public StockEntryWritable(StockEntryWritable toCopy) {
        this(toCopy.open, toCopy.high, toCopy.low, toCopy.close, toCopy.adjClose, toCopy.volume);
    }

    public static StockEntryWritable read(DataInput in) throws IOException {
        StockEntryWritable cew = new StockEntryWritable();
        cew.readFields(in);
        return cew;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeDouble(open);
        dataOutput.writeDouble(high);
        dataOutput.writeDouble(low);
        dataOutput.writeDouble(close);
        dataOutput.writeDouble(adjClose);
        dataOutput.writeLong(volume);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        this.open = dataInput.readDouble();
        this.high = dataInput.readDouble();
        this.low = dataInput.readDouble();
        this.close = dataInput.readDouble();
        this.adjClose = dataInput.readDouble();
        this.volume = dataInput.readLong();
    }

    public static StockEntryWritable readWritable(DataInput dataInput, StockEntryWritable obj) throws IOException {
         obj.readFields(dataInput);
         return obj;
    }

    public static StockEntryWritable readWritable(DataInput dataInput) throws IOException {
        return StockEntryWritable.readWritable(dataInput, new StockEntryWritable());
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

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volume) {
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
    public void add(StockEntryWritable other) {
        this.open += other.open;
        this.high += other.high;
        this.low += other.low;
        this.close += other.close;
        this.adjClose += other.adjClose;
        this.volume += other.volume;
    }

    @Override
    public void set(StockEntryWritable other) {
        super.set(other);
        this.open = other.open;
        this.high = other.high;
        this.low = other.low;
        this.close = other.close;
        this.adjClose = other.adjClose;
        this.volume = other.volume;
    }

    @Override
    public void min(StockEntryWritable other) {
        this.open = Double.min(this.open, other.open);
        this.high = Double.min(this.high, other.high);
        this.low = Double.min(this.low, other.low);
        this.close = Double.min(this.close, other.close);
        this.adjClose = Double.min(this.adjClose, other.adjClose);
        this.volume = Long.min(this.volume, other.volume);
    }

    @Override
    public void max(StockEntryWritable other) {
        this.open = Double.max(this.open, other.open);
        this.high = Double.max(this.high, other.high);
        this.low = Double.max(this.low, other.low);
        this.close = Double.max(this.close, other.close);
        this.adjClose = Double.max(this.adjClose, other.adjClose);
        this.volume = Long.max(this.volume, other.volume);
    }

    @Override
    public StockEntryWritable copyOf() {
        return new StockEntryWritable(this);
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


    public interface IEntryWritableBuilder<B extends AbstractBaseWritableBuilder, W extends AbstractBaseWritable>
                        extends IBaseWritableBuilder<B, W> {

        B setOpen(String open);

        B setHigh(String high);

        B setLow(String low);

        B setClose(String close);

        B setAdjClose(String adjClose);

        B setVolume(String volume);
    }

    public abstract static class AbstractStockEntryWritableBuilder<B extends AbstractBaseWritableBuilder, W extends StockEntryWritable>
            extends AbstractBaseWritableBuilder<B, W> implements IEntryWritableBuilder<B, W> {

        protected AbstractStockEntryWritableBuilder(Logger logger) {
            super(logger);
        }

        @Override
        public B setOpen(String open) {
            getWritable().setOpen(getDouble(open));
            return getThis();
        }

        @Override
        public B setHigh(String high) {
            getWritable().setHigh(getDouble(high));
            return getThis();
        }

        @Override
        public B setLow(String low) {
            getWritable().setLow(getDouble(low));
            return getThis();
        }

        @Override
        public B setClose(String close) {
            getWritable().setClose(getDouble(close));
            return getThis();
        }

        @Override
        public B setAdjClose(String adjClose) {
            getWritable().setAdjClose(getDouble(adjClose));
            return getThis();
        }

        @Override
        public B setVolume(String volume) {
            getWritable().setVolume(getLong(volume));
            return getThis();
        }
    }

    public static class StockEntryWritableBuilder
            extends AbstractStockEntryWritableBuilder<StockEntryWritableBuilder, StockEntryWritable> {

        private static final Logger logger = Logger.getLogger(StockEntryWritableBuilder.class);

        public static StockEntryWritableBuilder getInstance() {
            return new StockEntryWritableBuilder();
        }

        protected StockEntryWritableBuilder() {
            super(logger);
        }

        @Override
        public StockEntryWritableBuilder getThis() {
            return this;
        }

        @Override
        public StockEntryWritable getNewWritable() {
            return new StockEntryWritable();
        }

        @Override
        public StockEntryWritable build() {
            return getWritable();
        }
    }
}
