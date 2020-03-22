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

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StockEntryAvgWritable extends StockEntryWritable implements Writable {

    private long count;

    // Default constructor to allow (de)serialization
    public StockEntryAvgWritable() {
        super();
        this.count = 0;
    }

    public StockEntryAvgWritable(double open, double high, double low, double close, double adjClose,
                                 long volume, long count) {
        super(open, high, low, close, adjClose, volume);
        this.count = count;
    }

    public StockEntryAvgWritable(StockEntryAvgWritable toCopy) {
        this(toCopy.getOpen(), toCopy.getHigh(), toCopy.getLow(), toCopy.getClose(), toCopy.getAdjClose(),
            toCopy.getVolume(), toCopy.count);
    }

    public static StockEntryAvgWritable read(DataInput in) throws IOException {
        StockEntryAvgWritable cew = new StockEntryAvgWritable();
        cew.readFields(in);
        return cew;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        this.count = dataInput.readLong();
    }

    public static StockEntryAvgWritable readWritable(DataInput dataInput, StockEntryAvgWritable obj) throws IOException {
         obj.readFields(dataInput);
         return obj;
    }

    public static StockEntryAvgWritable readWritable(DataInput dataInput) throws IOException {
        return StockEntryAvgWritable.readWritable(dataInput, new StockEntryAvgWritable());
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public StockEntryAvgWritable copyOf() {
        return new StockEntryAvgWritable(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                super.toString() +
                ", count=" + count +
                '}';
    }

    public static class StockEntryAvgWritableBuilder
            extends AbstractStockEntryWritableBuilder<StockEntryAvgWritableBuilder, StockEntryAvgWritable> {

        private static final Logger logger = Logger.getLogger(StockEntryAvgWritableBuilder.class);

        public static StockEntryAvgWritableBuilder getInstance() {
            return new StockEntryAvgWritableBuilder();
        }

        protected StockEntryAvgWritableBuilder() {
            super(logger);
        }

        public StockEntryAvgWritableBuilder setCount(long count) {
            getWritable().setCount(count);
            return this;
        }

        @Override
        public StockEntryAvgWritableBuilder getThis() {
            return this;
        }

        @Override
        public StockEntryAvgWritable getNewWritable() {
            return new StockEntryAvgWritable();
        }

        @Override
        public StockEntryAvgWritable build() {
            return getWritable();
        }
    }

}
