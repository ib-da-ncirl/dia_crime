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

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Combiner to calculate average values for NASDAQ Composite stock entries:
 * - input key : stock id
 * - input value : StockEntryAvgWritable
 * - output key : stock id
 * - input value : StockEntryAvgWritable with average values and count
 */
public class StockEntryAvgCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {

    private MapWritable mapOut = new MapWritable();

    /**
     * Reduce the values for a key
     * @param key       Key value; date string
     * @param values    Values for the specified key
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

        BigDecimal open = new BigDecimal(0);
        BigDecimal high = new BigDecimal(0);
        BigDecimal low = new BigDecimal(0);
        BigDecimal close = new BigDecimal(0);
        BigDecimal adjClose = new BigDecimal(0);
        BigInteger volume = new BigInteger("0");
        AtomicLong count = new AtomicLong();

        for (MapWritable value : values) {
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                StockEntryAvgWritable stockValue = (StockEntryAvgWritable) entry.getValue();
                open = open.add(BigDecimal.valueOf(stockValue.getOpen()));
                high = high.add(BigDecimal.valueOf(stockValue.getHigh()));
                low = low.add(BigDecimal.valueOf(stockValue.getLow()));
                close = close.add(BigDecimal.valueOf(stockValue.getClose()));
                adjClose = adjClose.add(BigDecimal.valueOf(stockValue.getAdjClose()));
                volume = volume.add(BigInteger.valueOf(stockValue.getVolume()));
                count.addAndGet(stockValue.getCount());
            }
        }

        BigDecimal bdCount = BigDecimal.valueOf(count.get());
        StockEntryAvgWritable avgStock = new StockEntryAvgWritable();
        avgStock.setOpen(open.divide(bdCount, RoundingMode.CEILING).doubleValue());
        avgStock.setHigh(high.divide(bdCount, RoundingMode.CEILING).doubleValue());
        avgStock.setLow(low.divide(bdCount, RoundingMode.CEILING).doubleValue());
        avgStock.setClose(close.divide(bdCount, RoundingMode.CEILING).doubleValue());
        avgStock.setAdjClose(adjClose.divide(bdCount, RoundingMode.CEILING).doubleValue());
        avgStock.setVolume(volume.divide(BigInteger.valueOf(count.get())).longValue());
        avgStock.setCount(bdCount.longValue());

        mapOut.clear();
        mapOut.put(key, avgStock);

        context.write(key, mapOut);
    }

}
