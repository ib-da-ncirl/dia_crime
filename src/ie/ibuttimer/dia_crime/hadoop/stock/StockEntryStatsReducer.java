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

import ie.ibuttimer.dia_crime.hadoop.stats.IStats;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Reducer to calculate standard deviation for NASDAQ Composite stock entries:
 * - input key : stock id
 * - input value : StockEntryAvgWritable with average values and count
 * - output key : stock id
 * - input value : text with
 */
public class StockEntryStatsReducer extends Reducer<Text, MapWritable, Text, Text> implements IStats {

    static BigDecimal MAX_DECIMAL = new BigDecimal(Double.MAX_VALUE);
    static BigDecimal MIN_DECIMAL = new BigDecimal(Double.MIN_VALUE);
    static BigInteger MAX_INTEGER = BigInteger.valueOf(Long.MAX_VALUE);
    static BigInteger MIN_INTEGER = BigInteger.valueOf(Long.MIN_VALUE);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

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

        BigStockEntryWritable summer = new BigStockEntryWritable();
        BigStockEntryWritable minimiser = new BigStockEntryWritable(
                MAX_DECIMAL, MAX_DECIMAL, MAX_DECIMAL, MAX_DECIMAL, MAX_DECIMAL, MAX_INTEGER);
        BigStockEntryWritable maximiser = new BigStockEntryWritable(
                MIN_DECIMAL, MIN_DECIMAL, MIN_DECIMAL, MIN_DECIMAL, MIN_DECIMAL, MIN_INTEGER);

        // TODO probably need reducer only counter
        CounterEnums.ReducerCounter counter = getCounter(context);
        counter.reset();

        if (isStandardKey(key.toString())) {
            values.forEach(stock -> {
                stock.forEach((stockKey, stockEntry) -> {
                    summer.add((BigStockEntryWritable) stockEntry);
                    minimiser.min((BigStockEntryWritable) stockEntry);
                    maximiser.max((BigStockEntryWritable) stockEntry);
                    counter.increment();
                });
            });
            writeOutput(context, Stream.of(
                Pair.of(summer, new Text(getSumKeyTag(key.toString()))),
                Pair.of(minimiser, new Text(getMinKeyTag(key.toString()))),
                Pair.of(maximiser, new Text(getMaxKeyTag(key.toString())))
            ));
        } else {
            // slight duplication but min/min not required for squared values and it'll be quicker
            values.forEach(stock -> {
                stock.forEach((stockKey, stockEntry) -> {
                    summer.add((BigStockEntryWritable) stockEntry);
                    counter.increment();
                });
            });
            writeOutput(context, Stream.of(
                Pair.of(summer, key)
            ));
        }

        // write count to file
        counter.getCount().ifPresent(c -> {
            Map<String, String> map = Map.of(COUNT_PROP, Long.toString(c));
            writeOutput(context, new Text(getCountKeyTag(key.toString())), new Text(MapStringifier.stringify(map)));
        });
    }

    private void writeOutput(Context context, Stream<Pair<BigStockEntryWritable, Text>> stream) {
        Map<String, String> map = new TreeMap<>();

        stream.forEach(pair -> {
            BigStockEntryWritable result = pair.getLeft();
            map.clear();
            map.put(OPEN_PROP, result.getOpen().toPlainString());
            map.put(HIGH_PROP, result.getHigh().toPlainString());
            map.put(LOW_PROP, result.getLow().toPlainString());
            map.put(CLOSE_PROP, result.getClose().toPlainString());
            map.put(ADJCLOSE_PROP, result.getAdjClose().toPlainString());
            map.put(VOLUME_PROP, result.getVolume().toString());

            writeOutput(context, pair.getRight(), new Text(MapStringifier.stringify(map)));
        });
    }

    private void writeOutput(Context context, Text key, Text value) {
        try {
            context.write(key, value);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    protected CounterEnums.ReducerCounter getCounter(Reducer.Context context) {
        return new CounterEnums.ReducerCounter(context, CounterEnums.NasdaqCountersEnum.class.getName(),
                CounterEnums.NasdaqCountersEnum.COUNT.toString());
    }
}
