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

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;

/**
 * Reducer for a stock entries:
 * - input key : date
 * - input value : MapWritable<StockWritable>
 * - output key : date
 * - output value : MapWritable<StockWritable>
 */
public class StockReducer extends AbstractReducer<DateWritable, MapWritable, DateWritable, Text> {

    private static final Log LOG = LogFactory.getLog(StockReducer.class);

    public static final String STOCK_ID_SEPARATOR = ">";

    private final MapStringifier.ElementStringify idValStringifier = MapStringifier.ElementStringify.of(STOCK_ID_SEPARATOR);

    private Counters.ReducerCounter counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        counter = getCounter(context, CountersEnum.STOCK_REDUCER_COUNT);
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
    protected void reduce(DateWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

        // sort based on stock
        Map<String, String> map = new TreeMap<>();

        values.forEach(stock -> {
            stock.forEach((stockKey, stockEntry) -> {
                if (stockEntry instanceof StockWritable) {

                    map.clear();

                    // convert the StockWritable object map to a string map
                    ((StockWritable)stockEntry).toMap().forEach((k, v) -> map.put(k, v.toString()));

                    // create value string of <field>:<value> separated by ',' with leading stock id marker
                    // e.g. 2001-01-02	DJI>adjclose:10646.150391, close:10646.150391, date:2001-01-02, high:10797.019531, low:10585.360352, open:10790.919922, volume:253300000
                    try {
                        write(context, key, new Text(
                            idValStringifier.stringifyElement(stockKey.toString(), MAP_STRINGIFIER.stringify(map))
                        ));

                        counter.increment();
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    if (stockEntry == null) {
                        LOG.warn("Ignoring null object");
                    } else {
                        LOG.warn("Unexpected object of class: " + stockEntry.getClass().getName());
                    }
                }
            });
        });
    }

    public static Map<String, String> reduce(List<Map<String, StockWritable>> values, Counters.ReducerCounter counter) {

        // sort based on stock
        Map<String, String> map = new TreeMap<>();

        values.forEach(stock -> {
            stock.forEach((stockKey, stockEntry) -> {
                // convert the StockWritable object map to a string map
                stockEntry.toMap().forEach((k, v) -> map.put(stockKey + "_" + k, v.toString()));

                counter.increment();
            });
        });

        return map;
    }

    @Override
    protected DateWritable newKey(String key) {
        return DateWritable.ofDate(key);
    }

    @Override
    protected Text newValue(String value) {
        return new Text(value);
    }
}
