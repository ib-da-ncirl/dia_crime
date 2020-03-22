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

import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reducer to calculate average values for NASDAQ Composite stock entries:
 * - input key : stock id
 * - input value : StockEntryAvgWritable with average values and count
 * - output key : stock id
 * - input value : text with
 */
public class StockEntryAvgReducer extends Reducer<Text, MapWritable, Text, Text> {

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

        // sort based on stock
        Map<String, String> map = new TreeMap<>();

        values.forEach(avgStock -> {
            avgStock.forEach((stockKey, stockEntry) -> {
                StockEntryAvgWritable stockValue = (StockEntryAvgWritable) stockEntry;
                map.put("Open", Double.toString(stockValue.getOpen()));
                map.put("High", Double.toString(stockValue.getHigh()));
                map.put("Low", Double.toString(stockValue.getLow()));
                map.put("Close", Double.toString(stockValue.getClose()));
                map.put("AdjClose", Double.toString(stockValue.getAdjClose()));
                map.put("Volume", Long.toString(stockValue.getVolume()));
                map.put("Count", Long.toString(stockValue.getCount()));
            });
        });

        context.write(key, new Text(MapStringifier.stringify(map)));
    }
}
