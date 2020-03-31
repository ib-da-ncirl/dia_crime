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

package ie.ibuttimer.dia_crime.hadoop.merge;

import ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeWritable;
import ie.ibuttimer.dia_crime.hadoop.crime.ICrimeReducer;
import ie.ibuttimer.dia_crime.hadoop.misc.CounterEnums;
import ie.ibuttimer.dia_crime.hadoop.stock.StockWritable;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherReducer;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherWritable;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

import static ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer.outputCategories;
import static ie.ibuttimer.dia_crime.misc.Constants.ID_PROP;

public class MergeReducer extends AbstractMergeReducer<Text, CSWWrapperWritable, Text, Text> implements ICrimeReducer {

    private static Set<String> categorySet = new TreeSet<>();

    private MapStringifier.ElementStringify prependStringifier = MapStringifier.ElementStringify.of(CLASS_VAL_SEPARATOR);

    private DayWritable day = new DayWritable();

    public MergeReducer() {
        super();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        setLogger(getClass());
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
    protected void reduce(Text key, Iterable<CSWWrapperWritable> values, Context context) throws IOException, InterruptedException {

        CounterEnums.ReducerCounter counter = getCounter(context, MergeCountersEnum.REDUCER_COUNT);

        // split values by type
        List<CrimeWritable> crimeList = new ArrayList<>(1500);
        List<WeatherWritable> weatherList = new ArrayList<>(24);
        List<StockWritable> stockList = new ArrayList<>();

        values.forEach(iter_value -> {
            if (iter_value.getCrime() != null) {
                crimeList.add(iter_value.getCrime());
            } else if (iter_value.getWeather() != null) {
                weatherList.add(iter_value.getWeather());
            } else if (iter_value.getStock() != null) {
                stockList.add(iter_value.getStock());
            } else {
                throw new IllegalArgumentException("Unknown wrapped object: " + iter_value.getClass().getName());
            }
        });

        // reduce crime to daily totals
        Map<String, Integer> crimeMap = CrimeReducer.reduceToTotalsPerCategory(crimeList, counter, this);
        // reduce weather to daily averages
        Map<String, Object> weatherMap = WeatherReducer.reduceToAverages(weatherList, counter);

        // combine stock entries into single map
        Map<String, Object> entryMap = new TreeMap<>(crimeMap);
        entryMap.putAll(weatherMap);

        stockList.forEach(stock -> {
            String id = stock.getId();
            stock.toMap().entrySet().stream()
                .filter(es -> !es.getKey().equals(ID_PROP))
                .forEach(es -> entryMap.put(id + "_" + es.getKey(), es.getValue()));
        });

        // create value string of <category>:<count> separated by ',' with <total>:<count> at the end
        // e.g. 2001-01-01	01A:2, 02:87, 03:41, 04A:28, 04B:44, 05:66, 06:413, 07:60, 08A:43, 08B:252, 10:12, 11:73, 12:7, 14:233, 15:32, 16:5, 17:68, 18:89, 19:2, 20:44, 22:3, 24:4, 26:211, total:1819
        write(context, key, new Text(MapStringifier.stringify(entryMap)));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        outputCategories(context,this, getLogger());
    }

    @Override
    public Set<String> getCategorySet() {
        return categorySet;
    }
}
