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

package ie.ibuttimer.dia_crime.hadoop.crime;

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;
import static ie.ibuttimer.dia_crime.misc.Utils.iterableOfMapsToList;

/**
 * Reducer for a crime entry
 * - input key : date
 * - input value : MapWritable<date, CrimeWritable>
 * - output key : date
 * - output value : value string of <category>:<count> separated by ','
 */
public class CrimeReducer extends AbstractReducer<DateWritable, MapWritable, DateWritable, Text> implements IOutputType {

    private Map<String, OpTypeEntry> outputTypes;

    private MultipleOutputs<DateWritable, Text> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setSection(CRIME_PROP_SECTION);

        super.setup(context);
        setLogger(getClass());
        outputTypes = newOutputTypeMap();

        mos = new MultipleOutputs<>(context);
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

        Counters.ReducerCounter counter = getCounter(context, CountersEnum.CRIME_REDUCER_COUNT);

        // flatten the maps in values and get a list of the CrimeEntryWritables
        List<CrimeWritable> crimes = iterableOfMapsToList(values, CrimeWritable.class);

        Map<String, Integer> map = reduceToTotalsPerCategory(crimes, counter, this);

        // create value string of <category>:<count> separated by ',' with <total>:<count> at the end
        // e.g. 2001-01-01	01A:2, 02:87, 03:41, 04A:28, 04B:44, 05:66, 06:413, 07:60, 08A:43, 08B:252, 10:12, 11:73, 12:7, 14:233, 15:32, 16:5, 17:68, 18:89, 19:2, 20:44, 22:3, 24:4, 26:211, total:1819
        write(context, key, new Text(MAP_STRINGIFIER.stringify(map)));
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        if (context.getProgress() == 1.0) {

            formatOutputTypes(this).forEach(s -> {
                try {
                    write(mos, TYPES_NAMED_OP, DateWritable.COMMENT_KEY, new Text(s));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        } else {
            getLogger().info("Skipping generation of output types file, job incomplete: " + context.getProgress());
        }

        mos.close();
    }

    /**
     * Reduce inputs to totals per FBI code category
     * @param values
     * @param counter
     * @param reducer
     * @return
     */
    public static Map<String, Integer> reduceToTotalsPerCategory(
        List<CrimeWritable> values, Counters.ReducerCounter counter, IOutputType reducer) {

        Map<String, Integer> counts = new HashMap<>();

        // get counts for each category
        values.forEach(value -> {
            String category = value.getFbiCode();

            if (!counts.containsKey(category)) {
                counts.put(category, 0);

                // add category to output name/type info
                reducer.putOutputType(category, Integer.class, CRIME_PROP_SECTION);
            }
            counts.put(category, counts.get(category) + 1);

            if (counter != null) {
                counter.increment();
            }
        });

        // sort based on category
        int total = 0;  // total crimes for the day
        Map<String, Integer> map = new TreeMap<>();
        for (String category : counts.keySet()) {
            int count = counts.get(category);
            total += count;
            map.put(category, count);
        }
        map.put(TOTAL_PROP, total);

        // add total to output name/type info
        reducer.putOutputType(TOTAL_PROP, Integer.class, CRIME_PROP_SECTION);

        return map;
    }

    /**
     * Format the types of the properties for saving
     * @param otProduce
     */
    public static List<String> formatOutputTypes(IOutputType otProduce) {

        Map<String, OpTypeEntry> outputTypes = otProduce.getOutputTypeMap();

        return outputTypes.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(e -> e.getKey() + "," + e.getValue().getCls().getSimpleName() + "," + e.getValue().getSrc())
            .collect(Collectors.toList());
    }

    @Override
    public Map<String, OpTypeEntry> getOutputTypeMap() {
        return outputTypes;
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

