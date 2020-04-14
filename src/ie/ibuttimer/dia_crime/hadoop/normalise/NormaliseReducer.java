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

package ie.ibuttimer.dia_crime.hadoop.normalise;

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.crime.IOutputType;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.hadoop.regression.RegressionWritable;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsConfigReader;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer.saveOutputTypes;
import static ie.ibuttimer.dia_crime.misc.Constants.NORMALISE_PROP_SECTION;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;

/**
 * Reducer for statistics, which accumulates values to output sum of values
 * - input key : date
 * - input value : normalised file line text
 * - output key : date
 * - output value : normalised file line text
 */
public class NormaliseReducer extends AbstractReducer<DateWritable, RegressionWritable<String, Value>, DateWritable, Text>
                            implements IOutputType {

    private Counters.ReducerCounter counter;

    private Map<String, Class<?>> outputTypes;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setSection(NORMALISE_PROP_SECTION);

        super.setup(context);

        counter = getCounter(context, CountersEnum.NORMALISE_REDUCER_COUNT);

        outputTypes = new TreeMap<>();
    }

    @Override
    protected void reduce(DateWritable key, Iterable<RegressionWritable<String, Value>> values, Context context) throws IOException, InterruptedException {

        addOutputHeader(context, counter, List.of(), List.of());

        counter.increment();

        values.forEach(value -> {
            if (!key.equals(DateWritable.MIN)) {
                // standard line
                Map<String, String> outMap = new TreeMap<>();
                value.forEach((key1, value1) -> value1.asString(s -> {
                    outMap.put(key1, s);
                }));

                try {
                    context.write(key, new Text(MAP_STRINGIFIER.stringify(outMap)));

                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else {    // output types
                // TODO should really be a separate reducer
                // load output type from input
                StatsConfigReader cfgReader = new StatsConfigReader(getSection());
                Map<String, String> map = new HashMap<>();

                value.forEach((key1, value1) -> value1.asString(s -> map.put(key1, s)));
                outputTypes = cfgReader.convertOutputTypeClasses(map);
            }
        });
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        saveOutputTypes(context,this, this);
    }

    @Override
    public Map<String, Class<?>> getOutputTypeMap() {
        return outputTypes;
    }

    @Override
    protected DateWritable newKey(String key) {
        return DateWritable.COMMENT_KEY;
    }

    @Override
    protected Text newValue(String value) {
        return new Text(value);
    }
}
