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
import ie.ibuttimer.dia_crime.hadoop.regression.RegressionWritable;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsConfigReader;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsMapper;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

import static ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer.saveOutputTypes;
import static ie.ibuttimer.dia_crime.hadoop.normalise.NormaliseMapper.*;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Reducer for statistics, which accumulates values to output sum of values
 * - input key : property name plus specific identifier for squared value etc.
 * - input value : value
 * - output key : property name plus specific identifier for the statistic
 * - output value : value
 */
public class NormaliseReducer extends AbstractReducer<Text, RegressionWritable<String, Value>, Text, Text>
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
    protected void reduce(Text key, Iterable<RegressionWritable<String, Value>> values, Context context) throws IOException, InterruptedException {

        addOutputHeader(context, counter, List.of(), List.of());

        values.forEach(value -> {
            String valueType = key.toString();
            if (valueType.equals(NORM_OUTPUT_LINE)) {
                Text entryKey = new Text();
                value.getProperty(NORM_OUTPUT_KEY).asString(entryKey::set);

                Map<String, String> outMap = new TreeMap<>();
                value.entrySet().stream()
                    .filter(es -> !es.getKey().equals(NORM_OUTPUT_KEY))
                    .forEach(es ->
                        es.getValue().asString(s -> {
                            outMap.put(es.getKey(), s);
                        })
                    );

                try {
                    context.write(entryKey, new Text(MapStringifier.stringify(outMap)));

                    counter.increment();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (valueType.equals(NORM_OUTPUT_TYPES)){
                // load output type from input
                StatsConfigReader cfgReader = new StatsConfigReader(getSection());
                Map<String, String> map = new HashMap<>();

                value.entrySet().stream()
                    .filter(es -> !es.getKey().equals(NORM_OUTPUT_KEY))
                    .forEach(es -> {
                        es.getValue().asString(s -> map.put(es.getKey(), s));
                    });
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
    protected Text newKey(String key) {
        return new Text(key);
    }

    @Override
    protected Text newValue(String value) {
        return new Text(value);
    }
}
