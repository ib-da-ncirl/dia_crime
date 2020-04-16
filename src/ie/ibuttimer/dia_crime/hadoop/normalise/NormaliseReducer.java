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
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer.formatOutputTypes;
import static ie.ibuttimer.dia_crime.hadoop.normalise.NormaliseMapper.PARTITION;
import static ie.ibuttimer.dia_crime.misc.Constants.NORMALISE_PROP_SECTION;
import static ie.ibuttimer.dia_crime.misc.Constants.TYPES_NAMED_OP;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.COMMA;
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

    private Map<String, OpTypeEntry> outputTypes;

    private MultipleOutputs<DateWritable, Text> mos;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        setSection(NORMALISE_PROP_SECTION);

        super.setup(context);

        counter = getCounter(context, CountersEnum.NORMALISE_REDUCER_COUNT);

        outputTypes = new TreeMap<>();

        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(DateWritable key, Iterable<RegressionWritable<String, Value>> values, Context context) throws IOException, InterruptedException {

        addOutputHeader(context, counter, List.of(), List.of());

        counter.increment();

        values.forEach(value -> {
            if (!key.equals(DateWritable.MIN)) {
                // standard line
                Map<String, String> outMap = new TreeMap<>();
                value.entrySet().stream()
                    .filter(es -> !es.getKey().equals(PARTITION))   // remove the info used to partition the mapper output
                    .forEach(es -> es.getValue().asString(s -> {
                        outMap.put(es.getKey(), s);
                    }));

                try {
                    context.write(key, new Text(MAP_STRINGIFIER.stringify(outMap)));

                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else {    // output types
                // load output type from input
                StatsConfigReader cfgReader = new StatsConfigReader(getSection());
                Map<String, Pair<String, String>> map = new HashMap<>();

                value.forEach((key1, value1) -> value1.asString(s -> map.put(key1, COMMA.destringifyElement(s))));

                map.remove(PARTITION);  // remove the info used to partition the mapper output

                outputTypes = cfgReader.convertOutputTypeClasses(map);

                formatOutputTypes(this)
                    .forEach(s -> {
                        try {
                            write(mos, TYPES_NAMED_OP, DateWritable.COMMENT_KEY, new Text(s));
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
            }
        });
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        mos.close();
    }

    @Override
    public Map<String, OpTypeEntry> getOutputTypeMap() {
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
