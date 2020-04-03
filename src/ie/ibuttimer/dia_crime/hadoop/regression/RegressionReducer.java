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

package ie.ibuttimer.dia_crime.hadoop.regression;

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RegressionReducer extends AbstractReducer<Text, RegressionWritable<?, ?>, Text, Text> {

    private Counters.ReducerCounter counter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        counter = getCounter(context, CountersEnum.REGRESSION_REDUCER_COUNT);

        long x = context.getCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        ++x;
    }

    @Override
    protected void reduce(Text key, Iterable<RegressionWritable<?, ?>> values, Context context) throws IOException, InterruptedException {

//        Map<String, RegressionWritable<String, Object>> map = new HashMap<>();
//        values.forEach(v -> {
//            map.put(key.toString(), (RegressionWritable<String, Object>) v);
//        });
    }
}
