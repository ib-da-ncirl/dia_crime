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

package ie.ibuttimer.dia_crime;

import ie.ibuttimer.dia_crime.hadoop.weather.WeatherMapper;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherReducer;
import ie.ibuttimer.dia_crime.misc.Constants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;


public class WeatherDriver extends AbstractDriver {

    private static final Logger logger = Logger.getLogger(WeatherDriver.class);

    public WeatherDriver(DiaCrimeMain app) {
        super(app);
    }

    public static WeatherDriver of(DiaCrimeMain app) {
        return new WeatherDriver(app);
    }

    public int runWeatherJob(Properties properties) throws Exception {

        Pair<Integer, Map<String, Pair<Integer, Configuration>>> configRes = readConfigs(properties,
            Arrays.asList(WEATHER_PROP_SECTION), List.of());

        int resultCode = configRes.getLeft();

        if (resultCode == Constants.ECODE_SUCCESS) {
            Map<String, Pair<Integer, Configuration>> configs = configRes.getRight();
            Map<String, Class<? extends Mapper>> sections = new HashMap<>();
            Map<String, String> tags = new HashMap<>();

            sections.put(WEATHER_PROP_SECTION, WeatherMapper.class);

            Job job = initJob("Weather", configs, sections);

            job.setReducerClass(WeatherReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, MapWritable> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);

            resultCode = job.waitForCompletion(true) ? ECODE_SUCCESS : ECODE_FAIL;

            if (resultCode == ECODE_SUCCESS) {


            }
        }

        return resultCode;
    }

}
