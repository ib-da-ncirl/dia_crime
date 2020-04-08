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

import ie.ibuttimer.dia_crime.hadoop.crime.CrimeWrapMapper;
import ie.ibuttimer.dia_crime.hadoop.merge.CSWWrapperWritable;
import ie.ibuttimer.dia_crime.hadoop.merge.MergeReducer;
import ie.ibuttimer.dia_crime.hadoop.stock.DowJonesStockWrapMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.NasdaqStockWrapMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.SP500StockWrapMapper;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherWrapMapper;
import ie.ibuttimer.dia_crime.misc.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static ie.ibuttimer.dia_crime.StockDriver.addStockSpecificsToConfig;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Hadoop driver class for merge related jobs
 */
public class MergeDriver extends AbstractDriver {

    private static final Logger logger = Logger.getLogger(MergeDriver.class);

    public MergeDriver(DiaCrimeMain app) {
        super(app);
    }

    public static MergeDriver of(DiaCrimeMain app) {
        return new MergeDriver(app);
    }

    public Job getMergeJob(Properties properties) throws Exception {

        Job job = null;
        Configuration conf = new Configuration();

        AtomicInteger resultCode = new AtomicInteger(ECODE_SUCCESS);
        List.of(StockDriver.getSectionLists(), WeatherDriver.getSectionLists(),
                CrimeDriver.getSectionLists()).forEach(pair -> {

            if (resultCode.get() == ECODE_SUCCESS) {
                int rcode = readConfigs(conf, properties, pair.getLeft(), pair.getRight());
                if (rcode != ECODE_SUCCESS) {
                    resultCode.set(rcode);
                }
            }
        });

        if (resultCode.get() == ECODE_SUCCESS) {
            Map<String, Class<? extends Mapper<?,?,?,?>>> sections = new HashMap<>();

            addStockSpecificsToConfig(conf);

            sections.put(NASDAQ_PROP_SECTION, NasdaqStockWrapMapper.class);
            sections.put(DOWJONES_PROP_SECTION, DowJonesStockWrapMapper.class);
            sections.put(SP500_PROP_SECTION, SP500StockWrapMapper.class);
            sections.put(CRIME_PROP_SECTION, CrimeWrapMapper.class);
            sections.put(WEATHER_PROP_SECTION, WeatherWrapMapper.class);

            job = initJob("Merge", conf, sections);

            job.setReducerClass(MergeReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CSWWrapperWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, CSWWrapperWritable> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        return job;
    }


    public int runMergeJob(JobConfig cfg) throws Exception {

        int resultCode = Constants.ECODE_FAIL;
        Job job = getMergeJob(cfg.properties);
        if (job != null) {
            if (cfg.wait) {
                resultCode = job.waitForCompletion(cfg.verbose) ? Constants.ECODE_SUCCESS : Constants.ECODE_FAIL;
            } else {
                job.submit();
                resultCode = ECODE_RUNNING;
            }
        }

        return resultCode;
    }

}
