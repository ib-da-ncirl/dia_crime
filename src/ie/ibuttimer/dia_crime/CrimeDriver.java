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

import ie.ibuttimer.dia_crime.hadoop.crime.CrimeMapper;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeReducer;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.misc.Constants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.util.*;

import static ie.ibuttimer.dia_crime.hadoop.merge.MergeReducer.CRIME_WEATHER_STOCK;
import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.Constants.ECODE_RUNNING;

/**
 * Hadoop driver class for crime related jobs
 */
public class CrimeDriver extends AbstractDriver {

    private static final Logger logger = Logger.getLogger(CrimeDriver.class);

    public CrimeDriver(DiaCrimeMain app) {
        super(app);
    }

    public static CrimeDriver of(DiaCrimeMain app) {
        return new CrimeDriver(app);
    }

    public static Pair<List<String>, List<String>> getSectionLists() {
        // List<String> sections, List<String> commonSection
        return Pair.of(Collections.singletonList(CRIME_PROP_SECTION), List.of());
    }

    public Job getCrimeJob(Properties properties) throws Exception {

        Pair<List<String>, List<String>> sectionLists = getSectionLists();

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());;

        if (resultCode == Constants.ECODE_SUCCESS) {
            Map<String, InputCfg> ipSections = new HashMap<>();
            Map<String, OutputCfg> opSections = new HashMap<>();

            ipSections.put(CRIME_PROP_SECTION, InputCfg.of(CrimeMapper.class));

            opSections.put(CRIME_PROP_SECTION, OutputCfg.of(TYPES_NAMED_OP, DateWritable.class, Text.class));

            job = initJob("Crime", conf, ipSections, opSections);

            job.setReducerClass(CrimeReducer.class);

            job.setMapOutputKeyClass(DateWritable.class);
            job.setMapOutputValueClass(MapWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <DateWritable, MapWritable> -> reduce -> <DateWritable, Text> (output)
             */
            job.setOutputKeyClass(DateWritable.class);
            job.setOutputValueClass(Text.class);
        }

        return job;
    }


    public int runCrimeJob(JobConfig cfg) throws Exception {

        int resultCode = Constants.ECODE_FAIL;
        Job job = getCrimeJob(cfg.properties);
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

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
