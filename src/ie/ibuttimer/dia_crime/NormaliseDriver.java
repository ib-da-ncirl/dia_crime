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

import ie.ibuttimer.dia_crime.hadoop.ITagger;
import ie.ibuttimer.dia_crime.hadoop.merge.MergeReducer;
import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.hadoop.normalise.NormaliseMapper;
import ie.ibuttimer.dia_crime.hadoop.normalise.NormalisePartitioner;
import ie.ibuttimer.dia_crime.hadoop.normalise.NormaliseReducer;
import ie.ibuttimer.dia_crime.hadoop.regression.RegressionWritable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.util.*;

import static ie.ibuttimer.dia_crime.hadoop.merge.MergeReducer.CRIME_WEATHER_STOCK;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Hadoop driver class for normalisation related jobs
 */
public class NormaliseDriver extends AbstractDriver implements ITagger {

    private static final Logger logger = Logger.getLogger(NormaliseDriver.class);

    public NormaliseDriver(DiaCrimeMain app) {
        super(app);
    }

    public static NormaliseDriver of(DiaCrimeMain app) {
        return new NormaliseDriver(app);
    }

    public static Pair<List<String>, List<String>> getSectionLists() {
        // List<String> sections, List<String> commonSection
        return Pair.of(Collections.singletonList(NORMALISE_PROP_SECTION), List.of());
    }


    public Job getNormaliseJob(Properties properties) throws Exception {

        Pair<List<String>, List<String>> sectionLists = getSectionLists();

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());

        if (resultCode == ECODE_SUCCESS) {
            Map<String, InputCfg> ipSections = new HashMap<>();
            Map<String, OutputCfg> opSections = new HashMap<>();

            ipSections.put(makeSubSectionKey(NORMALISE_PROP_SECTION, CSW_IN_PATH_PROP),
                InputCfg.of(NormaliseMapper.CwsNormaliseMapper.class, CSW_IN_PATH_PROP));
            ipSections.put(makeSubSectionKey(NORMALISE_PROP_SECTION, CS_IN_PATH_PROP),
                InputCfg.of(NormaliseMapper.CsNormaliseMapper.class, CS_IN_PATH_PROP));
            ipSections.put(makeSubSectionKey(NORMALISE_PROP_SECTION, CW_IN_PATH_PROP),
                InputCfg.of(NormaliseMapper.CwNormaliseMapper.class, CW_IN_PATH_PROP));

            opSections.put(makeSubSectionKey(NORMALISE_PROP_SECTION, CRIME_WEATHER_STOCK),
                    OutputCfg.of(TYPES_NAMED_OP, DateWritable.class, Text.class));

            job = initJob("Normalise", conf, ipSections, opSections);

            job.setReducerClass(NormaliseReducer.class);

            // Creates reduce instances
            job.setNumReduceTasks(MergeReducer.MERGE_SECTIONS.size());
            // Set the partitioner class
            job.setPartitionerClass(NormalisePartitioner.class);

            job.setMapOutputKeyClass(DateWritable.class);
            job.setMapOutputValueClass(RegressionWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <DateWritable, RegressionWritable> -> reduce -> <DateWritable, Text> (output)
             */
            job.setOutputKeyClass(DateWritable.class);
            job.setOutputValueClass(Text.class);
        }

        return job;
    }


    public int runNormaliseJob(JobConfig cfg) throws Exception {

        int resultCode = ECODE_FAIL;
        Job job = getNormaliseJob(cfg.properties);
        if (job != null) {
            if (cfg.wait) {
                resultCode = job.waitForCompletion(cfg.verbose) ? ECODE_SUCCESS : ECODE_FAIL;
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
