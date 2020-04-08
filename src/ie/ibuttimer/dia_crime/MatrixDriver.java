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

import ie.ibuttimer.dia_crime.hadoop.matrix.CoordinateWritable;
import ie.ibuttimer.dia_crime.hadoop.matrix.MatrixMapper;
import ie.ibuttimer.dia_crime.hadoop.matrix.MatrixReducer;
import ie.ibuttimer.dia_crime.hadoop.matrix.MatrixWritable;
import ie.ibuttimer.dia_crime.misc.Constants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Hadoop driver class for matrix related jobs
 */
public class MatrixDriver extends AbstractDriver {

    private static final Logger logger = Logger.getLogger(MatrixDriver.class);

    public MatrixDriver(DiaCrimeMain app) {
        super(app);
    }

    public static MatrixDriver of(DiaCrimeMain app) {
        return new MatrixDriver(app);
    }

    public static Pair<List<String>, List<String>> getSectionLists() {
        // List<String> sections, List<String> commonSection
        return Pair.of(List.of(MATRIX_PROP_1_SECTION, MATRIX_PROP_2_SECTION), List.of());
    }

    public Job getMatrixJob(Properties properties) throws Exception {

        Pair<List<String>, List<String>> sectionLists = getSectionLists();

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());

        if (resultCode == Constants.ECODE_SUCCESS) {
            Map<String, Class<? extends Mapper<?,?,?,?>>> sections = new HashMap<>();
            Map<String, String> tags = new HashMap<>();

            sections.put(MATRIX_PROP_1_SECTION, MatrixMapper.MatrixMapper1.class);
            sections.put(MATRIX_PROP_2_SECTION, MatrixMapper.MatrixMapper2.class);

            job = initJob("Matrix", conf, sections);

            job.setReducerClass(MatrixReducer.class);

            job.setMapOutputKeyClass(CoordinateWritable.class);
            job.setMapOutputValueClass(MatrixWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <CoordinateWritable, MatrixWritable> -> reduce -> <Text, DoubleWritable> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        }

        return job;
    }

    public int runMatrixJob(JobConfig cfg) throws Exception {

        int resultCode = ECODE_FAIL;
        Job job = getMatrixJob(cfg.properties);
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

}
