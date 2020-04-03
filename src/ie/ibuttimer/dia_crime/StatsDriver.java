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

import ie.ibuttimer.dia_crime.hadoop.io.FileUtil;
import ie.ibuttimer.dia_crime.hadoop.stats.*;
import ie.ibuttimer.dia_crime.misc.Constants;
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.Functional.exceptionLoggingConsumer;


public class StatsDriver extends AbstractDriver {

    private static final Logger logger = Logger.getLogger(StatsDriver.class);

    public StatsDriver(DiaCrimeMain app) {
        super(app);
    }

    public static StatsDriver of(DiaCrimeMain app) {
        return new StatsDriver(app);
    }

    public static Pair<List<String>, List<String>> getSectionLists() {
        // List<String> sections, List<String> commonSection
        return Pair.of(Collections.singletonList(STATS_PROP_SECTION), List.of());
    }


    public Job getStatsJob(Properties properties) throws Exception {

        Pair<List<String>, List<String>> sectionLists = getSectionLists();

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());

        if (resultCode == ECODE_SUCCESS) {
            Map<String, Class<? extends Mapper<?,?,?,?>>> sections = new HashMap<>();

            sections.put(STATS_PROP_SECTION, StatsMapper.class);

            job = initJob("Stats", conf, sections);

            job.setReducerClass(StatsReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Value.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, Value> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        return job;
    }


    public int runStatsJob(Properties properties) throws Exception {

        int resultCode = Constants.ECODE_FAIL;
        Job job = getStatsJob(properties);
        if (job != null) {
            resultCode = job.waitForCompletion(true) ? Constants.ECODE_SUCCESS : Constants.ECODE_FAIL;

            if (resultCode == ECODE_SUCCESS) {

                PropertyWrangler propertyWrangler = new PropertyWrangler();

                Configuration conf = job.getConfiguration();

                StatsConfigReader cfgReader = new StatsConfigReader(StatsMapper.getCsvEntryMapperCfg());

                List<String> variables = cfgReader.readVariables(conf);
                List<String> numericTypes = cfgReader.getNumericFields(conf);

                getSectionLists().getLeft().forEach(section -> {
                    propertyWrangler.setRoot(section);
                    Path outDir = new Path(conf.get(propertyWrangler.getPropertyPath(OUT_PATH_PROP)));
                    StatsCalc statsCalc = new StatsCalc(outDir, conf, "part-r-00000");
                    String outPath = conf.get(propertyWrangler.getPropertyPath(STATS_PATH_PROP), section + "_stats.txt");
                    FileUtil fileUtil = new FileUtil(outDir, conf);

                    List<String> outputText = new ArrayList<>();

                    variables.forEach(
                        exceptionLoggingConsumer(tag -> {
                            Result.Set results = statsCalc.calcAll(tag, numericTypes);

                            Result result = results.get(tag);
                            if (result.isSuccess()) {

                                outputText.add(tag);

                                Arrays.asList(StatsCalc.Stat.values()).forEach(stat -> {
                                    switch (stat) {
                                        case STDDEV:
                                            result.getStddev().ifPresent(st -> outputText.add("Standard deviation: " + st));
                                            break;
                                        case VARIANCE:
                                            result.getVariance().ifPresent(st -> outputText.add("Variance: " + st));
                                            break;
                                        case MEAN:
                                            result.getMean().ifPresent(st -> outputText.add("Mean: " + st));
                                            break;
                                        case MIN:
                                            result.getMin().ifPresent(st -> outputText.add("Min: " + st));
                                            break;
                                        case MAX:
                                            result.getMax().ifPresent(st -> outputText.add("Max: " + st));
                                            break;
                                    }
                                });
                                outputText.add("");
                            }
                        }, IOException.class, logger)
                    );
                    try (FSDataOutputStream stream = fileUtil.fileWriteOpen(outPath, true)) {
                        fileUtil.write(stream, outputText);
                        fileUtil.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

            }
        }

        return resultCode;
    }

}
