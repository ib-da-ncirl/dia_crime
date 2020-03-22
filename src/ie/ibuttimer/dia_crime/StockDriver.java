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
import ie.ibuttimer.dia_crime.hadoop.stats.Result;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsCalc;
import ie.ibuttimer.dia_crime.hadoop.stock.*;
import ie.ibuttimer.dia_crime.misc.Constants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static ie.ibuttimer.dia_crime.misc.Constants.*;


public class StockDriver {

    private static final Logger logger = Logger.getLogger(StockDriver.class);

    private DiaCrimeMain app;

    public StockDriver(DiaCrimeMain app) {
        this.app = app;
    }

    public int runStockJob(Properties properties) throws Exception {

        Pair<Integer, Map<String, Pair<Integer, Configuration>>> configRes = readConfigs(properties,
            Arrays.asList(NASDAQ_PROP_SECTION, DOWJONES_PROP_SECTION, SP500_PROP_SECTION));

        int resultCode = configRes.getLeft();

        if (resultCode == Constants.ECODE_SUCCESS) {
            Map<String, Pair<Integer, Configuration>> configs = configRes.getRight();

            Configuration nasdaqConf = configs.get(NASDAQ_PROP_SECTION).getRight();
            Configuration dowjonesSetup = configs.get(DOWJONES_PROP_SECTION).getRight();
            Configuration sp500Setup = configs.get(SP500_PROP_SECTION).getRight();

            Job job = Job.getInstance(nasdaqConf);
            job.setJarByClass(StockDriver.class);
            job.setJobName("Stocks");

            MultipleInputs.addInputPath(job, new Path(nasdaqConf.get(IN_PATH_PROP)),
                    TextInputFormat.class, NasdaqStockEntryMapper.class);
            MultipleInputs.addInputPath(job, new Path(dowjonesSetup.get(IN_PATH_PROP)),
                    TextInputFormat.class, DowJonesStockEntryMapper.class);
            MultipleInputs.addInputPath(job, new Path(sp500Setup.get(IN_PATH_PROP)),
                    TextInputFormat.class, SP500StockEntryMapper.class);

            FileOutputFormat.setOutputPath(job, new Path(nasdaqConf.get(OUT_PATH_PROP)));

            job.setReducerClass(StockEntryReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, MapWritable> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);

            resultCode = job.waitForCompletion(true) ? Constants.ECODE_SUCCESS : Constants.ECODE_FAIL;
        }

        return resultCode;
    }

    public Job initJob(String name, Map<String, Pair<Integer, Configuration>> configs,
                       Map<String, Class<? extends Mapper>> sections) throws Exception {

        if (sections.size() == 0) {
            throw new IllegalArgumentException("No sections specified");
        }
        if (sections.size() != configs.size()) {
            throw new IllegalArgumentException("Number of sections and configurations does not match");
        }

        // doesn't matter which config is used for job
        Configuration primaryConf = configs.entrySet().iterator().next().getValue().getRight();
        Job job = Job.getInstance(primaryConf);
        job.setJarByClass(StockDriver.class);
        job.setJobName(name);

        String outPath = primaryConf.get(OUT_PATH_PROP);
        if (sections.size() > 1) {
            // multiple inputs
            Map<String, String> outputPaths = new HashMap<>();
            sections.forEach((s, cls) -> {
                Configuration conf = configs.get(s).getRight();
                MultipleInputs.addInputPath(job, new Path(conf.get(IN_PATH_PROP)), TextInputFormat.class, cls);

                outputPaths.put(s, conf.get(OUT_PATH_PROP));
            });

            // check all out paths are the same
            for (String key : outputPaths.keySet()) {
                if (!outPath.equals(outputPaths.get(key))) {
                    outPath = null;
                    break;
                }
            }
        } else {
            FileInputFormat.addInputPath(job, new Path(primaryConf.get(IN_PATH_PROP)));
        }

        if (outPath != null) {
            FileOutputFormat.setOutputPath(job, new Path(outPath));
        } else {
            // TODO MultipleOutputs
            throw new UnsupportedOperationException("Multiple output path not currently supported");
        }

        return job;
    }

    public int runStockStatsJob(Properties properties) throws Exception {

        Pair<Integer, Map<String, Pair<Integer, Configuration>>> configRes = readConfigs(properties,
            Arrays.asList(NASDAQ_PROP_SECTION, DOWJONES_PROP_SECTION, SP500_PROP_SECTION));

        int resultCode = configRes.getLeft();

        if (resultCode == Constants.ECODE_SUCCESS) {
            Map<String, Pair<Integer, Configuration>> configs = configRes.getRight();
            Map<String, Class<? extends Mapper>> sections = new HashMap<>();
            Map<String, String> tags = new HashMap<>();

            sections.put(NASDAQ_PROP_SECTION, NasdaqStockEntryStatsMapper.class);
            sections.put(DOWJONES_PROP_SECTION, DowJonesStockEntryStatsMapper.class);
            sections.put(SP500_PROP_SECTION, SP500StockEntryStatsMapper.class);
            tags.put(NASDAQ_PROP_SECTION, NASDAQ_ID);
            tags.put(DOWJONES_PROP_SECTION, DOWJONES_ID);
            tags.put(SP500_PROP_SECTION, SP500_ID);

            Job job = initJob("StocksStats", configs, sections);

            job.setReducerClass(StockEntryStatsReducer.class);

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

                configs.forEach((key, value) -> {
                    Configuration conf = value.getRight();
                    Path outDir = new Path(value.getRight().get(OUT_PATH_PROP));
                    StatsCalc statsCalc = new StatsCalc(outDir, conf, "part-r-00000");
                    String outPath = conf.get(STATS_PATH_PROP, key + "_stats.txt");
                    FileUtil fileUtil = new FileUtil(outDir, conf);

                    Result.Set results = null;
                    try {
                        results = statsCalc.calcAll(tags.get(key), BigStockEntryWritable.NUMERIC_FIELDS);
                        try (FSDataOutputStream stream = fileUtil.fileWriteOpen(outPath, true)) {

                            List<String> outputText = new ArrayList<>();

                            Result.Set finalResults = results;
                            BigStockEntryWritable.NUMERIC_FIELDS.forEach(f -> {
                                Result result = finalResults.get(f.name());
                                if (result.isSuccess()) {

                                    outputText.add(f.name());

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
                            });
                            try {
                                fileUtil.write(stream, outputText);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            fileUtil.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

            }
        }

        return resultCode;
    }

    public int runStockAvgJob(Properties properties) throws Exception {

        Pair<Integer, Configuration> nasdaqSetup = app.setupJob(properties,
                Pair.of(NASDAQ_PROP_SECTION, Collections.singletonList(STOCK_PROP_SECTION)));
//        Pair<Integer, Configuration> dowjonesSetup = app.setupJob(properties,
//                Pair.of(DOWJONES_PROP_SECTION, Collections.singletonList(STOCK_PROP_SECTION)));
//        Pair<Integer, Configuration> sp500Setup = app.setupJob(properties,
//                Pair.of(SP500_PROP_SECTION, Collections.singletonList(STOCK_PROP_SECTION)));

        int resultCode = ECODE_CONFIG_ERROR;
//        for (Pair<Integer, Configuration> setup : Arrays.asList(nasdaqSetup, dowjonesSetup, sp500Setup)) {
        for (Pair<Integer, Configuration> setup : Arrays.asList(nasdaqSetup)) {
            resultCode = setup.getLeft();
            if (resultCode != ECODE_SUCCESS) {
                break;
            }
        }

        if (resultCode == Constants.ECODE_SUCCESS) {
            Configuration nasdaqConf = nasdaqSetup.getRight();

            Job job = Job.getInstance(nasdaqConf);
            job.setJarByClass(StockDriver.class);
            job.setJobName("StocksAvg");

//            MultipleInputs.addInputPath(job, new Path(nasdaqConf.get(IN_PATH_PROP)),
//                    TextInputFormat.class, NasdaqStockEntryMapper.class);
//            MultipleInputs.addInputPath(job, new Path(dowjonesSetup.getRight().get(IN_PATH_PROP)),
//                    TextInputFormat.class, DowJonesStockEntryMapper.class);
//            MultipleInputs.addInputPath(job, new Path(sp500Setup.getRight().get(IN_PATH_PROP)),
//                    TextInputFormat.class, SP500StockEntryMapper.class);

            FileInputFormat.addInputPath(job, new Path(nasdaqConf.get(IN_PATH_PROP)));
            FileOutputFormat.setOutputPath(job, new Path(nasdaqConf.get(OUT_PATH_PROP)));

            job.setMapperClass(NasdaqStockEntryAvgMapper.class);
            job.setCombinerClass(StockEntryAvgCombiner.class);
            job.setReducerClass(StockEntryAvgReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, MapWritable> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);

            resultCode = job.waitForCompletion(true) ? Constants.ECODE_SUCCESS : Constants.ECODE_FAIL;
        }

        return resultCode;
    }

    private Pair<Integer, Map<String, Pair<Integer, Configuration>>> readConfigs(Properties properties, List<String> sections) {

        // There is duplication of common property reading for stocks, unnecessary but convenient

        Map<String, Pair<Integer, Configuration>> configs = new HashMap<>();

        sections.forEach(s -> {
            try {
                configs.put(s, app.setupJob(properties, Pair.of(s, Collections.singletonList(STOCK_PROP_SECTION))));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        int resultCode = ECODE_CONFIG_ERROR;
        for (String id : configs.keySet()) {
            resultCode = configs.get(id).getLeft();
            if (resultCode != ECODE_SUCCESS) {
                break;
            }
        }

        return Pair.of(resultCode, configs);
    }

}
