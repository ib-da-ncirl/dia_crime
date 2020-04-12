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
import ie.ibuttimer.dia_crime.hadoop.io.FileReader;
import ie.ibuttimer.dia_crime.hadoop.regression.*;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import ie.ibuttimer.dia_crime.misc.Constants;
import ie.ibuttimer.dia_crime.misc.DebugLevel;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.HADOOP_KEY_VAL;
import static ie.ibuttimer.dia_crime.misc.Utils.getSpacedDialog;

/**
 * Hadoop driver class for linear regression related jobs
 */
public class LinearRegressionDriver extends AbstractDriver implements ITagger {

    private static final Logger logger = Logger.getLogger(LinearRegressionDriver.class);

    public LinearRegressionDriver(DiaCrimeMain app) {
        super(app);
    }

    public static LinearRegressionDriver of(DiaCrimeMain app) {
        return new LinearRegressionDriver(app);
    }

    public static Pair<List<String>, List<String>> getSectionLists() {
        // List<String> sections, List<String> commonSection
        return Pair.of(Collections.singletonList(REGRESSION_PROP_SECTION), List.of());
    }

    public static Pair<List<String>, List<String>> getVerificationSectionLists() {
        // List<String> sections, List<String> commonSection
        return Pair.of(List.of(VERIFICATION_PROP_SECTION), List.of(REGRESSION_PROP_SECTION));
    }


    public Job getLinearRegressionJob(Properties properties) throws Exception {

        Pair<List<String>, List<String>> sectionLists = getSectionLists();

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());

        if (resultCode == ECODE_SUCCESS) {
            Map<String, SectionCfg> sections = new HashMap<>();

            sections.put(REGRESSION_PROP_SECTION, SectionCfg.of(RegressionTrainMapper.class));

            job = initJob("Linear Regression", conf, sections);

            job.setReducerClass(RegressionTrainReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(RegressionWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, RegressionWritable> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        return job;
    }

    public int runLinearRegressionJob(JobConfig cfg) throws Exception {

        ConfigReader cfgReader = new ConfigReader(REGRESSION_PROP_SECTION);
        long maxEpochs = 0;
        long epoch = 0;
        double targetCost = 0;
        double steadyTarget = 0;
        int steadyLimit = 0;
        double cost = Double.MAX_VALUE;
        int resultCode = ECODE_FAIL;

        int steadyCount = 0;
        Map<String, Long> lastCoef = new HashMap<>();
        Map<String, Long> thisCoef = new HashMap<>();

        if (cfg.wait) {
            Map<String, String> epochSetting = null;
            do {
                Job job = getLinearRegressionJob(cfg.properties);
                if (job != null) {
                    Configuration conf = job.getConfiguration();
                    if (epoch == 0) {
                        maxEpochs = cfgReader.getConfigProperty(conf, EPOCH_LIMIT_PROP, 0L).longValue();
                        targetCost = cfgReader.getConfigProperty(conf, TARGET_COST_PROP, 0.0).doubleValue();
                        steadyTarget = cfgReader.getConfigProperty(conf, STEADY_TARGET_PROP, 0.0).doubleValue();
                        steadyLimit = cfgReader.getConfigProperty(conf, STEADY_LIMIT_PROP, 0).intValue();

                        steadyTarget = Math.pow(10, steadyTarget);

                        clearResults(job, cfgReader);
                    } else {
                        if (steadyTarget > 0 && steadyLimit > 0) {
                            double finalSteadyTarget = steadyTarget;
                            epochSetting.forEach((key, value) -> {
                                Double val = Double.parseDouble(value) * finalSteadyTarget;
                                thisCoef.put(key, val.longValue());
                            });
                            AtomicInteger match = new AtomicInteger(0);
                            thisCoef.forEach((key, value) -> {
                                if (value.equals(lastCoef.get(key))) {
                                    match.incrementAndGet();
                                }
                                lastCoef.put(key, value);
                            });
                            if (match.get() == thisCoef.keySet().size()) {
                                ++steadyCount;
                                if (steadyCount == steadyLimit) {
                                    break;
                                }
                            } else {
                                steadyCount = 0;
                            }
                        }

                        // update settings
                        epochSetting.forEach((key, value) -> conf.set(cfgReader.getPropertyPath(key), value));
                    }

                    ++epoch;
                    conf.set(cfgReader.getPropertyPath(CURRENT_EPOCH_PROP), Long.toString(epoch));
                    conf.set(CONF_PROPERTY_ROOT, cfgReader.getRoot());

                    logger.info(getSpacedDialog(String.format("Running epoch %d of a maximum of %d", epoch, maxEpochs)));

                    resultCode = job.waitForCompletion(cfg.verbose) ? ECODE_SUCCESS : ECODE_FAIL;

                    if (resultCode == ECODE_SUCCESS) {
                        epochSetting = regressionJobReport(job, cfgReader, epoch);
                    } else {
                        break;
                    }
                } else {
                    break;
                }

            } while ((epoch < maxEpochs) && (cost > targetCost));
        } else {
            Job job = getLinearRegressionJob(cfg.properties);
            if (job != null) {
                job.submit();
                resultCode = ECODE_RUNNING;
            }
        }

        return resultCode;
    }

    private Map<String, String> regressionJobReport(Job job, ConfigReader cfgReader, long epoch) throws IOException {

        Map<String, String> result = new HashMap<>();

        Configuration conf = job.getConfiguration();
        DebugLevel debugLevel = DebugLevel.getSetting(conf, cfgReader);
        boolean showHighDebug = DebugLevel.HIGH.showMe(debugLevel);

        Path outDir = new Path(conf.get(cfgReader.getPropertyPath(OUT_PATH_PROP)));
        FileReader reader = new FileReader(outDir, conf);

        if (reader.wasSuccess()) {
            // read result
            reader.open("part-r-00000");
            List<String> lines = reader.getAllLines(l -> !l.startsWith(COMMENT_PREFIX));
            reader.close();

            if (epoch == 1) {
                // add param info header
                List<String> linesPlus = new ArrayList<>();
                List<String> rawLines = new ArrayList<>(getTagStrings(conf, cfgReader));
                List.of(FILTER_START_DATE_PROP, FILTER_END_DATE_PROP, TRAIN_START_DATE_PROP, TRAIN_END_DATE_PROP,
                        INDEPENDENTS_PROP, DEPENDENT_PROP)
                    .forEach(p -> {
                        String setting = cfgReader.getConfigProperty(conf, p, "");
                        rawLines.add(String.format("%s : %s", p, setting));
                    });
                rawLines.forEach(l -> linesPlus.add(String.format("%s %s", COMMENT_PREFIX, l)));
                lines = linesPlus;
            }

            // append to result history
            Files.write(Paths.get(getResultsPath(job, cfgReader)), lines, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            // update config for next epoch
            lines.stream().findFirst().ifPresent(l -> {
                if (!l.startsWith(COMMENT_PREFIX)) {
                    Pair<String, String> keyVal = HADOOP_KEY_VAL.destringifyElement(l);

                    logger.info(getSpacedDialog(String.format("Epoch %s - %s", keyVal.getLeft(), keyVal.getRight())));

                    Map<String, String> map = MapStringifier.mapify(keyVal.getRight());

                    List.of(WEIGHT_PROP, BIAS_PROP).forEach(prop -> {
                        result.put(prop, map.get(prop));
                    });
                }
            });
        }
        return result;
    }

    private void clearResults(Job job, ConfigReader cfgReader) throws IOException {
        Files.deleteIfExists(Paths.get(getResultsPath(job, cfgReader)));
    }
    private String getResultsPath(Job job, ConfigReader cfgReader) {
        return job.getConfiguration().get(cfgReader.getPropertyPath(TRAIN_OUTPUT_PATH_PROP), "regression.txt");
    }


    public Job getRegressionValidationJob(Properties properties) throws Exception {

        Pair<List<String>, List<String>> sectionLists = getVerificationSectionLists();

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());

        if (resultCode == ECODE_SUCCESS) {
            Map<String, SectionCfg> sections = new HashMap<>();


            // load weight & bias from result of regression job
            ConfigReader cfgReader = new ConfigReader(VERIFICATION_PROP_SECTION);
            String regPath = conf.get(cfgReader.getPropertyPath(VALIDATE_MODEL_PATH_PROP), "");
            FileReader fileReader = new FileReader(new Path(regPath), conf);

            List<String> lines = fileReader.open().getAllLines(); // should only be 1 line
            fileReader.close();

            lines.stream().
                filter(l -> !l.startsWith(COMMENT_PREFIX)).
                findFirst().ifPresent(l -> {
                    Pair<String, String> keyVal = HADOOP_KEY_VAL.destringifyElement(l);
                    Map<String, String> map = MapStringifier.mapify(keyVal.getRight());

                    List.of(WEIGHT_PROP, BIAS_PROP).forEach(prop -> {
                        String path = cfgReader.getPropertyPath(prop);
                        String value = map.get(prop);

                        updateConfiguration(conf, path, value, cfgReader);
                    });
                });

            sections.put(VERIFICATION_PROP_SECTION, SectionCfg.of(RegressionValidateMapper.class));

            job = initJob("Regression Verification", conf, sections);

            job.setReducerClass(RegressionValidateReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(RegressionWritable.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, RegressionWritable> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        }

        return job;
    }

    public int runRegressionValidationJob(JobConfig cfg) throws Exception {

        int resultCode = Constants.ECODE_FAIL;
        Job job = getRegressionValidationJob(cfg.properties);
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
