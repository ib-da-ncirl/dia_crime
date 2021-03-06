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
import ie.ibuttimer.dia_crime.misc.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static ie.ibuttimer.dia_crime.hadoop.regression.AbstractRegressionMapper.WEIGHT_KV_SEPARATOR;
import static ie.ibuttimer.dia_crime.hadoop.regression.AbstractRegressionMapper.WEIGHT_SEPARATOR;
import static ie.ibuttimer.dia_crime.hadoop.regression.RegressionTrainReducer.COST;
import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.HADOOP_KEY_VAL;
import static ie.ibuttimer.dia_crime.misc.MapStringifier.MAP_STRINGIFIER;
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

        // update training path if required
        updatePropertyWithTimestamp(properties, TRAIN_OUTPUT_PATH_PROP, PropertyWrangler.of(REGRESSION_PROP_SECTION));

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());

        if (resultCode == ECODE_SUCCESS) {
            Map<String, InputCfg> ipSections = new HashMap<>();

            ipSections.put(REGRESSION_PROP_SECTION, InputCfg.of(RegressionTrainMapper.class));

            job = initJob("Linear Regression", conf, ipSections);

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
        int increaseLimit = 0;
        int steadyDecimalPlaces = 0;
        double cost;
        long consecutiveCount = 0;
        Pair<Long, Double> min = Pair.of(0L, Double.MAX_VALUE);;
        int resultCode = ECODE_FAIL;
        String terminateCondition = null;
        boolean terminate = false;
        int steadyCount = 0;
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime = null;
        Map<String, Long> lastCoef = new HashMap<>();
        Map<String, Long> thisCoef = new HashMap<>();

        if (cfg.wait) {
            Map<String, String> epochSetting = null;
            do {
                if (steadyTarget > 0 && steadyLimit > 0) {
                    // check for steady state
                    double finalSteadyTarget = steadyTarget;
                    BiConsumer<String, String> biConsumer = (key, value) -> {
                        Double val = Double.parseDouble(value) * finalSteadyTarget;
                        thisCoef.put(key, val.longValue());
                    };
                    epochSetting.forEach((key, value) -> {
                        if (key.equals(WEIGHT_PROP)) {
                            MapStringifier.of(WEIGHT_SEPARATOR, WEIGHT_KV_SEPARATOR).mapify(value)
                                .forEach(biConsumer);
                        } else {
                            biConsumer.accept(key, value);
                        }
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
                            terminateCondition = String.format("Steady state condition satisfied at %d decimal places for %d epochs",
                                steadyDecimalPlaces, steadyLimit);
                            break;
                        }
                    } else {
                        steadyCount = 0;
                    }
                }
                if (endTime != null) {
                    if (LocalDateTime.now().isAfter(endTime)) {
                        terminateCondition = String.format("Run time expired (%d min)",
                            ChronoUnit.MINUTES.between(startTime, LocalDateTime.now()));
                        break;
                    }
                }

                Job job = getLinearRegressionJob(cfg.properties);
                if (job != null) {
                    Configuration conf = job.getConfiguration();
                    if (epoch == 0) {
                        maxEpochs = cfgReader.getConfigProperty(conf, EPOCH_LIMIT_PROP, 0L).longValue();
                        targetCost = cfgReader.getConfigProperty(conf, TARGET_COST_PROP, 0.0).doubleValue();
                        steadyDecimalPlaces = cfgReader.getConfigProperty(conf, STEADY_TARGET_PROP, 0.0).intValue();
                        steadyLimit = cfgReader.getConfigProperty(conf, STEADY_LIMIT_PROP, 0).intValue();
                        increaseLimit = cfgReader.getConfigProperty(conf, INCREASE_LIMIT_PROP, 0).intValue();
                        int minutes = cfgReader.getConfigProperty(conf, TARGET_TIME_PROP, 0).intValue();

                        if (minutes > 0) {
                            endTime = LocalDateTime.now().plus(Duration.ofMinutes(minutes));
                        }

                        steadyTarget = Math.pow(10, steadyDecimalPlaces);

                        clearResults(job, cfgReader);
                    } else {
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

                        cost = Double.parseDouble(epochSetting.get(COST));

                        if (cost < min.getRight()) {
                            min = Pair.of(epoch, cost);
                            if (consecutiveCount > 0) {
                                consecutiveCount = 0;   // end consecutive increase
                            } else {
                                --consecutiveCount;
                            }
                        } else if (consecutiveCount < 0) {
                            consecutiveCount = 0;   // end consecutive decrease
                        } else {
                            ++consecutiveCount;
                            if (consecutiveCount >= increaseLimit) {
                                terminateCondition = "Consecutive cost increase limit exceeded";
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    break;
                }

                terminate = true;
                if (cost <= targetCost) {
                    terminateCondition = String.format("Target cost condition satisfied at %f", cost);
                } else if (epoch >= maxEpochs) {
                    terminateCondition = String.format("Max epoch condition satisfied at %d", epoch);
                } else {
                    terminate = false;
                }
            } while (!terminate);

            if (terminateCondition != null) {
                List<String> msg = List.of("Regression Complete", terminateCondition,
                    String.format("Minimum cost [%f] identified at epoch %d", min.getRight(), min.getLeft()),
                    epochSetting.toString()
                );
                logger.info(Utils.getDialog(msg));

            }
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

        // may take a moment for filesystem to catch up, so if not 'wasSuccess' wait a tick
        if (!reader.wasSuccess()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (reader.wasSuccess()) {
            // read result
            reader.open("part-r-00000");
            String timestamp = LocalDateTime.now().toString();
            List<String> lines = reader.getAllLines(l -> !l.startsWith(COMMENT_PREFIX),
                                                    l -> timestamp + "  " + l);

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
                linesPlus.addAll(lines);
                lines = linesPlus;
            }

            // append to result history
            Files.write(Paths.get(getResultsPath(job, cfgReader)), lines, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            // update config for next epoch
            lines.stream()
                .filter(l -> !l.startsWith(COMMENT_PREFIX))
                .findFirst().ifPresent(l -> {
                    Pair<String, String> keyVal = HADOOP_KEY_VAL.destringifyElement(l);

                    logger.info(getSpacedDialog(String.format("Epoch %s - %s", keyVal.getLeft(), keyVal.getRight())));

                    Map<String, String> map = MAP_STRINGIFIER.mapify(keyVal.getRight());

                    List.of(WEIGHT_PROP, BIAS_PROP, COST).forEach(prop -> {
                        result.put(prop, map.get(prop));
                    });
                });
        }
        return result;
    }

    private void clearResults(Job job, ConfigReader cfgReader) throws IOException {
        Files.deleteIfExists(Paths.get(getResultsPath(job, cfgReader)));
    }
    private String getResultsPath(Job job, ConfigReader cfgReader) {
        return getProperty(job, cfgReader, TRAIN_OUTPUT_PATH_PROP, "regression.txt");
    }

    private String getProperty(Job job, ConfigReader cfgReader, String property, String dfltValue) {
        return job.getConfiguration().get(cfgReader.getPropertyPath(property), dfltValue);
    }


    public Job getRegressionValidationJob(Properties properties) throws Exception {

        Pair<List<String>, List<String>> sectionLists = getVerificationSectionLists();

        // update training path if required
        updatePropertyWithTimestamp(properties, VERIFY_OUTPUT_PATH_PROP, PropertyWrangler.of(VERIFICATION_PROP_SECTION));

        Job job = null;
        Configuration conf = new Configuration();
        int resultCode = readConfigs(conf, properties, sectionLists.getLeft(), sectionLists.getRight());

        if (resultCode == ECODE_SUCCESS) {
            Map<String, InputCfg> ipSections = new HashMap<>();


            // load weight & bias from result of regression job
            ConfigReader cfgReader = new ConfigReader(VERIFICATION_PROP_SECTION);
            String regPath = conf.get(cfgReader.getPropertyPath(VALIDATE_MODEL_PATH_PROP), "");
            FileReader fileReader = new FileReader(new Path(regPath), conf);

            List<String> lines = fileReader.open().getAllLines(); // should only be 1 line
            fileReader.close();

            lines.stream().
                filter(l -> !TextUtils.isEmpty(l)).
                filter(l -> !l.startsWith(COMMENT_PREFIX)).
                findFirst().ifPresent(l -> {
                    Pair<String, String> keyVal = HADOOP_KEY_VAL.destringifyElement(l);
                    Map<String, String> map = MAP_STRINGIFIER.mapify(keyVal.getRight());

                    List.of(WEIGHT_PROP, BIAS_PROP).forEach(prop -> {
                        String path = cfgReader.getPropertyPath(prop);
                        String value = map.get(prop);

                        updateConfiguration(conf, path, value, cfgReader);
                    });
                });

            ipSections.put(VERIFICATION_PROP_SECTION, InputCfg.of(RegressionValidateMapper.class));

            job = initJob("Regression Verification", conf, ipSections);

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
                if (resultCode == ECODE_SUCCESS) {
                    verificationJobReport(job, new ConfigReader(VERIFICATION_PROP_SECTION));
                }
            } else {
                job.submit();
                resultCode = ECODE_RUNNING;
            }
        }

        return resultCode;
    }

    private void verificationJobReport(Job job, ConfigReader cfgReader) throws IOException {

        Map<String, String> result = new HashMap<>();

        Configuration conf = job.getConfiguration();

        Path outDir = new Path(conf.get(cfgReader.getPropertyPath(OUT_PATH_PROP)));
        FileReader reader = new FileReader(outDir, conf);

        // may take a moment for filesystem to catch up, so if not 'wasSuccess' wait a tick
        if (!reader.wasSuccess()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (reader.wasSuccess()) {
            // read result
            Files.copy(Paths.get(conf.get(cfgReader.getPropertyPath(OUT_PATH_PROP)), "part-r-00000"),
                Paths.get(getProperty(job, cfgReader, VERIFY_OUTPUT_PATH_PROP, "verification.txt")));
        }

        reader.close();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
