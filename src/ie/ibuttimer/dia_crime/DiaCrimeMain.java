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

import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeMapper;
import ie.ibuttimer.dia_crime.hadoop.matrix.MatrixMapper;
import ie.ibuttimer.dia_crime.hadoop.normalise.NormaliseMapper;
import ie.ibuttimer.dia_crime.hadoop.regression.RegressionTrainMapper;
import ie.ibuttimer.dia_crime.hadoop.regression.RegressionValidateMapper;
import ie.ibuttimer.dia_crime.hadoop.stats.StatsMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.DowJonesStockMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.NasdaqStockMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.SP500StockMapper;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherMapper;
import ie.ibuttimer.dia_crime.misc.*;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Main class providing argument processing
 */
public class DiaCrimeMain {

    private static final Logger logger = Logger.getLogger(DiaCrimeMain.class);

    private static Map<String, ICsvMapperCfg> propDefaults;
    static {
        propDefaults = new HashMap<>();

        propDefaults.put(CRIME_PROP_SECTION, CrimeMapper.getClsCsvMapperCfg());
        propDefaults.put(NASDAQ_PROP_SECTION, NasdaqStockMapper.getClsCsvMapperCfg());
        propDefaults.put(DOWJONES_PROP_SECTION, DowJonesStockMapper.getClsCsvMapperCfg());
        propDefaults.put(SP500_PROP_SECTION, SP500StockMapper.getClsCsvMapperCfg());
        propDefaults.put(WEATHER_PROP_SECTION, WeatherMapper.getClsCsvMapperCfg());
        propDefaults.put(STATS_PROP_SECTION, StatsMapper.getClsCsvMapperCfg());
        propDefaults.put(NORMALISE_PROP_SECTION, NormaliseMapper.getClsCsvMapperCfg());
        propDefaults.put(REGRESSION_PROP_SECTION, RegressionTrainMapper.getClsCsvMapperCfg());
        propDefaults.put(VERIFICATION_PROP_SECTION, RegressionValidateMapper.getClsCsvMapperCfg());
        propDefaults.put(MATRIX_PROP_1_SECTION, MatrixMapper.MatrixMapper1.getClsCsvMapperCfg());
        propDefaults.put(MATRIX_PROP_2_SECTION, MatrixMapper.MatrixMapper2.getClsCsvMapperCfg());
    }

    private static final String DEFLT_CFG_FILE = "config.properties";
    private static final String MULTIPLE_CFG_FILE_SEP = ";";

    private static final String OPT_HELP = "h";
    private static final String OPT_CFG = "c";
    private static final String OPT_JOB = "j";
    private static final String OPT_WAIT = "w";
    private static final String OPT_NO_WAIT = "nw";
    private static final String OPT_LIST_JOBS = "l";
    private static final String OPT_MULTI_JOB = "m";
    private static final String OPT_IN_ROOT = "i";
    private static final String OPT_OUT_ROOT = "o";

    /* sample argument lists
        -j weather -c prod.properties;config.properties
        -j stocks -c prod.properties;config.properties
        -j crime -c prod.properties;config.properties
        -j merge -c prod.properties;config.properties;merge.properties
        -j stats -c prod.properties;config.properties;stats.properties
        -j normalise -c prod.properties;config.properties;normalise.properties
        -j linear_regression -c prod.properties;config.properties;regression.properties
        -j regression_verify -c prod.properties;config.properties;regression.properties;verification.properties
        -j matrix_multiply -c prod.properties;config.properties;matrix.properties
        -m <path to file>
     */

    private static final String JOB_WEATHER = "weather";
    private static final String JOB_STOCKS = "stocks";
    private static final String JOB_CRIME = "crime";
    private static final String JOB_MERGE = "merge";
    private static final String JOB_STATS = "stats";
    private static final String JOB_NORMALISE = "normalise";
    private static final String JOB_LINEAR_REGRESSION = "linear_regression";
    private static final String JOB_VERIFY_REGRESSION = "verify_regression";
    private static final String JOB_MATRIX_MULTIPLY = "matrix_multiply";
    private static final List<Triple<String, String, String>> jobList;
    private static final String jobListFmt;
    static {
        jobList = new ArrayList<>();
        jobList.add(Triple.of(JOB_WEATHER, "process the weather file", "Weather Job"));
        jobList.add(Triple.of(JOB_STOCKS, "process the stock files", "Stocks Job"));
        jobList.add(Triple.of(JOB_CRIME, "process the crime file", "Crime Job"));
        jobList.add(Triple.of(JOB_MERGE, "merge crime, stocks & weather to a single file", "Merge Job"));
        jobList.add(Triple.of(JOB_STATS, "perform basic statistics analysis", "Statistics Job"));
        jobList.add(Triple.of(JOB_NORMALISE, "normalise numeric values", "Normalise Job"));
        jobList.add(Triple.of(JOB_LINEAR_REGRESSION, "perform a linear regression on merged crime, stocks & weather data", "Linear Regression Job"));
        jobList.add(Triple.of(JOB_VERIFY_REGRESSION, "verify a linear regression on merged crime, stocks & weather data", "Regression Verification Job"));
        jobList.add(Triple.of(JOB_MATRIX_MULTIPLY, "perform a matrix multiplication", "Matrix Multiplication Job"));

        OptionalInt width = jobList.stream().map(Triple::getLeft).mapToInt(String::length).max();
        StringBuffer sb = new StringBuffer("  %");
        width.ifPresent(w -> sb.append("-").append(w));
        sb.append("s : %s%n");
        jobListFmt = sb.toString();
    }

    private static Options options;
    static {
        options = new Options();
        options.addOption(OPT_HELP, false, "print this message");
        options.addOption(OPT_CFG, true, "configuration file(s), multiple files separated by '" +
            MULTIPLE_CFG_FILE_SEP + "'");
        options.addOption(OPT_JOB, true, "name of job to run");
        options.addOption(OPT_WAIT, false, "wait for job completion, [default]");
        options.addOption(OPT_NO_WAIT, false, "do not wait for job completion");
        options.addOption(OPT_LIST_JOBS, false, "list available jobs");
        options.addOption(OPT_MULTI_JOB, true, "process multiple jobs as per specified file");
        options.addOption(OPT_IN_ROOT, true, "input root folder");
        options.addOption(OPT_OUT_ROOT, true, "output root folder");
    }

    private String inPathRoot = "";
    private String outPathRoot = "";


    public static void main(String[] args) throws Exception {

        DiaCrimeMain app = new DiaCrimeMain();

        CommandLineParser parser = new BasicParser();
        int resultCode = ECODE_SUCCESS;
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption(OPT_IN_ROOT)) {
                app.inPathRoot = cmd.getOptionValue(OPT_IN_ROOT);
            }
            if (cmd.hasOption(OPT_OUT_ROOT)) {
                app.outPathRoot = cmd.getOptionValue(OPT_OUT_ROOT);
            }

            if (cmd.hasOption(OPT_MULTI_JOB)) {
                String jobFile = cmd.getOptionValue(OPT_MULTI_JOB);
                if (org.apache.http.util.TextUtils.isEmpty(jobFile)) {
                    resultCode = ECODE_CONFIG_ERROR;
                    System.out.format("No job file specified");
                    app.help();
                }

                File file = FileUtils.getFile(jobFile);
                List<String> contents = FileUtils.readLines(file, StandardCharsets.UTF_8);

                for (String jobSpec : contents) {
                    if (!jobSpec.trim().startsWith(COMMENT_PREFIX)) {
                        resultCode = app.processJob(jobSpec.split(" "));
                        if (resultCode != ECODE_SUCCESS) {
                            break;
                        }
                    }
                }
            } else {
                resultCode = app.processJob(args);
            }
        } catch (ParseException pe) {
            System.out.format("%s%n%n", pe.getMessage());
            app.help();
            resultCode = ECODE_FAIL;
        }

        System.exit(resultCode);
    }

    private int processJob(String[] args) throws Exception {

        CommandLineParser parser = new BasicParser();
        int resultCode = ECODE_SUCCESS;
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption(OPT_HELP)) {
                // print help
                help();
            } else if (cmd.hasOption(OPT_LIST_JOBS)) {
                // print job list
                jobList();
            } else {
                String resourceFile;
                if (cmd.hasOption(OPT_CFG)) {
                    // print job list
                    resourceFile = cmd.getOptionValue(OPT_CFG);
                } else {
                    resourceFile = DEFLT_CFG_FILE;
                }

                if (cmd.hasOption(OPT_JOB)) {
                    // read the config
                    Properties properties = getResources(resourceFile);

                    if (!TextUtils.isEmpty(inPathRoot)) {
                        properties.setProperty("global.in_path_root", inPathRoot);
                    }
                    if (!TextUtils.isEmpty(outPathRoot)) {
                        properties.setProperty("global.out_path_root", outPathRoot);
                    }

                    if (properties.isEmpty()) {
                        resultCode = ECODE_CONFIG_ERROR;
                        System.out.format("No configuration specified, properties empty%n%n");
                        help();
                    } else {
                        // run the job
                        AbstractDriver.JobConfig jobCfg = AbstractDriver.JobConfig.of(properties,
                            (!cmd.hasOption(OPT_NO_WAIT)), inPathRoot, outPathRoot);

                        String name = cmd.getOptionValue(OPT_JOB);
                        jobList.stream()
                            .filter(e -> e.getLeft().equals(name))
                            .findFirst()
                            .ifPresent(t -> logger.info(Utils.getDialog("Running " + t.getRight())));

                        switch (name) {
                            case JOB_WEATHER:
                                resultCode = WeatherDriver.of(this).runWeatherJob(jobCfg);
                                break;
                            case JOB_STOCKS:
                                resultCode = StockDriver.of(this).runStockJob(jobCfg);
                                break;
                            case JOB_CRIME:
                                resultCode = CrimeDriver.of(this).runCrimeJob(jobCfg);
                                break;
                            case JOB_MERGE:
                                resultCode = MergeDriver.of(this).runMergeJob(jobCfg);
                                break;
                            case JOB_STATS:
                                resultCode = StatsDriver.of(this).runStatsJob(jobCfg);
                                break;
                            case JOB_NORMALISE:
                                resultCode = NormaliseDriver.of(this).runNormaliseJob(jobCfg);
                                break;
                            case JOB_LINEAR_REGRESSION:
                                resultCode = LinearRegressionDriver.of(this).runLinearRegressionJob(jobCfg);
                                break;
                            case JOB_VERIFY_REGRESSION:
                                resultCode = LinearRegressionDriver.of(this).runRegressionValidationJob(jobCfg);
                                break;
                            case JOB_MATRIX_MULTIPLY:
                                resultCode = MatrixDriver.of(this).runMatrixJob(jobCfg);
                                break;
                            default:
                                System.out.format("Unknown job: %s%n%n", cmd.getOptionValue(OPT_JOB));
                                jobList();
                                resultCode = ECODE_CONFIG_ERROR;
                        }
                    }
                } else {
                    System.out.format("No arguments specified%n%n");
                    help();
                }
            }
        } catch (ParseException pe) {
            System.out.format("%s%n%n", pe.getMessage());
            help();
            resultCode = ECODE_FAIL;
        }

        return resultCode;
    }

    private void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("dia_crime", options);
    }

    private void jobList() {
        System.out.println("Job List");
        jobList.forEach(job -> System.out.format(jobListFmt, job.getLeft(), job.getRight()));
    }


    /**
     * Load configuration for specified MapReduce.
     * @param conf          Configuration to populate
     * @param properties    Properties
     * @param main          Main section used as key for ICsvMapperCfg
     * @param supplementary Property sections to be added after main
     * @return Overall result code
     */
    public int setupJob(Configuration conf, Properties properties, String main, List<String> supplementary) {

        List<String> allSections = new ArrayList<>();
        allSections.add(main);
        if (supplementary != null) {
            allSections.addAll(supplementary);
        }

        getConfiguration(conf, properties, main, allSections);

        if (DebugLevel.getSetting(conf, main).showMe(DebugLevel.HIGH)) {
            Map<String, String> map = conf.getPropsWithPrefix(main);
            map.forEach((key, val) -> logger.info(String.format("%s%s - [%s]", main, key, val)));
        }

        int resultCode = checkConfiguration(conf, main);

        if (resultCode == Constants.ECODE_SUCCESS) {

            // HACK dev hack to delete output folder
            resultCode = devPrep(conf);
            if (resultCode != Constants.ECODE_SUCCESS) {
                System.exit(resultCode);
            }
        }

        return resultCode;
    }

    /**
     * Load resources from the specified file(s). Multiple files are separated by ':'.
     * @param filename  Resource filename(s)
     * @return Properties
     */
    private Properties getResources(String filename) {
        Properties properties = new Properties();

        Arrays.asList(filename.split(MULTIPLE_CFG_FILE_SEP)).forEach(name -> {
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(name)) {
                if (input != null) {
                    properties.load(input);
                } else {
                    System.out.println("Unable to load " + name);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
        return properties;
    }

    /**
     * Load configuration for specified MapReduce.
     * Config sections are loaded in the following order they appear in <code>sections</code>, and are added under
     * <code>main</code>.
     * @param properties    Property object to read
     * @param main          Main section used as key for ICsvMapperCfg
     * @param sections      Property sections for MapReduce
     * @return Configuration
     */
    private Configuration getConfiguration(Configuration conf, Properties properties, String main, List<String> sections) {

        ICsvMapperCfg entryMapperCfg = propDefaults.get(main);
        if (entryMapperCfg != null) {
            PropertyWrangler confPropWrangler = new PropertyWrangler(main);

            // get map of possible keys and default values
            HashMap<String, String> propDefault = entryMapperCfg.getPropertyDefaults();

            // read all the sections and save to conf under main section
            for (String section : sections) {
                // read the config
                PropertyWrangler sectionWrangler = new PropertyWrangler(section);

                properties.stringPropertyNames().stream()
                    .filter(sectionWrangler::hasRoot)
                    .forEach(k -> {
                        String prop = sectionWrangler.getPropertyName(k);
                        if (propDefault.containsKey(prop)) {
                            String propertyValue = getProperty(properties, k, propDefault.get(prop));
                            conf.set(confPropWrangler.getPropertyPath(prop), propertyValue);
                        }
                    });
            }
        }
        return conf;
    }

    public String getProperty(Properties properties, String name, String dfltValue) {
        String propertyValue = properties.getProperty(name, dfltValue).trim();

        if (propertyValue.startsWith(PROPERTY_ALIAS)) {
            // read other property for value
            Pair<String, String> otherProperty = MapStringifier.ElementStringify.of(PROPERTY_ALIAS_SEPARATOR)
                .destringifyElement(propertyValue);
            if (otherProperty.getRight() != null) {
                propertyValue = properties.getProperty(otherProperty.getRight().trim(), "");
            }
        } else if (propertyValue.startsWith(PROPERTY_SUM)) {
            // read other properties for value
            Pair<String, String> propertyPair = MapStringifier.ElementStringify.of(PROPERTY_ALIAS_SEPARATOR)
                .destringifyElement(propertyValue);
            if (propertyPair.getRight() != null) {
                Pair<String, String> otherProperties =
                    MapStringifier.ElementStringify.COMMA.destringifyElement(propertyPair.getRight());
                StringBuilder sb = new StringBuilder();
                if (otherProperties.getLeft() != null) {
                    sb.append(properties.getProperty(otherProperties.getLeft().trim(), ""));
                }
                if (otherProperties.getRight() != null) {
                    sb.append(otherProperties.getRight().trim());
                }
                propertyValue = sb.toString();
            }
        }
        return propertyValue;
    }




    /**
     * Check configuration for specified MapReduce
     * @param conf      Configuration
     * @param section   Property section for MapReduce
     * @return  ECODE_SUCCESS if configuration valid, ECODE_CONFIG_ERROR otherwise
     */
    private int checkConfiguration(Configuration conf, String section) {
        int resultCode;

        ICsvMapperCfg entryMapperCfg = propDefaults.get(section);
        if (entryMapperCfg != null) {
            Pair<Integer, List<String>> chkRes = entryMapperCfg.checkConfiguration(conf);
            resultCode = chkRes.getLeft();
            if (resultCode != ECODE_SUCCESS) {
                chkRes.getRight().forEach(this::logConfigurationError);
            }
        } else {
            resultCode = ECODE_CONFIG_ERROR;
        }

        return resultCode;
    }

    private int logConfigurationError(String error) {
        System.err.println(error);
        logger.error(error);
        return Constants.ECODE_CONFIG_ERROR;
    }


    /**
     * Delete the output directory and all its contents
     * @param conf  Configuration
     * @return  ECODE_SUCCESS or ECODE_FAIL
     */
    private int devPrep(Configuration conf) {
        AtomicInteger resultCode = new AtomicInteger(ECODE_SUCCESS);

        conf.iterator().forEachRemaining(entry -> {
            String key = entry.getKey();
            if (key.endsWith(OUT_PATH_PROP)) {
                String outPath = conf.get(key);
                if (!TextUtils.isEmpty(outPath)) {
                    java.nio.file.Path path = java.nio.file.Path.of(outPath);
                    if (Files.isDirectory(path)) {
                        try {
                            Files.walk(java.nio.file.Path.of(outPath))
                                .sorted(Comparator.reverseOrder())
                                .map(java.nio.file.Path::toFile)
                                .forEach(File::delete);
                        } catch (IOException e) {
                            String error = "Error: devPrep IOException";
                            System.err.println(error);
                            logger.error(error, e);
                            resultCode.set(ECODE_FAIL);
                        }
                    }
                }
            }
        });

        return resultCode.get();
    }

}
