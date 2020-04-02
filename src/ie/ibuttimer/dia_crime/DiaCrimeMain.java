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

import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeMapper;
import ie.ibuttimer.dia_crime.hadoop.regression.RegressionMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.DowJonesStockMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.NasdaqStockMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.SP500StockMapper;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherMapper;
import ie.ibuttimer.dia_crime.misc.Constants;
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ie.ibuttimer.dia_crime.misc.Constants.*;


public class DiaCrimeMain {

    private static final Logger logger = Logger.getLogger(DiaCrimeMain.class);

    private static Map<String, ICsvEntryMapperCfg> propDefaults;
    static {
        propDefaults = new HashMap<>();

        propDefaults.put(CRIME_PROP_SECTION, CrimeMapper.getCsvEntryMapperCfg());
        propDefaults.put(NASDAQ_PROP_SECTION, NasdaqStockMapper.getCsvEntryMapperCfg());
        propDefaults.put(DOWJONES_PROP_SECTION, DowJonesStockMapper.getCsvEntryMapperCfg());
        propDefaults.put(SP500_PROP_SECTION, SP500StockMapper.getCsvEntryMapperCfg());
        propDefaults.put(WEATHER_PROP_SECTION, WeatherMapper.getCsvEntryMapperCfg());
        propDefaults.put(REGRESSION_PROP_SECTION, RegressionMapper.getCsvEntryMapperCfg());
    }

    private static final String DEFLT_CFG_FILE = "config.properties";
    private static final String MULTIPLE_CFG_FILE_SEP = ";";

    private static final String OPT_HELP = "h";
    private static final String OPT_CFG = "c";
    private static final String OPT_JOB = "j";
    private static final String OPT_LIST_JOBS = "l";

    /* sample argument lists
        -j weather
        -j stocks
        -j crime
        -j stock_stats -c config.properties;stock_stats.properties
        -j merge
        -j linear_regression -c config.properties;regression.properties
     */

    private static final String JOB_WEATHER = "weather";
    private static final String JOB_STOCKS = "stocks";
    private static final String JOB_STOCK_STATS = "stock_stats";
    private static final String JOB_CRIME = "crime";
    private static final String JOB_MERGE = "merge";
    private static final String JOB_LINEAR_REGRESSION = "linear_regression";
    private static final List<Pair<String, String>> jobList;
    private static final String jobListFmt;
    static {
        jobList = new ArrayList<>();
        jobList.add(Pair.of(JOB_WEATHER, "process the weather file"));
        jobList.add(Pair.of(JOB_STOCKS, "process the stock files"));
        jobList.add(Pair.of(JOB_STOCK_STATS, "calculate stock statistics"));
        jobList.add(Pair.of(JOB_CRIME, "process the crime file"));
        jobList.add(Pair.of(JOB_MERGE, "merge crime, stocks & weather to a single file"));
        jobList.add(Pair.of(JOB_LINEAR_REGRESSION, "perform a linear regression on merged crime, stocks & weather data"));

        OptionalInt width = jobList.stream().map(Pair::getLeft).mapToInt(String::length).max();
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
        options.addOption(OPT_LIST_JOBS, false, "list available jobs");
    }

    public static void main(String[] args) throws Exception {

        DiaCrimeMain app = new DiaCrimeMain();

        CommandLineParser parser = new BasicParser();
        int resultCode = ECODE_SUCCESS;
        try {
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption(OPT_HELP)) {
                // print help
                app.help();
            } else if (cmd.hasOption(OPT_LIST_JOBS)) {
                // print job list
                app.jobList();
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
                    Properties properties = app.getResources(resourceFile);
                    if (properties.isEmpty()) {
                        resultCode = ECODE_CONFIG_ERROR;
                        System.out.format("No configuration specified, properties empty%n%n");
                        app.help();
                    } else {
                        // run the job
                        switch (cmd.getOptionValue(OPT_JOB)) {
                            case JOB_WEATHER:
                                resultCode = WeatherDriver.of(app).runWeatherJob(properties);
                                break;
                            case JOB_STOCKS:
                                resultCode = StockDriver.of(app).runStockJob(properties);
                                break;
                            case JOB_STOCK_STATS:
                                resultCode = StockDriver.of(app).runStockStatsJob(properties);
                                break;
                            case JOB_CRIME:
                                resultCode = CrimeDriver.of(app).runCrimeJob(properties);
                                break;
                            case JOB_MERGE:
                                resultCode = MergeDriver.of(app).runMergeJob(properties);
                                break;
                            case JOB_LINEAR_REGRESSION:
                                resultCode = LinearRegressionDriver.of(app).runLinearRegressionJob(properties);
                                break;
                            default:
                                System.out.format("Unknown job: %s%n%n", cmd.getOptionValue(OPT_JOB));
                                app.jobList();
                                resultCode = ECODE_CONFIG_ERROR;
                        }
                    }
                } else {
                    System.out.format("No arguments specified%n%n");
                    app.help();
                }
            }
        } catch (ParseException pe) {
            System.out.format("%s%n%n", pe.getMessage());
            app.help();
            resultCode = ECODE_FAIL;
        }

        System.exit(resultCode);
    }

    private void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("dia_crime", options);
    }

    private void jobList() {
        System.out.println("Job List");
        jobList.forEach(pair -> System.out.format(jobListFmt, pair.getLeft(), pair.getRight()));
    }


    /**
     * @param conf
     * @param properties
     * @param main
     * @param supplementary
     * @return
     */
    public int setupJob(Configuration conf, Properties properties, String main, List<String> supplementary) {

        List<String> allSections = new ArrayList<>();
        allSections.add(main);
        if (supplementary != null) {
            allSections.addAll(supplementary);
        }

        getConfiguration(conf, properties, main, allSections);

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
     * Load configuration for specified MapReduce
     * @param properties    Property object to read
     * @param main          Main section used as key for ICsvEntryMapperCfg
     * @param sections      Property sections for MapReduce
     * @return Configuration
     */
    private Configuration getConfiguration(Configuration conf, Properties properties, String main, List<String> sections) {

        ICsvEntryMapperCfg entryMapperCfg = propDefaults.get(main);
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
                            conf.set(confPropWrangler.getPropertyPath(prop),
                                properties.getProperty(k, propDefault.get(prop)));
                        }
                    });
            }
        }
        return conf;
    }

    /**
     * Check configuration for specified MapReduce
     * @param conf      Configuration
     * @param section   Property section for MapReduce
     * @return  ECODE_SUCCESS if configuration valid, ECODE_CONFIG_ERROR otherwise
     */
    private int checkConfiguration(Configuration conf, String section) {
        int resultCode;

        ICsvEntryMapperCfg entryMapperCfg = propDefaults.get(section);
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
