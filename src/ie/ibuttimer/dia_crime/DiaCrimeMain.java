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
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeEntryMapper;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeEntryReducer;
import ie.ibuttimer.dia_crime.hadoop.crime.CrimeEntryWritable;
import ie.ibuttimer.dia_crime.hadoop.stock.DowJonesStockEntryMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.NasdaqStockEntryMapper;
import ie.ibuttimer.dia_crime.hadoop.stock.SP500StockEntryMapper;
import ie.ibuttimer.dia_crime.hadoop.weather.WeatherMapper;
import ie.ibuttimer.dia_crime.misc.Constants;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;


public class DiaCrimeMain {

    private static final Logger logger = Logger.getLogger(DiaCrimeMain.class);

    private static Map<String, ICsvEntryMapperCfg> propDefaults;
    static {
        propDefaults = new HashMap<>();

        propDefaults.put(CRIME_PROP_SECTION, CrimeEntryMapper.getCsvEntryMapperCfg());
        propDefaults.put(NASDAQ_PROP_SECTION, NasdaqStockEntryMapper.getCsvEntryMapperCfg());
        propDefaults.put(DOWJONES_PROP_SECTION, DowJonesStockEntryMapper.getCsvEntryMapperCfg());
        propDefaults.put(SP500_PROP_SECTION, SP500StockEntryMapper.getCsvEntryMapperCfg());
        propDefaults.put(WEATHER_PROP_SECTION, WeatherMapper.getCsvEntryMapperCfg());
    }

    private static final String OPT_HELP = "h";
    private static final String OPT_CFG = "c";
    private static final String OPT_JOB = "j";
    private static final String OPT_LIST_JOBS = "l";

    private static final String JOB_WEATHER = "weather";
    private static final String JOB_STOCKS = "stocks";
    private static final String JOB_STOCK_STATS = "stockstats";
    private static final List<Pair<String, String>> jobList;
    private static final String jobListFmt;
    static {
        jobList = new ArrayList<>();
        jobList.add(Pair.of(JOB_WEATHER, "process the weather file"));
        jobList.add(Pair.of(JOB_STOCKS, "process the stock files"));
        jobList.add(Pair.of(JOB_STOCK_STATS, "calculate stock statistics"));

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
        options.addOption(OPT_CFG, true, "configuration file");
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
                    resourceFile = "config.properties";
                }

                if (cmd.hasOption(OPT_JOB)) {
                    Properties properties = app.getResources(resourceFile);

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
                        default:
                            System.out.format("Unknown job: %s%n%n", cmd.getOptionValue(OPT_JOB));
                            app.jobList();
                            resultCode = ECODE_CONFIG_ERROR;
                    }
                    // TODO will probably remove stockDriver.runStockJob
                    //StockDriver stockDriver = new StockDriver(app);
                    //resultCode = stockDriver.runStockJob(properties);

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
     * @param properties    Properties
     * @param sections      Pair of main section, plus additional sections
     * @return
     * @throws Exception
     */
    public Pair<Integer, Configuration> setupJob(Properties properties, Pair<String, List<String>> sections) throws Exception {

        String main = sections.getLeft();
        List<String> supplementary = sections.getRight();

        List<String> allSections = new ArrayList<>();
        allSections.add(main);
        if (supplementary != null) {
            allSections.addAll(supplementary);
        }

        Configuration conf = getConfiguration(properties, main, allSections);

        int resultCode = checkConfiguration(conf, sections.getLeft());

        if (resultCode == Constants.ECODE_SUCCESS) {

            // HACK dev hack to delete output folder
            resultCode = devPrep(conf);
            if (resultCode != Constants.ECODE_SUCCESS) {
                System.exit(resultCode);
            }
        }

        return Pair.of(resultCode, conf);
    }

    public int runCrimeJob(DiaCrimeMain app, Properties properties) throws Exception {

        Pair<Integer, Configuration> setup = setupJob(properties, Pair.of(CRIME_PROP_SECTION, null));
        int resultCode = setup.getLeft();

        if (resultCode == Constants.ECODE_SUCCESS) {
            Configuration conf = setup.getRight();

            Job job = Job.getInstance(conf);
            job.setJarByClass(DiaCrimeMain.class);
            job.setJobName("DIA Crime");

            FileInputFormat.addInputPath(job, new Path(conf.get(IN_PATH_PROP)));
            FileOutputFormat.setOutputPath(job, new Path(conf.get(OUT_PATH_PROP)));

            job.setMapperClass(CrimeEntryMapper.class);
            job.setReducerClass(CrimeEntryReducer.class);

            /*
             * Input and Output types of a MapReduce job:
             * (input) <k1, v1> -> map -> <k2, v2> -> combine -> <k2, v2> -> reduce -> <k3, v3> (output)
             * (input) <LongWritable, Text> -> map -> <Text, CrimeEntryWritable> -> reduce -> <Text, Text> (output)
             */
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(CrimeEntryWritable.class);

            resultCode = job.waitForCompletion(true) ? Constants.ECODE_SUCCESS : Constants.ECODE_FAIL;
        }

        return resultCode;
    }

    /**
     * Load resources from the specified file
     * @param filename  Resource filename
     * @return Properties
     */
    private Properties getResources(String filename) {
        Properties properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(filename)) {
            if (input != null) {
                properties.load(input);
            } else {
                System.out.println("Unable to load " + filename);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return properties;
    }

    /**
     * Load configuration for specified MapReduce
     * @param properties    Property object to read
     * @param key           Key for ICsvEntryMapperCfg
     * @param sections      Property sections for MapReduce
     * @return Configuration
     */
    private Configuration getConfiguration(Properties properties, String key, List<String> sections) {
        Configuration conf = new Configuration();

        ICsvEntryMapperCfg entryMapperCfg = propDefaults.get(key);
        if (entryMapperCfg != null) {

            // get map of possible keys and default values
            HashMap<String, String> propDefault = entryMapperCfg.getPropertyDefaults();

            for (String section : sections) {
                // read the config
                String sectionMark = section + PROPERTY_SEPARATOR;
                properties.stringPropertyNames().stream()
                        .filter(k -> k.startsWith(sectionMark))
                        .forEach(k -> {
                            String prop = k.substring(sectionMark.length());
                            if (propDefault.containsKey(prop)) {
                                conf.set(prop, properties.getProperty(k, propDefault.get(prop)));
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
                chkRes.getRight()
                        .forEach(this::logConfigurationError);
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
        int resultCode = Constants.ECODE_SUCCESS;
        String outPath = conf.get(OUT_PATH_PROP);
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
                    resultCode = Constants.ECODE_FAIL;
                }
            }
        }
        return resultCode;
    }

}
