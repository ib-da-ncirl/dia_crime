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
import ie.ibuttimer.dia_crime.misc.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Base class for hadoop job processing
 */
public abstract class AbstractDriver {

    private DiaCrimeMain app;

    public AbstractDriver(DiaCrimeMain app) {
        this.app = app;
    }

    public DiaCrimeMain getApp() {
        return app;
    }

    /**
     * Initialise a job
     * @param name      Job name
     * @param conf      Job configuration
     * @param inputs    Job inputs
     * @param outputs   Job outputs
     * @return
     * @throws Exception
     */
    public Job initJob(String name, Configuration conf,
                       Map<String, InputCfg> inputs, Map<String, OutputCfg> outputs) throws Exception {

        if (inputs.size() == 0) {
            throw new IllegalArgumentException("No sections specified");
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(AbstractDriver.class);
        job.setJobName(name);

        PropertyWrangler propertyWrangler = new PropertyWrangler();

        String outPath = null;
        if (inputs.size() > 1) {
            // multiple inputs
            Map<String, String> outputPaths = new HashMap<>();
            inputs.forEach((section, cfg) -> {

                if (isSubSectionKey(section)) {
                    propertyWrangler.setRoot(getSubSection(section).getLeft());
                } else {
                    propertyWrangler.setRoot(section);
                }

                String inPathProp = propertyWrangler.getPropertyPath(cfg.inPath);
                String inPath = conf.get(inPathProp);
                MultipleInputs.addInputPath(job,
                    new Path(inPath), TextInputFormat.class, cfg.mapper);

                if (!cfg.inPath.equals(InputCfg.DEFAULT_IN_PATH)) {
                    if (DebugLevel.getSetting(conf, section).showMe(DebugLevel.HIGH)) {
                        getLogger().info(String.format("!! Non-standard mapper input path: %s - [%s] %s",
                            inPathProp, inPath, cfg.mapper.getSimpleName()));
                    }
                }

                outputPaths.put(section, conf.get(propertyWrangler.getPropertyPath(OUT_PATH_PROP)));
            });

            // check all out paths are the same
            for (String key : outputPaths.keySet()) {
                String pathOut = outputPaths.get(key);
                if (outPath == null) {
                    outPath = pathOut;
                } else if (!outPath.equals(pathOut)) {
                    outPath = null;
                    break;
                }
            }
        } else {
            assert inputs.size() == 1;

            AtomicReference<String> singleOutPath = new AtomicReference<>();
            inputs.forEach((section, cfg) -> {
                propertyWrangler.setRoot(section);
                singleOutPath.set(conf.get(propertyWrangler.getPropertyPath(OUT_PATH_PROP)));

                job.setMapperClass(cfg.mapper);
            });

            outPath = singleOutPath.get();
            FileInputFormat.addInputPath(job, new Path(conf.get(propertyWrangler.getPropertyPath(IN_PATH_PROP))));
        }

        if (outPath != null) {
            FileOutputFormat.setOutputPath(job, new Path(outPath));
        } else {
            // TODO multiple output paths
            throw new UnsupportedOperationException("Multiple output path not currently supported");
        }

        outputs.forEach((section, cfg) -> {
            MultipleOutputs.addNamedOutput(job, cfg.namedOutput, TextOutputFormat.class, cfg.keyClass, cfg.valueClass);
        });

        return job;
    }

    /**
     * Initialise a single output job
     * @param name      Job name
     * @param conf      Job configuration
     * @param inputs    Job inputs
     * @return
     * @throws Exception
     */
    public Job initJob(String name, Configuration conf, Map<String, InputCfg> inputs) throws Exception {
        return initJob(name, conf, inputs, Collections.emptyMap());
    }

    /**
     * Load configuration for specified MapReduce.
     * @param conf          Configuration to populate
     * @param properties    Properties
     * @param sections      Main sections used as key for ICsvMapperCfg
     * @param commonSection Property sections to be added after each main
     * @return
     */
    protected int readConfigs(Configuration conf, Properties properties, List<String> sections, List<String> commonSection) {

        // There is duplication of common property reading for stocks, unnecessary but convenient

        Map<String, Integer> configResults = new HashMap<>();

        sections.forEach(section -> {
            try {
                List<String> supplementarySections = new ArrayList<>(commonSection);
                supplementarySections.add(GLOBAL_PROP_SECTION);

                configResults.put(section, app.setupJob(conf, properties, section, supplementarySections));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        int resultCode = ECODE_CONFIG_ERROR;
        for (String id : configResults.keySet()) {
            resultCode = configResults.get(id);
            if (resultCode != ECODE_SUCCESS) {
                break;
            }
        }

        return resultCode;
    }

    protected void setMultipleInputsInputPath(Job job, String section,
                                                Map<String, Pair<Integer, Configuration>> configs,
                                                Map<String, Class<? extends Mapper<?,?,?,?>>> sections) {

        if (configs.containsKey(section)) {
            Configuration conf = configs.get(section).getRight();

            MultipleInputs.addInputPath(job, new Path(conf.get(IN_PATH_PROP)), TextInputFormat.class,
                sections.get(section));
        }
    }

    protected void setAllMultipleInputsInputPath(Job job,
                                                Map<String, Pair<Integer, Configuration>> configs,
                                                 Map<String, Class<? extends Mapper<?,?,?,?>>> sections) {

        configs.forEach((section, value) -> {
            Configuration conf = configs.get(section).getRight();

            MultipleInputs.addInputPath(job, new Path(conf.get(IN_PATH_PROP)), TextInputFormat.class,
                sections.get(section));
        });
    }

    private void updateConfiguration(Configuration conf, String setting, String value, boolean log) {
        conf.set(setting, value);
        if (log) {
            getLogger().info(String.format("!! Configuration updated %s - [%s]", setting, value));
        }
    }

    private void updateProperties(Properties properties, String setting, String value, boolean log) {
        properties.setProperty(setting, value);
        if (log) {
            getLogger().info(String.format("!! Properties updated %s - [%s]", setting, value));
        }
    }

    protected void updateConfiguration(Configuration conf, String setting, String value, ICsvMapperCfg sCfgChk) {
        updateConfiguration(conf, setting, value, DebugLevel.getSetting(conf, sCfgChk).showMe(DebugLevel.HIGH));
    }

    protected void updateConfiguration(Configuration conf, String setting, String value, String section) {
        updateConfiguration(conf, setting, value, DebugLevel.getSetting(conf, section).showMe(DebugLevel.HIGH));
    }

    protected void updateConfiguration(Configuration conf, String setting, String value, IPropertyWrangler wrangler) {
        updateConfiguration(conf, setting, value, DebugLevel.getSetting(conf, wrangler).showMe(DebugLevel.HIGH));
    }

    protected void updateConfigPropertyWithTimestamp(Configuration conf, String property, IPropertyWrangler wrangler) {
        String setting = wrangler.getPropertyPath(property);
        String propValue = conf.get(setting, "");
        int hash = propValue.hashCode();
        if (propValue.contains("<datetime>")) {
            propValue = propValue.replace("<datetime>", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }
        if (propValue.contains("<date>")) {
            propValue = propValue.replace("<date>", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE));
        }
        if (propValue.contains("<time>")) {
            propValue = propValue.replace("<time>", LocalTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME));
        }
        if (propValue.hashCode() != hash) {
            updateConfiguration(conf, setting, propValue, DebugLevel.getSetting(conf, wrangler).showMe(DebugLevel.HIGH));
        }
    }

    protected String makeSubSectionKey(String section, String sub) {
        return MapStringifier.elementStringifier().stringifyElement(section, sub);
    }

    protected boolean isSubSectionKey(String section) {
        return MapStringifier.elementStringifier().isElementified(section);
    }

    protected Pair<String, String> getSubSection(String section) {
        return MapStringifier.elementStringifier().destringifyElement(section);
    }

    protected abstract Logger getLogger();

    protected void updatePropertyWithTimestamp(Properties properties, String property, IPropertyWrangler wrangler) {
        String setting = wrangler.getPropertyPath(property);
        String propValue = properties.getProperty(setting, "");
        int hash = propValue.hashCode();
        if (propValue.contains("<datetime>")) {
            propValue = propValue.replace("<datetime>", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }
        if (propValue.contains("<date>")) {
            propValue = propValue.replace("<date>", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE));
        }
        if (propValue.contains("<time>")) {
            propValue = propValue.replace("<time>", LocalTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME));
        }
        if (propValue.hashCode() != hash) {
            updateProperties(properties, setting, propValue, true);
        }
    }


    /**
     * Job configuration object
     */
    public static class JobConfig {

        Properties properties;
        boolean wait;
        boolean verbose;

        private JobConfig(Properties properties) {
            this(properties , true);
        }

        private JobConfig(Properties properties, boolean wait) {
            this.properties = properties;
            this.wait = wait;
            this.verbose = true;
        }

        public static JobConfig of(Properties properties, boolean wait) {
            return new JobConfig(properties, wait);
        }

        public static JobConfig of(Properties properties) {
            return new JobConfig(properties);
        }
    }

    /**
     * Input configuration object
     */
    public static class InputCfg {

        static final String DEFAULT_IN_PATH = IN_PATH_PROP;

        Class<? extends Mapper<?, ?, ?, ?>> mapper;
        String inPath;  // in path property name

        public InputCfg(Class<? extends Mapper<?, ?, ?, ?>> mapper, String inPath) {
            this.mapper = mapper;
            this.inPath = inPath;
        }

        static InputCfg of(Class<? extends Mapper<?, ?, ?, ?>> mapper, String inPath) {
            return new InputCfg(mapper, inPath);
        }

        static InputCfg of(Class<? extends Mapper<?, ?, ?, ?>> mapper) {
            return new InputCfg(mapper, DEFAULT_IN_PATH);
        }
    }

    /**
     * Output configuration object
     */
    public static class OutputCfg {

        String namedOutput;
        Class<?> keyClass;
        Class<?> valueClass;

        public OutputCfg(String namedOutput, Class<?> keyClass, Class<?> valueClass) {
            this.namedOutput = namedOutput;
            this.keyClass = keyClass;
            this.valueClass = valueClass;
        }
        public static OutputCfg of (String namedOutput, Class<?> keyClass, Class<?> valueClass) {
            return new OutputCfg(namedOutput, keyClass, valueClass);
        }
    }
}
