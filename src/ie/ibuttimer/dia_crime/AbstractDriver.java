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
import ie.ibuttimer.dia_crime.misc.DebugLevel;
import ie.ibuttimer.dia_crime.misc.IPropertyWrangler;
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

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


    public Job initJob(String name, Configuration conf,
                       Map<String, SectionCfg> sections) throws Exception {

        if (sections.size() == 0) {
            throw new IllegalArgumentException("No sections specified");
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(AbstractDriver.class);
        job.setJobName(name);

        PropertyWrangler propertyWrangler = new PropertyWrangler();

        String outPath = null;
        if (sections.size() > 1) {
            // multiple inputs
            Map<String, String> outputPaths = new HashMap<>();
            sections.forEach((section, cfg) -> {
                propertyWrangler.setRoot(section);

                String inPathProp = propertyWrangler.getPropertyPath(cfg.inPath);
                String inPath = conf.get(inPathProp);
                MultipleInputs.addInputPath(job,
                    new Path(inPath), TextInputFormat.class, cfg.mapper);

                if (!cfg.inPath.equals(SectionCfg.DEFAULT_IN_PATH)) {
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
            assert sections.size() == 1;

            AtomicReference<String> singleOutPath = new AtomicReference<>();
            sections.forEach((section, cfg) -> {
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
            // TODO MultipleOutputs
            throw new UnsupportedOperationException("Multiple output path not currently supported");
        }

        return job;
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
                  Map<String, Pair<Integer, Configuration>> configs, Map<String, Class<? extends Mapper>> sections) {

        if (configs.containsKey(section)) {
            Configuration conf = configs.get(section).getRight();

            MultipleInputs.addInputPath(job, new Path(conf.get(IN_PATH_PROP)), TextInputFormat.class,
                sections.get(section));
        }
    }

    protected void setAllMultipleInputsInputPath(Job job,
                  Map<String, Pair<Integer, Configuration>> configs, Map<String, Class<? extends Mapper>> sections) {

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

    protected void updateConfiguration(Configuration conf, String setting, String value, ICsvMapperCfg sCfgChk) {
        updateConfiguration(conf, setting, value, DebugLevel.getSetting(conf, sCfgChk).showMe(DebugLevel.HIGH));
    }

    protected void updateConfiguration(Configuration conf, String setting, String value, String section) {
        updateConfiguration(conf, setting, value, DebugLevel.getSetting(conf, section).showMe(DebugLevel.HIGH));
    }

    protected void updateConfiguration(Configuration conf, String setting, String value, IPropertyWrangler wrangler) {
        updateConfiguration(conf, setting, value, DebugLevel.getSetting(conf, wrangler).showMe(DebugLevel.HIGH));
    }


    protected abstract Logger getLogger();

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

    public static class SectionCfg {

        static final String DEFAULT_IN_PATH = IN_PATH_PROP;

        Class<? extends Mapper<?, ?, ?, ?>> mapper;
        String inPath;  // in path property name

        public SectionCfg(Class<? extends Mapper<?, ?, ?, ?>> mapper, String inPath) {
            this.mapper = mapper;
            this.inPath = inPath;
        }

        static SectionCfg of(Class<? extends Mapper<?, ?, ?, ?>> mapper, String inPath) {
            return new SectionCfg(mapper, inPath);
        }

        static SectionCfg of(Class<? extends Mapper<?, ?, ?, ?>> mapper) {
            return new SectionCfg(mapper, DEFAULT_IN_PATH);
        }
    }

}
