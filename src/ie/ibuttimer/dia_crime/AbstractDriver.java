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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;


public abstract class AbstractDriver {

    private DiaCrimeMain app;

    public AbstractDriver(DiaCrimeMain app) {
        this.app = app;
    }

    public DiaCrimeMain getApp() {
        return app;
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
        job.setJarByClass(AbstractDriver.class);
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

            sections.forEach((s, cls) -> {
                job.setMapperClass(cls);
            });
        }

        if (outPath != null) {
            FileOutputFormat.setOutputPath(job, new Path(outPath));
        } else {
            // TODO MultipleOutputs
            throw new UnsupportedOperationException("Multiple output path not currently supported");
        }

        return job;
    }

    protected Pair<Integer, Map<String, Pair<Integer, Configuration>>>
                    readConfigs(Properties properties, List<String> sections, List<String> commonSection) {

        // There is duplication of common property reading for stocks, unnecessary but convenient

        Map<String, Pair<Integer, Configuration>> configs = new HashMap<>();

        sections.forEach(s -> {
            try {
                List<String> allSections = new ArrayList<>(commonSection);
                allSections.add(GLOBAL_PROP_SECTION);

                configs.put(s, app.setupJob(properties, Pair.of(s, allSections)));
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
