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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.Constants.*;
import static ie.ibuttimer.dia_crime.misc.Functional.exceptionLoggingConsumer;


public class StatsDriver extends AbstractDriver implements ITagger {

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
                    String dependent = conf.get(propertyWrangler.getPropertyPath(DEPENDENT_PROP), "");
                    FileUtil fileUtil = new FileUtil(outDir, conf);

                    List<String> outputText = new ArrayList<>();

                    // add relevant parameter tags
                    getTagStrings(conf, section).forEach(tagLine -> {
                        outputText.add(COMMENT_PREFIX + " " + tagLine);
                    });
                    outputText.add("");

                    variables.stream().sorted().
                        forEach(
                            exceptionLoggingConsumer(id1 -> {
                                Result.Set results = statsCalc.calcAll(id1, numericTypes);

                                Result result = results.get(id1);
                                if (result.isSuccess()) {

                                    outputText.add(id1);

                                    Arrays.asList(StatsCalc.Stat.values()).forEach(stat -> {
                                        switch (stat) {
                                            case STDDEV:
                                                result.getStddev().ifPresent(st -> outputText.add("  Standard deviation: " + st));
                                                break;
                                            case VARIANCE:
                                                result.getVariance().ifPresent(st -> outputText.add("  Variance: " + st));
                                                break;
                                            case MEAN:
                                                result.getMean().ifPresent(st -> outputText.add("  Mean: " + st));
                                                break;
                                            case MIN:
                                                result.getMin().ifPresent(st -> outputText.add("  Min: " + st));
                                                break;
                                            case MAX:
                                                result.getMax().ifPresent(st -> outputText.add("  Max: " + st));
                                                break;
                                        }
                                    });
                                    outputText.add("");
                                }
                            }, IOException.class, logger)
                        );

                    outputText.add(heading("Correlation array"));
                    try {
                        int count = numericTypes.size();
                        String[][] resultArray = new String[count][count];

                        AtomicInteger width = new AtomicInteger(0);
                        numericTypes.stream().mapToInt(String::length).max().ifPresent(width::set);
                        if (width.get() < 5) {
                            width.set(5);
                        }
                        String strFmt = "%" + width.get() + "s ";
                        String corFmt = "%" + width.get() + ".2f";

                        List<String> sortedNumerics = numericTypes.stream().sorted().collect(Collectors.toList());

                        // calc all correlations
                        List<Map.Entry<String, Result>> corList = statsCalc.calcAllCorrelation(numericTypes)
                            .entryList().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .collect(Collectors.toList());

                        List<CorResults> resultsMap = new ArrayList<>(List.of(
                            new CorResults(0.9, 1.001),
                            new CorResults(0.8, 0.9),
                            new CorResults(0.7, 0.8),
                            new CorResults(0.6, 0.7),
                            new CorResults(0.5, 0.6),
                            new CorResults(0.0, 0.5)
                        ));
                        CorResults anomalies = new CorResults(0.0, 0.0);
                        AtomicInteger kpWidth = new AtomicInteger(0);
                        List<String> skipList = new ArrayList<>();

                        sortedNumerics.forEach(id1 -> {
                            sortedNumerics.stream()
                                .filter(id2 -> !id1.equals(id2))
                                .filter(id2 ->
                                    // reversed properties are not in skip list
                                    skipList.stream()
                                        .noneMatch(statsCalc.getKeyPair(id1, id2)::equals)
                                )
                                .forEach(id2 -> {

                                    String leftRightPair = statsCalc.getKeyPair(id1, id2);
                                    String rightLeftPair = statsCalc.getKeyPair(id2, id1);

                                    // no need to calc right-left as its the same as left-right
                                    skipList.add(rightLeftPair);

                                    if (leftRightPair.length() > kpWidth.get()) {
                                        kpWidth.set(leftRightPair.length());
                                    }

                                    corList.stream()
                                        .filter(e -> e.getKey().equals(leftRightPair) || e.getKey().equals(rightLeftPair))
                                        .findFirst()
                                        .ifPresent(e -> {
                                            Pair<String, String> keyPair = statsCalc.splitKeyPair(e.getKey());
                                            int idx1 = sortedNumerics.indexOf(keyPair.getLeft());
                                            int idx2 = sortedNumerics.indexOf(keyPair.getRight());
                                            e.getValue().getCorrelation().ifPresent(v -> {
                                                resultArray[idx1][idx2] = String.format(corFmt, v);
                                                resultArray[idx2][idx1] = resultArray[idx1][idx2];

                                                double absV = Math.abs(v);
                                                resultsMap.stream()
                                                    .filter(cor -> (cor.min <= absV) && (absV < cor.max))
                                                    .findFirst()
                                                    .orElseGet(() -> anomalies)
                                                    .entries.put(leftRightPair, v);
                                            });
                                        });
                                });
                            });

                        StringBuilder sb = new StringBuilder(String.format(strFmt, ""));
                        sortedNumerics.forEach(id1 -> {
                            sb.append(String.format(strFmt, id1));
                        });
                        String arrayHeader = sb.toString();
                        String dependentBanner = banner(arrayHeader.length(), '*');;
                        outputText.add(String.format("%s%n%s", arrayHeader, dependentBanner));

                        if (dependent.length() == 0) {
                            dependentBanner = null;
                        }

                        String finalDependentBanner = dependentBanner;
                        sortedNumerics.forEach(id1 -> {
                            sb.delete(0, sb.capacity());
                            int idx1 = sortedNumerics.indexOf(id1);
                            boolean addBanner = id1.equals(dependent);
                            sortedNumerics.forEach(id2 -> {

                                if (sb.length() == 0) {
                                    if (addBanner) {
                                        sb.append(String.format("%s%n%s%n%s%n",
                                            finalDependentBanner, arrayHeader, finalDependentBanner));
                                    }
                                    sb.append(String.format(strFmt, id1));
                                }
                                int idx2 = sortedNumerics.indexOf(id2);
                                sb.append(String.format(strFmt, resultArray[idx1][idx2]));
                            });
                            outputText.add(sb.toString());
                        });
                        outputText.add("");

                        outputText.add(heading("Correlation tiers"));
                        resultsMap.add(anomalies);
                        resultsMap.forEach(tier -> {
                            if (tier.min == tier.max) {
                                outputText.add("Anomalies (see individual correlations for more details");
                            } else {
                                StringBuilder csb = new StringBuilder("Correlation >= " + tier.min);
                                if (tier.max < 1.0) {
                                    csb.append(", < ").append(tier.max);
                                }
                                outputText.add(csb.toString());
                            }

                            tier.entries.entrySet().stream()
                                .sorted((es1, es2) -> {
                                    double v1 = es1.getValue();
                                    double v2 = es2.getValue();
                                    if (v1 > v2) {
                                        return -1;
                                    } else if (v1 < v2) {
                                        return 1;
                                    }
                                    return 0;
                                })
                                .forEach(es ->
                                    outputText.add(String.format("  %-" + kpWidth.get() + "s : %f", es.getKey(), es.getValue()))
                                );
                            outputText.add("");
                        });

                        outputText.add(heading("Individual correlations"));
                        corList.forEach(map -> {
                            Result corRes = map.getValue();
                            if (corRes.isSuccess()) {
                                outputText.add(map.getKey());
                                corRes.getCorrelationAndParams().ifPresent(cor -> {
                                    StringBuilder psb = new StringBuilder()
                                        .append(String.format("  Correlation: %f%n", cor.getLeft()))
                                        .append(              "  Params: ");
                                    cor.getRight().forEach((key, value) -> {
                                        psb.append(key).append("=");
                                        value.asString(psb::append);
                                        psb.append(" ");
                                    });
                                    outputText.add(
                                        psb.append(String.format("%n%n")).toString()
                                    );
                                });
                            }
                        });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

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

    private String banner(int length, char chr) {
        char[] line = new char[length];
        Arrays.fill(line, chr);
        return new String(line);
    }

    private String heading(String heading) {
        return String.format("%s%n%s%n", heading, banner(heading.length(), '-'));
    }


    class CorResults {
        double min;
        double max;
        Map<String, Double> entries;

        public CorResults(double min, double max) {
            this.min = min;
            this.max = max;
            this.entries = new HashMap<>();
        }
    }

}
