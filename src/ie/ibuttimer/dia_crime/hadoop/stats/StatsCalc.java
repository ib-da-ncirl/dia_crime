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

package ie.ibuttimer.dia_crime.hadoop.stats;

import ie.ibuttimer.dia_crime.hadoop.regression.RegressionWritable;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static ie.ibuttimer.dia_crime.hadoop.stats.AbstractStatsCalc.Stat.STDDEV;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

public class StatsCalc extends AbstractStatsCalc implements IStats {

    private static final Logger logger = Logger.getLogger(StatsCalc.class.getSimpleName());

    private RegressionWritable<String, Value> zero;

    public StatsCalc(Path path, Configuration conf, String filename) {
        super(path, conf, filename, logger);
    }

    private void setZero(List<String> fields) {
        zero = new RegressionWritable<>();
        fields.forEach(f -> zero.put(f, Value.of(BigDecimal.ZERO)));
    }

    @Override
    public Result.Set calcStat(String id, List<Stat> stats, List<String> fields) throws IOException {
        Result.Set resultSet = new Result.Set();

        setZero(fields);

        List<String> lines = getLines(id);
        if (lines != null) {
            Set<String> req = new HashSet<>();
            stats.forEach(stat -> {
                switch (stat) {
                    case STDDEV:
                    case VARIANCE:
                        req.add(SQUARE_KEY_TAG);
                        // fall thru
                    case MEAN:
                        req.add(SUM_KEY_TAG);
                        req.add(COUNT_KEY_TAG);
                        break;
                    case MIN:
                        req.add(MIN_KEY_TAG);
                        break;
                    case MAX:
                        req.add(MAX_KEY_TAG);
                        break;
                    case COUNT:
                        req.add(COUNT_KEY_TAG);
                        break;
                    case ZERO_COUNT:
                        req.add(ZERO_KEY_TAG);
                        break;
                }
            });

            AtomicReference<Value> sum = new AtomicReference<>();
            AtomicReference<Value> sumOfSq = new AtomicReference<>();
            AtomicReference<Value> min = new AtomicReference<>();
            AtomicReference<Value> max = new AtomicReference<>();
            AtomicReference<Value> count = new AtomicReference<>();
            AtomicReference<Value> zero = new AtomicReference<>();
            for (String key : req) {
                switch (key) {
                    case SQUARE_KEY_TAG:    readEntry(lines, getSquareKeyTag(id)).ifPresent(sumOfSq::set);  break;
                    case SUM_KEY_TAG:       readEntry(lines, getSumKeyTag(id)).ifPresent(sum::set);         break;
                    case COUNT_KEY_TAG:     readEntry(lines, getCountKeyTag(id)).ifPresent(count::set);     break;
                    case MIN_KEY_TAG:       readEntry(lines, getMinKeyTag(id)).ifPresent(min::set);         break;
                    case MAX_KEY_TAG:       readEntry(lines, getMaxKeyTag(id)).ifPresent(max::set);         break;
                    case ZERO_KEY_TAG:      readEntry(lines, getZeroKeyTag(id)).ifPresent(zero::set);       break;
                }
            }

            Value finalSumOfSq = sumOfSq.get();
            Value finalSum = sum.get();
            Value finalCount = count.get();
            Value finalZero = zero.get();
            Value finalMin = min.get();
            Value finalMax = max.get();
            Result result = new Result();
            stats.forEach(stat -> {
                switch (stat) {
                    case STDDEV:
                    case VARIANCE:
                        if (finalSum.isPresent() && finalSumOfSq.isPresent() && finalCount.isPresent()) {
                            if (stat == STDDEV) {
                                result.setStddev(
                                    calcStdDev(finalSum, finalSumOfSq, finalCount.longValue())
                                );
                            } else {
                                result.setVariance(
                                    calcVariance(finalSum, finalSumOfSq, finalCount.longValue())
                                );
                            }
                        }
                        break;
                    case MEAN:
                        if (finalSum.isPresent() && finalCount.isPresent()) {
                            double meanVal = calcMean(finalSum, finalCount.longValue());
                            result.setMean(meanVal);
                        }
                        break;
                    case MIN:
                        finalMin.asDouble(result::setMin);
                        break;
                    case MAX:
                        finalMax.asDouble(result::setMax);
                        break;
                    case COUNT:
                        finalCount.asLong(result::setCount);
                        break;
                    case ZERO_COUNT:
                        finalZero.asLong(result::setZeroCount);
                        break;
                }
                resultSet.set(id, result);
            });
        }
        return resultSet;
    }

    @Override
    public Result.Set calcStat(String idX, String idY, List<Stat> stats, List<String> fields) throws IOException {
        Result.Set resultSet = new Result.Set();

        String keyPairXY = getKeyPair(idX, idY);
        String keyPairYX = getKeyPair(idY, idX);
        List<String> lines = new ArrayList<>();
        lines.addAll(getLines(idX));
        lines.addAll(getLines(idY));

        if (lines.size() > 0) {
            Set<String> req = new HashSet<>();
            Map<String, String> corTags = new HashMap<>();
            stats.forEach(stat -> {
                if (stat == Stat.COR) {
                    /* need the following key/values:
                        one of:
                            <property 1 name>+<property 2 name>-PRD-SUM - sum of product of property 1 & 2 values
                            <property 2 name>+<property 1 name>-PRD-SUM - sum of product of property 1 & 2 values
                        <property 1 name>-SQ-SUM - sum of squared values of property 1
                        <property 2 name>-SQ-SUM - sum of squared values of property 2
                        <property 1 name>-SUM - sum of values of property 1
                        <property 2 name>-SUM - sum of values of property 2
                        plus any count (should all be the same)
                        <property 1 name>-CNT - count of values
                     */
                    List.of(Pair.of(keyPairXY, SUMOFPRODUCT_XY),
                            Pair.of(keyPairYX, SUMOFPRODUCT_YX))
                        .forEach(pair -> {
                            String tag = getSumKeyTag(getProductKeyTag(pair.getLeft()));
                            corTags.put(pair.getRight(), tag);
                            req.add(tag);
                        });

                    Consumer<? super Pair<String, String>> consumer = p -> {
                        String ftag = getSumKeyTag(getSquareKeyTag(p.getRight()));
                        corTags.put(p.getLeft(), ftag);
                        req.add(ftag);
                    };
                    List.of(Pair.of(SUMOFXSQ, idX), Pair.of(SUMOFYSQ, idY)).forEach(consumer);
                    consumer = p -> {
                        String ftag = getSumKeyTag(p.getRight());
                        corTags.put(p.getLeft(), ftag);
                        req.add(ftag);
                    };
                    List.of(Pair.of(SUMOFX, idX), Pair.of(SUMOFY, idY)).forEach(consumer);

                    String tag = getCountKeyTag(idX);
                    corTags.put(COUNTOFXY, tag);
                    req.add(tag);
                }
            });

            Map<String, Value> valueMap = new HashMap<>();
            for (String key : req) {
                readEntry(lines, key).ifPresent(v -> valueMap.put(key, v));
            }

            Result result = new Result();
            stats.forEach(stat -> {
                if (stat == Stat.COR) {
                    String sumOfPrdTag = null;
                    String sumOfPrdTagNotReq = null;
                    List<String> missing = new ArrayList<>();

                    corTags.values().forEach(valKey -> {
                        if (!valueMap.containsKey(valKey)) {
                            missing.add(valKey);
                        }
                    });
                    boolean missingPrdXY = missing.contains(corTags.get(SUMOFPRODUCT_XY));
                    boolean missingPrdYX = missing.contains(corTags.get(SUMOFPRODUCT_YX));
                    if (missingPrdXY && !missingPrdYX) {
                        sumOfPrdTag = SUMOFPRODUCT_YX;
                        sumOfPrdTagNotReq = corTags.get(SUMOFPRODUCT_XY);
                        missing.remove(sumOfPrdTagNotReq);
                    } else if (!missingPrdXY && missingPrdYX) {
                        sumOfPrdTag = SUMOFPRODUCT_XY;
                        sumOfPrdTagNotReq = corTags.get(SUMOFPRODUCT_YX);
                        missing.remove(sumOfPrdTagNotReq);
                    }
                    if (missing.size() > 0) {
                        throw new IllegalStateException("Missing value(s) for " + missing);
                    }

                    // save params for result
                    Map<String, Value> params = new HashMap<>();
                    String finalSumOfPrdTagReverse = sumOfPrdTagNotReq;
                    corTags.values().stream()
                        .filter(valKey -> !valKey.equals(finalSumOfPrdTagReverse))
                        .forEach(valKey -> params.put(valKey, valueMap.get(valKey)));

                    result.setCorrelation(
                        calcCorrelation(
                            valueMap.get(corTags.get(sumOfPrdTag)),
                            valueMap.get(corTags.get(SUMOFX)),
                            valueMap.get(corTags.get(SUMOFY)),
                            valueMap.get(corTags.get(SUMOFXSQ)),
                            valueMap.get(corTags.get(SUMOFYSQ)),
                            valueMap.get(corTags.get(COUNTOFXY)).longValue()),
                        params
                    );
                }
                resultSet.set(keyPairXY, result);
            });
        }
        return resultSet;
    }

    private Optional<Value> readEntry(List<String> lines, String keyTag) {
        AtomicReference<Optional<Value>> entry = new AtomicReference<>(Value.empty());
        lines.stream()
            .filter(l -> l.startsWith(keyTag))
            .findFirst()
            .ifPresent(l -> {
                String line = getValueFromLine(l);
                if (!TextUtils.isEmpty(line)) {
                    entry.set(Optional.of(
                        Value.of(
                            Double.valueOf(line)
                        )
                    ));
                }
            });
        return entry.get();
    }

}
