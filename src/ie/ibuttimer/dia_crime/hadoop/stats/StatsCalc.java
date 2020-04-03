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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.TextUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static ie.ibuttimer.dia_crime.hadoop.stats.AbstractStatsCalc.Stat.STDDEV;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

public class StatsCalc extends AbstractStatsCalc implements IStats {

    private RegressionWritable<String, Value> zero;

    public StatsCalc(Path path, Configuration conf, String filename) {
        super(path, conf, filename);
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
                }
            });

            Value sum = null;
            Value sumOfSq = null;
            Value min = null;
            Value max = null;
            long count = -1;
            for (String key : req) {
                switch (key) {
                    case SQUARE_KEY_TAG:    sumOfSq = readEntry(lines, id, getSquareKeyTag(id));        break;
                    case SUM_KEY_TAG:       sum = readEntry(lines, id, getSumKeyTag(id));               break;
                    case COUNT_KEY_TAG:     count = readLong(lines, getCountKeyTag(id), COUNT_PROP);    break;
                    case MIN_KEY_TAG:       min = readEntry(lines, id, getMinKeyTag(id));               break;
                    case MAX_KEY_TAG:       max = readEntry(lines, id, getMaxKeyTag(id));               break;
                }
            }

            Value finalSumOfSq = sumOfSq;
            Value finalSum = sum;
            long finalCount = count;
            Value finalMin = min;
            Value finalMax = max;
            Result result = new Result();
            stats.forEach(stat -> {
                switch (stat) {
                    case STDDEV:
                    case VARIANCE:
                        assert finalSum != null;
                        assert finalSumOfSq != null;
                        if (finalSum.isPresent() && finalSumOfSq.isPresent()) {
                            if (stat == STDDEV) {
                                result.setStddev(
                                    calcStdDev(finalSum, finalSumOfSq, finalCount)
                                );
                            } else {
                                result.setVariance(
                                    calcVariance(finalSum, finalSumOfSq, finalCount)
                                );
                            }
                        }
                        break;
                    case MEAN:
                        assert finalSum != null;
                        if (finalSum.isPresent()) {
                            double meanVal = calcMean(finalSum, finalCount);
                            result.setMean(meanVal);
                        }
                        break;
                    case MIN:
                        assert finalMin != null;
                        if (finalMin.isPresent()) {
                            result.setMin(finalMin.doubleValue());
                        }
                        break;
                    case MAX:
                        assert finalMax != null;
                        if (finalMax.isPresent()) {
                            result.setMax(finalMax.doubleValue());
                        }
                        break;
                }
                resultSet.set(id, result);
            });
        }
        return resultSet;
    }


    private Value readEntry(List<String> lines, String key, String lineTag) {
        AtomicReference<Value> entry = new AtomicReference<>(Value.of(0));
        lines.stream()
            .filter(l -> l.startsWith(lineTag))
            .findFirst()
            .ifPresent(l -> {
                String line = getValueFromLine(l);
                if (!TextUtils.isEmpty(line)) {
                    entry.set(Value.of(
                        Double.valueOf(line)
                    ));
                }
            });
        return entry.get();
    }

}
