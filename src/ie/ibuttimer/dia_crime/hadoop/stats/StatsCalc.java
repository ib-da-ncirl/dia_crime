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

import com.google.common.base.Charsets;
import ie.ibuttimer.dia_crime.hadoop.io.FileUtil;
import ie.ibuttimer.dia_crime.hadoop.stock.BigStockWritable;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.TextUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.hadoop.stats.StatsCalc.Stat.*;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

public class StatsCalc implements IStats {

    public enum Stat { STDDEV, VARIANCE, MEAN, MIN, MAX }

    private String filename;
    private FileUtil fileUtil;

    public StatsCalc(Path path, Configuration conf, String filename) {
        this.filename = filename;
        this.fileUtil = new FileUtil(path, conf);
    }

    public Result.Set calcStat(String id, List<Stat> stats, List<String> fields) throws IOException {
        Result.Set resultSet = new Result.Set();
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

            BigStockWritable sum = null;
            BigStockWritable sumOfSq = null;
            BigStockWritable min = null;
            BigStockWritable max = null;
            long count = -1;
            for (String key : req) {
                switch (key) {
                    case SQUARE_KEY_TAG:    sumOfSq = readBigStock(lines, getSquareKeyTag(id));         break;
                    case SUM_KEY_TAG:       sum = readBigStock(lines, getSumKeyTag(id));                break;
                    case COUNT_KEY_TAG:     count = readLong(lines, getCountKeyTag(id), COUNT_PROP);    break;
                    case MIN_KEY_TAG:       min = readBigStock(lines, getMinKeyTag(id));                break;
                    case MAX_KEY_TAG:       max = readBigStock(lines, getMaxKeyTag(id));                break;
                }
            }

            BigStockWritable finalSumOfSq = sumOfSq;
            BigStockWritable finalSum = sum;
            long finalCount = count;
            BigStockWritable finalMin = min;
            BigStockWritable finalMax = max;
            fields.forEach(f -> {
                Result result = new Result();
                stats.forEach(stat -> {
                    switch (stat) {
                        case STDDEV:
                        case VARIANCE:
                            assert finalSum != null;
                            assert finalSumOfSq != null;
                            if (finalSum.getField(f).isPresent() && finalSumOfSq.getField(f).isPresent()) {
                                if (stat == STDDEV) {
                                    result.setStddev(
                                        calcStdDev(finalSum.getField(f).get(), finalSumOfSq.getField(f).get(), finalCount)
                                    );
                                } else {
                                    result.setVariance(
                                        calcVariance(finalSum.getField(f).get(), finalSumOfSq.getField(f).get(), finalCount)
                                    );
                                }
                            }
                            break;
                        case MEAN:
                            assert finalSum != null;
                            finalSum.getField(f).ifPresent(s -> {
                                BigDecimal meanVal;
                                if (s.isBigInteger()) {
                                    meanVal = calcMean(s.bigIntegerValue(), finalCount);
                                } else if (s.isBigDecimal()) {
                                    meanVal = calcMean(s.bigDecimalValue(), finalCount);
                                } else {
                                    throw new UnsupportedOperationException("Primitive means not supported atm");
                                }
                                result.setMean(meanVal.doubleValue());
                            });
                            break;
                        case MIN:
                            assert finalMin != null;
                            finalMin.getField(f).ifPresent(m -> {
                                result.setMin(m.doubleValue());
                            });
                            break;
                        case MAX:
                            assert finalMax != null;
                            finalMax.getField(f).ifPresent(m -> {
                                result.setMax(m.doubleValue());
                            });
                            break;
                    }
                    resultSet.set(f, result);
                });
            });
        }
        return resultSet;
    }

    private double calcStdDev(Value sum, Value sumOfSq, long count) {
        double stddev;
        if (sumsumOfSqCheck(sum, sumOfSq).equals(BigDecimal.class.getSimpleName())) {
            stddev = calcStdDev(sum.bigDecimalValue(), sumOfSq.bigDecimalValue(), count);
        } else {
            stddev = calcStdDev(sum.bigIntegerValue(), sumOfSq.bigIntegerValue(), count);
        }
        return stddev;
    }

    private double calcVariance(Value sum, Value sumOfSq, long count) {
        BigDecimal stddev;
        if (sumsumOfSqCheck(sum, sumOfSq).equals(BigDecimal.class.getSimpleName())) {
            stddev = calcVariance(sum.bigDecimalValue(), sumOfSq.bigDecimalValue(), count);
        } else {
            stddev = calcVariance(sum.bigIntegerValue(), sumOfSq.bigIntegerValue(), count);
        }
        return stddev.doubleValue();
    }

    private String sumsumOfSqCheck(Value sum, Value sumOfSq) {
        String type;
        Class<?> sumCls = sum.getValueClass();
        Class<?> sumOfSqCls = sumOfSq.getValueClass();
        if (!sumCls.equals(sumOfSqCls)) {
            throw new IllegalArgumentException("Mixed type sum and sum of squares arg");
        } else if (sumCls.equals(BigDecimal.class)) {
            type = BigDecimal.class.getSimpleName();
        } else if (sumCls.equals(BigInteger.class)) {
            type = BigInteger.class.getSimpleName();
        } else {
            throw new UnsupportedOperationException("Only BigNumbers supported atm");
        }
        return type;
    }

    public Result.Set calcStdDev(String id, List<String> fields) throws IOException {
        return calcStat(id, STDDEV, fields);
    }

    public Result.Set calcMean(String id, List<String> fields) throws IOException {
        return calcStat(id, MEAN, fields);
    }

    public Result.Set calcAll(String id, List<String> fields) throws IOException {
        return calcStat(id, Arrays.asList(Stat.values()), fields);
    }

    public Result.Set calcStat(String id, Stat stat, List<String> fields) throws IOException {
        return calcStat(id, Collections.singletonList(stat), fields);
    }


    private List<String> getLines(String id) throws IOException {
        List<String> lines = null;
        if (fileUtil.wasSuccess()) {
            FSDataInputStream stream = fileUtil.fileReadOpen(filename);

            try (InputStreamReader inputStream = new InputStreamReader(stream, Charsets.UTF_8);
                    BufferedReader reader = new BufferedReader(inputStream)
            ) {
                lines = reader.lines()
                    .filter(l -> l.startsWith(id))
                    .collect(Collectors.toList());
            }
        }
        return lines;
    }

    private BigStockWritable readBigStock(List<String> lines, String lineTag) {
        BigStockWritable entry = new BigStockWritable();
        lines.stream()
            .filter(l -> l.startsWith(lineTag))
            .findFirst()
            .ifPresent(l -> {
                String line = getValueFromLine(l);
                if (!TextUtils.isEmpty(line)) {
                    Map<String, String> map = MapStringifier.mapify(line);
                    entry.setOpen(map.getOrDefault(OPEN_PROP, "0"));
                    entry.setHigh(map.getOrDefault(HIGH_PROP, "0"));
                    entry.setLow(map.getOrDefault(LOW_PROP, "0"));
                    entry.setClose(map.getOrDefault(CLOSE_PROP, "0"));
                    entry.setAdjClose(map.getOrDefault(ADJCLOSE_PROP, "0"));
                    entry.setVolume(map.getOrDefault(VOLUME_PROP, "0"));
                }
            });
        return entry;
    }

    public long readLong(List<String> lines, String lineTag, String valueTag) {
        AtomicLong value = new AtomicLong();
        lines.stream()
            .filter(l -> l.startsWith(lineTag))
            .findFirst()
            .ifPresent(l -> {
                String line = getValueFromLine(l);
                if (!TextUtils.isEmpty(line)) {
                    Map<String, String> map = MapStringifier.mapify(line);
                    value.set(Long.parseLong(map.getOrDefault(valueTag, "0")));
                }
            });
        return value.get();
    }



    private String getValueFromLine(String line) {
        // split on tab and value is 2nd
        // e.g. IXIC_SUM	adjclose:504678.989255, close:504678.989255, high:512398.669308, low:496843.279168, open:504955.630492, volume:465069500000
        String[] splits = line.split("\t");
        return (splits.length == 2 ? splits[1] : "");
    }

    private BigDecimal calcMean(BigDecimal sum, long count) {
        return sum.divide(BigDecimal.valueOf(count), RoundingMode.CEILING);
    }

    private BigDecimal calcVariance(BigDecimal sum, BigDecimal sumOfSq, long count) {
        /* variance = (sum of differences from mean squared) / count
                    = (sum of squares / count) - (mean^2 / count)
         */
        BigDecimal meanSq = calcMean(sum, count).pow(2);
        BigDecimal avgSumOfSq = calcMean(sumOfSq, count);
        return avgSumOfSq.subtract(meanSq);
    }

    private double calcStdDev(BigDecimal sum, BigDecimal sumOfSq, long count) {
        /* standard deviation = sqrt(variance) */
        return Math.sqrt(calcVariance(sum, sumOfSq, count).doubleValue());
    }

    private BigDecimal calcMean(BigInteger sum, long count) {
        BigInteger mean = sum.divide(BigInteger.valueOf(count));
        return BigDecimal.valueOf(mean.doubleValue());
    }

    private BigDecimal calcVariance(BigInteger sum, BigInteger sumOfSq, long count) {
        return calcVariance(new BigDecimal(sum), new BigDecimal(sumOfSq), count);
    }

    private double calcStdDev(BigInteger sum, BigInteger sumOfSq, long count) {
        /* standard deviation = sqrt(variance) */
        return Math.sqrt(calcVariance(sum, sumOfSq, count).doubleValue());
    }


}
