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
import ie.ibuttimer.dia_crime.misc.Functional;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class AbstractStatsCalc implements IStats {

    public enum Stat {
        STDDEV,     // standard deviation
        VARIANCE,   // variance
        MEAN,       // mean
        MIN,        // min
        MAX,        // max
        COR;        // pearson correlation coefficient

        public static List<Stat> singleElementValues() {
            return Arrays.stream(values())
                .filter(s -> multipleElementValues().contains(s))
                .collect(Collectors.toList());
        }

        public static List<Stat> multipleElementValues() {
            return Collections.singletonList(COR);
        }
    }

    protected static final String SUMOFPRODUCT_XY = "sumOfProductXY";
    protected static final String SUMOFPRODUCT_YX = "sumOfProductYX";
    protected static final String SUMOFX = "sumOfX";
    protected static final String SUMOFY = "sumOfY";
    protected static final String SUMOFXSQ = "sumOfXSq";
    protected static final String SUMOFYSQ = "sumOfYSq";

    private String filename;
    private FileUtil fileUtil;
    private Logger logger;

    public AbstractStatsCalc(Path path, Configuration conf, String filename, Logger logger) {
        this.filename = filename;
        this.fileUtil = new FileUtil(path, conf);
        this.logger = logger;
    }

    protected double calcStdDev(Value sum, Value sumOfSq, long count) {
        double stddev;
        switch (sumSumOfSqCheck(sum, sumOfSq)) {
            case BIG_DECIMAL:
                stddev = calcStdDev(sum.bigDecimalValue(), sumOfSq.bigDecimalValue(), count);
                break;
            case BIG_INT:
                stddev = calcStdDev(sum.bigIntegerValue(), sumOfSq.bigIntegerValue(), count);
                break;
            default:
                stddev = calcStdDev(sum.doubleValue(), sumOfSq.doubleValue(), count);
                break;
        }
        return stddev;
    }

    protected double calcVariance(Value sum, Value sumOfSq, long count) {
        double variance;
        switch (sumSumOfSqCheck(sum, sumOfSq)) {
            case BIG_DECIMAL:
                variance = calcVariance(sum.bigDecimalValue(), sumOfSq.bigDecimalValue(), count)
                            .doubleValue();
                break;
            case BIG_INT:
                variance = calcVariance(sum.bigIntegerValue(), sumOfSq.bigIntegerValue(), count)
                            .doubleValue();
                break;
            default:
                variance = calcVariance(sum.doubleValue(), sumOfSq.doubleValue(), count);
                break;
        }
        return variance;
    }

    protected double calcMean(Value sum, long count) {
        double mean;
        switch (typeCheck(sum)) {
            case BIG_DECIMAL:
                mean = calcMean(sum.bigDecimalValue(), count).doubleValue();
                break;
            case BIG_INT:
                mean = calcMean(sum.bigIntegerValue(), count).doubleValue();
                break;
            default:
                mean = calcMean(sum.doubleValue(), count);
                break;
        }
        return mean;
    }

    protected double calcCorrelation(Value sumOfProductXY, Value sumOfX, Value sumOfY,
                                     Value sumOfXSq, Value sumOfYSq, long count) {
        double cor;
        switch (typeCheck(sumOfProductXY)) {
            case BIG_DECIMAL:
                cor = calcCorrelation(sumOfProductXY.bigDecimalValue(), sumOfX.bigDecimalValue(), sumOfY.bigDecimalValue(),
                    sumOfXSq.bigDecimalValue(), sumOfYSq.bigDecimalValue(), count).doubleValue();
                break;
            case BIG_INT:
                cor = calcCorrelation(sumOfProductXY.bigIntegerValue(), sumOfX.bigIntegerValue(), sumOfY.bigIntegerValue(),
                    sumOfXSq.bigIntegerValue(), sumOfYSq.bigIntegerValue(), count).doubleValue();
                break;
            default:
                cor = calcCorrelation(sumOfProductXY.doubleValue(), sumOfX.doubleValue(), sumOfY.doubleValue(),
                    sumOfXSq.doubleValue(), sumOfYSq.doubleValue(), count);
                break;
        }
        return cor;
    }

    private enum CalcType { BIG_INT, BIG_DECIMAL, DOUBLE }

    protected CalcType sumSumOfSqCheck(Value sum, Value sumOfSq) {
        CalcType type;
        CalcType sumCls = typeCheck(sum);
        CalcType sumOfSqCls = typeCheck(sumOfSq);
        if (!sumCls.equals(sumOfSqCls)) {
            throw new IllegalArgumentException("Mixed type sum and sum of squares arg");
        } else {
            type = sumCls;
        }
        return type;
    }

    protected CalcType typeCheck(Value val) {
        CalcType type;
        Class<?> valClass = val.getValueClass();
        if (valClass.equals(BigDecimal.class)) {
            type = CalcType.BIG_DECIMAL;
        } else if (valClass.equals(BigInteger.class)) {
            type = CalcType.BIG_INT;
        } else {
            type = CalcType.DOUBLE;
        }
        return type;
    }

    public abstract Result.Set calcStat(String id, List<Stat> stats, List<String> fields) throws IOException;

    public abstract Result.Set calcStat(String id1, String id2, List<Stat> stats, List<String> fields) throws IOException;

    public Result.Set calcStdDev(String id, List<String> fields) throws IOException {
        return calcStat(id, Stat.STDDEV, fields);
    }

    public Result.Set calcMean(String id, List<String> fields) throws IOException {
        return calcStat(id, Stat.MEAN, fields);
    }

    public Result.Set calcCorrelation(String id1, String id2, List<String> fields) throws IOException {
        return calcStat(id1, id2, Stat.COR, fields);
    }

    public Result.Set calcAllCorrelation(List<String> fields) throws IOException {
        Result.Set results = new Result.Set();

        List<String> sortedFields = fields.stream().sorted().collect(Collectors.toList());
        AtomicInteger index = new AtomicInteger(0);

        sortedFields.forEach(id1 -> {
            sortedFields.stream()
                .skip(index.get())
                .filter(id2 -> !id1.equals(id2))
                .forEach(
                    Functional.exceptionLoggingConsumer(
                        id2 -> {
                            Result.Set res = calcCorrelation(id1, id2, fields);
                            res.entryList().forEach(e -> results.set(e.getKey(), e.getValue()));
                        }, IOException.class, logger
                    )
                );
            index.incrementAndGet();
        });

        return results;
    }

    public Result.Set calcAll(String id, List<String> fields) throws IOException {
        return calcStat(id, Arrays.asList(Stat.values()), fields);
    }

    public Result.Set calcStat(String id, Stat stat, List<String> fields) throws IOException {
        return calcStat(id, Collections.singletonList(stat), fields);
    }

    public Result.Set calcStat(String id1, String id2, Stat stat, List<String> fields) throws IOException {
        return calcStat(id1, id2, Collections.singletonList(stat), fields);
    }


    protected List<String> getLines(String id) throws IOException {
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

    protected String getValueFromLine(String line) {
        // split on tab and value is 2nd
        // e.g. IXIC_SUM	adjclose:504678.989255, close:504678.989255, high:512398.669308, low:496843.279168, open:504955.630492, volume:465069500000
        String[] splits = line.split("\t");
        return (splits.length == 2 ? splits[1] : "");
    }

    protected double calcMean(double sum, long count) {
        return sum / count;
    }

    protected double calcVariance(double sum, double sumOfSq, long count) {
        /* variance = (sum of differences from mean squared) / count
                    = (sum of squares / count) - (mean^2 / count)
         */
        double meanSq = Math.pow(calcMean(sum, count), 2);
        double avgSumOfSq = calcMean(sumOfSq, count);
        return avgSumOfSq - meanSq;
    }

    protected double calcStdDev(double sum, double sumOfSq, long count) {
        /* standard deviation = sqrt(variance) */
        return Math.sqrt(calcVariance(sum, sumOfSq, count));
    }

    protected double calcCorrelation(double sumOfProductXY, double sumOfX, double sumOfY,
                                     double sumOfXSq, double sumOfYSq, long count) {
        /*                        (n * sumOfProductXY) - (sumOfX * sumOfY)
           correlation = -----------------------------------------------------------------
                         sqrt( ((n * sumOfXSq) - sumOfX^2) * ((n * sumOfYSq) - sumOfY^2) )
        */
        double divisor = overflowCheck(count * sumOfProductXY) - overflowCheck(sumOfX * sumOfY);
        double dividendL = overflowCheck(count * sumOfXSq) - overflowCheck(Math.pow(sumOfX, 2));
        double dividendR = overflowCheck(count * sumOfYSq) - overflowCheck(Math.pow(sumOfY, 2));
        return divisor / sqrt(overflowCheck(dividendL * dividendR));
    }

    private double overflowCheck(double num) {
        if (Double.isInfinite(num) || Double.isNaN(num)) {
            throw new ArithmeticException("Overflow during calculation");
        }
        return num;
    }

    private double sqrt(double num) {
        if (num < 0.0) {
            throw new ArithmeticException("Square root of minus number [" + num + "]is NaN");
        } else {
            num = Math.sqrt(num);
        }
        return num;
    }

    protected BigDecimal calcMean(BigDecimal sum, long count) {
        return sum.divide(BigDecimal.valueOf(count), RoundingMode.UP);
    }

    protected BigDecimal calcVariance(BigDecimal sum, BigDecimal sumOfSq, long count) {
        /* variance = (sum of differences from mean squared) / count
                    = (sum of squares / count) - (mean^2 / count)
         */
        BigDecimal meanSq = calcMean(sum, count).pow(2);
        BigDecimal avgSumOfSq = calcMean(sumOfSq, count);
        return avgSumOfSq.subtract(meanSq);
    }

    protected double calcStdDev(BigDecimal sum, BigDecimal sumOfSq, long count) {
        /* standard deviation = sqrt(variance) */
        return Math.sqrt(calcVariance(sum, sumOfSq, count).doubleValue());
    }

    protected BigDecimal calcCorrelation(BigDecimal sumOfProductXY, BigDecimal sumOfX, BigDecimal sumOfY,
                                     BigDecimal sumOfXSq, BigDecimal sumOfYSq, long count) {
        /*                        (n * sumOfProductXY) - (sumOfX * sumOfY)
           correlation = -----------------------------------------------------------------
                         sqrt( ((n * sumOfXSq) - sumOfX^2) * ((n * sumOfYSq) - sumOfY^2) )
        */
        BigDecimal bgCount = BigDecimal.valueOf(count);
        BigDecimal divisor = sumOfProductXY.multiply(bgCount).subtract(sumOfX.multiply(sumOfY));
        BigDecimal dividendX = sumOfXSq.multiply(bgCount).subtract(sumOfX.pow(2));
        BigDecimal dividendY = sumOfYSq.multiply(bgCount).subtract(sumOfY.pow(2));
        return divisor.divide(dividendX.multiply(dividendY).sqrt(MathContext.DECIMAL64), RoundingMode.UP);
    }


    protected BigDecimal calcMean(BigInteger sum, long count) {
        BigInteger mean = sum.divide(BigInteger.valueOf(count));
        return BigDecimal.valueOf(mean.doubleValue());
    }

    protected BigDecimal calcVariance(BigInteger sum, BigInteger sumOfSq, long count) {
        return calcVariance(new BigDecimal(sum), new BigDecimal(sumOfSq), count);
    }

    protected double calcStdDev(BigInteger sum, BigInteger sumOfSq, long count) {
        /* standard deviation = sqrt(variance) */
        return Math.sqrt(calcVariance(sum, sumOfSq, count).doubleValue());
    }

    protected BigDecimal calcCorrelation(BigInteger sumOfProductXY, BigInteger sumOfX, BigInteger sumOfY,
                                     BigInteger sumOfXSq, BigInteger sumOfYSq, long count) {
        return calcCorrelation(new BigDecimal(sumOfProductXY), new BigDecimal(sumOfX), new BigDecimal(sumOfY),
                                new BigDecimal(sumOfXSq), new BigDecimal(sumOfYSq), count);
    }
}
