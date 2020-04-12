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

import ie.ibuttimer.dia_crime.hadoop.stock.BigStockWritable;
import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ie.ibuttimer.dia_crime.hadoop.stats.AbstractStatsCalc.Stat.STDDEV;
import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Stocks specific statistics calculation
 */
@Deprecated
public class StockStatsCalc extends AbstractStatsCalc {

    private static final Logger logger = Logger.getLogger(StockStatsCalc.class.getSimpleName());

    public StockStatsCalc(Path path, Configuration conf, String filename) {
        super(path, conf, filename, logger);
    }

    @Override
    public Result.Set calcStat(String id, List<Stat> stats, List<String> fields) throws IOException {
        Result.Set resultSet = new Result.Set();
        List<String> lines = getLines(id);
        if (lines != null) {
            Set<NameTag> req = new HashSet<>();
            stats.forEach(stat -> {
                switch (stat) {
                    case STDDEV:
                    case VARIANCE:
                        req.add(NameTag.SQ);
                        // fall thru
                    case MEAN:
                        req.add(NameTag.SUM);
                        // fall thru
                    case COUNT:
                        req.add(NameTag.CNT);
                        break;
                    case MIN:
                        req.add(NameTag.MIN);
                        break;
                    case MAX:
                        req.add(NameTag.MAX);
                        break;
                    case ZERO_COUNT:
                        req.add(NameTag.ZERO);
                        break;
                }
            });

            BigStockWritable sum = null;
            BigStockWritable sumOfSq = null;
            BigStockWritable min = null;
            BigStockWritable max = null;
            long count = -1;
            for (NameTag key : req) {
                switch (key) {
                    case SQ:    sumOfSq = readBigStock(lines, key.getKeyTag(id));       break;
                    case SUM:   sum = readBigStock(lines, key.getKeyTag(id));           break;
                    case CNT:   count = readLong(lines, key.getKeyTag(id), COUNT_PROP); break;
                    case MIN:   min = readBigStock(lines, key.getKeyTag(id));           break;
                    case MAX:   max = readBigStock(lines, key.getKeyTag(id));           break;
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

    @Override
    public Result.Set calcStat(String id1, String id2, List<Stat> stats, List<String> fields) throws IOException {
        throw new UnsupportedOperationException("2 property stats not supported for stocks");
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

}
