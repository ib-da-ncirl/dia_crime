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

import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class Result {
    private Map<String, Double> values;
    private Map<String, Map<String, Value>> calcParams;

    Result() {
        this.values = new HashMap<>();
        this.calcParams = new HashMap<>();
    }

    public boolean isSuccess() {
        return values.size() > 0;
    }

    private boolean isSet(AbstractStatsCalc.Stat stat) {
        return values.keySet().stream().anyMatch(stat.name()::equals);
    }

    private void set(AbstractStatsCalc.Stat stat, double value) {
        values.put(stat.name(), value);
    }

    private void set(AbstractStatsCalc.Stat stat, double value, Map<String, Value> params) {
        values.put(stat.name(), value);
        calcParams.put(stat.name(), params);
    }

    public Optional<Double> getStat(AbstractStatsCalc.Stat stat) {
        if (isSet(stat)) {
            return Optional.of(values.get(stat.name()));
        } else {
            return Optional.empty();
        }
    }

    public Optional<Pair<Double, Map<String, Value>>> getStatAndParams(AbstractStatsCalc.Stat stat) {
        if (isSet(stat)) {
            return Optional.of(Pair.of(
                    values.get(stat.name()), calcParams.get(stat.name())
                )
            );
        } else {
            return Optional.empty();
        }
    }

    public Optional<Double> getStddev() {
        return getStat(AbstractStatsCalc.Stat.STDDEV);
    }

    public void setStddev(double stddev) {
        set(AbstractStatsCalc.Stat.STDDEV, stddev);
    }

    public Optional<Double> getVariance() {
        return getStat(AbstractStatsCalc.Stat.VARIANCE);
    }

    public void setVariance(double variance) {
        set(AbstractStatsCalc.Stat.VARIANCE, variance);
    }

    public Optional<Double> getMean() {
        return getStat(AbstractStatsCalc.Stat.MEAN);
    }

    public void setMean(double mean) {
        set(AbstractStatsCalc.Stat.MEAN, mean);
    }

    public Optional<Double> getMin() {
        return getStat(AbstractStatsCalc.Stat.MIN);
    }

    public void setMin(double min) {
        set(AbstractStatsCalc.Stat.MIN, min);
    }

    public Optional<Double> getMax() {
        return getStat(AbstractStatsCalc.Stat.MAX);
    }

    public void setMax(double max) {
        set(AbstractStatsCalc.Stat.MAX, max);
    }

    public Optional<Double> getCorrelation() {
        return getStat(AbstractStatsCalc.Stat.COR);
    }

    public Optional<Pair<Double, Map<String, Value>>> getCorrelationAndParams() {
        return getStatAndParams(AbstractStatsCalc.Stat.COR);
    }

    public void setCorrelation(double cor, Map<String, Value> params) {
        set(AbstractStatsCalc.Stat.COR, cor, params);
    }


    public static class Set {

        private Map<String, Result> set;

        public Set() {
            this.set = new HashMap<>();
        }

        public Set(List<String> keys) {
            this();
            keys.forEach(k -> set.put(k, null));
        }

        public void set(String key, Result result) {
            set.put(key, result);
        }

        public Result get(String key) {
            Result result;
            if (set.containsKey(key)) {
                result = set.get(key);
            } else {
                result = new Result();
            }
            return result;
        }

        public List<Map.Entry<String, Result>> entryList() {
            return new ArrayList<>(set.entrySet());
        }
    }
}
