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

import java.util.*;

public class Result {
    private boolean success;
    private double stddev;
    private double variance;
    private double mean;
    private double min;
    private double max;

    private boolean[] set = new boolean[StockStatsCalc.Stat.values().length];

    Result() {
        this.success = false;
        this.stddev = 0;
        this.variance = 0;
        this.mean = 0;
        this.min = 0;
        this.max = 0;

        Arrays.fill(this.set, false);
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    private boolean isSet(StockStatsCalc.Stat stat) {
        return set[stat.ordinal()];
    }

    private void set(StockStatsCalc.Stat stat) {
        set[stat.ordinal()] = true;
        success = true;
    }

    private Optional<Double> getStat(StockStatsCalc.Stat stat) {
        if (isSet(stat)) {
            double value;
            switch (stat) {
                case STDDEV:    value = stddev;     break;
                case VARIANCE:  value = variance;   break;
                case MEAN:      value = mean;       break;
                case MIN:       value = min;        break;
                case MAX:       value = max;        break;
                default:    throw new IllegalArgumentException("Unknown stat " + stat.toString());
            }
            return Optional.of(value);
        } else {
            return Optional.empty();
        }
    }

    public Optional<Double> getStddev() {
        return getStat(StockStatsCalc.Stat.STDDEV);
    }

    public void setStddev(double stddev) {
        this.stddev = stddev;
        set(StockStatsCalc.Stat.STDDEV);
    }

    public Optional<Double> getVariance() {
        return getStat(StockStatsCalc.Stat.VARIANCE);
    }

    public void setVariance(double variance) {
        this.variance = variance;
        set(StockStatsCalc.Stat.VARIANCE);
    }

    public Optional<Double> getMean() {
        return getStat(StockStatsCalc.Stat.MEAN);
    }

    public void setMean(double mean) {
        this.mean = mean;
        set(StockStatsCalc.Stat.MEAN);
    }

    public Optional<Double> getMin() {
        return getStat(StockStatsCalc.Stat.MIN);
    }

    public void setMin(double min) {
        this.min = min;
        set(StockStatsCalc.Stat.MIN);
    }

    public Optional<Double> getMax() {
        return getStat(StockStatsCalc.Stat.MAX);
    }

    public void setMax(double max) {
        this.max = max;
        set(StockStatsCalc.Stat.MAX);
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
    }
}
