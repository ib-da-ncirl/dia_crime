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

package ie.ibuttimer.dia_crime.hadoop.matrix;

import ie.ibuttimer.dia_crime.hadoop.AbstractReducer;
import ie.ibuttimer.dia_crime.hadoop.CountersEnum;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import org.apache.hadoop.io.*;
import org.apache.hadoop.shaded.com.google.common.util.concurrent.AtomicDouble;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Matrix multiplication reducer
 */
public class MatrixReducer extends AbstractReducer<CoordinateWritable, MatrixWritable, Text, DoubleWritable> {

    private Text keyOut;
    private DoubleWritable valueOut;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        keyOut = new Text();
        valueOut = new DoubleWritable();
    }

    /**
     * Reduce the values for a key
     * @param key       Key value; date string
     * @param values    Values for the specified key
     * @param context   Current context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(CoordinateWritable key, Iterable<MatrixWritable> values, Context context) throws IOException, InterruptedException {

        Counters.ReducerCounter counter = getCounter(context, CountersEnum.MATRIX_REDUCER_COUNT);

        // have one entry for the row of the 1st matrix and all the individual entries entries for a column of the 2nd matrix
        AtomicReference<MatrixWritable> multiplicand = new AtomicReference<>();
        ArrayList<MatrixWritable> multiplier = new ArrayList<>();
        values.forEach(iter -> {
            MatrixWritable value = iter.copyOf();
            if (value.getId().equals(MatrixMapper.EqElement.MULTIPLICAND.name())) {
                multiplicand.set(value);
            } else {
                multiplier.add(value);
            }
        });

        AtomicDouble calculated = new AtomicDouble(0);
        AtomicInteger column = new AtomicInteger(0);
        multiplier.stream()
            .sorted(Comparator.comparingLong(MatrixWritable::getRow))
            .map(mulRow -> mulRow.getValue().get(0))
            .forEach(mulRow -> {
                // accumulate m1 column value * m2 row value
                double multLhs = multiplicand.get().getValue().get(column.getAndIncrement());
                calculated.getAndAdd(multLhs * mulRow);
            });

        keyOut.set(String.format("%d,%d", key.getRow(), key.getCol()));
        valueOut.set(calculated.get());

        // e.g. 0,0   1234
        context.write(keyOut, valueOut);
    }

}
