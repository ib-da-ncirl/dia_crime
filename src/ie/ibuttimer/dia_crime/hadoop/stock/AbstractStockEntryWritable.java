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

package ie.ibuttimer.dia_crime.hadoop.stock;

import ie.ibuttimer.dia_crime.hadoop.AbstractBaseWritable;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class AbstractStockEntryWritable<W extends AbstractBaseWritable>
    extends AbstractBaseWritable
    implements Writable {

    public enum Fields { DATE, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME };
    public static List<Fields> NUMERIC_FIELDS = Arrays.asList(Fields.OPEN, Fields.HIGH, Fields.LOW, Fields.CLOSE,
        Fields.ADJ_CLOSE, Fields.VOLUME);

    public abstract Optional<Number> getField(Fields field);

    /**
     * Add other's values to this object's values
     * @param other
     */
    public void add(W other) {
        // no op
    }

    @Override
    public <T extends AbstractBaseWritable> void set(T other) {
        super.set(other);
    }
    /**
     * Set this object's values to the min of this object's and other's values
     * @param other
     */
    public void min(W other) {
        // no op
    }

    /**
     * Set this object's values to the max of this object's and other's values
     * @param other
     */
    public void max(W other) {
        // no op
    }

    public abstract W copyOf();

}
