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
import ie.ibuttimer.dia_crime.hadoop.stats.IStatWritable;
import org.apache.hadoop.io.Writable;

import java.util.*;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

public abstract class AbstractStockEntryWritable<W extends AbstractBaseWritable>
    extends AbstractBaseWritable<W>
    implements Writable, IStatWritable<W> {

    public static List<String> FIELDS;
    public static List<String> NUMERIC_FIELDS = Arrays.asList(OPEN_PROP, HIGH_PROP, LOW_PROP, CLOSE_PROP, ADJCLOSE_PROP,
        VOLUME_PROP);
    static {
        FIELDS = new ArrayList<>(AbstractBaseWritable.FIELDS);
        FIELDS.addAll(NUMERIC_FIELDS);
    }

    @Override
    public List<String> getFieldsList() {
        return FIELDS;
    }

}
