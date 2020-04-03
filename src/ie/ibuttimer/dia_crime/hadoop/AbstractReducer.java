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

package ie.ibuttimer.dia_crime.hadoop;

import ie.ibuttimer.dia_crime.hadoop.merge.IValueDecorator;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class AbstractReducer<KI, VI, KO, VO> extends Reducer<KI, VI, KO, VO> {

    public static final String CLASS_VAL_SEPARATOR = ":-";

    private IValueDecorator<VO> decorator;

    private static Logger logger = null;

    public AbstractReducer() {
        this(null);
    }

    public AbstractReducer(IValueDecorator<VO> decorator) {
        this.decorator = decorator;
    }

    public void setDecorator(IValueDecorator<VO> decorator) {
        this.decorator = decorator;
    }

    public static Logger getLogger() {
        if (logger == null) {
            setLogger(AbstractReducer.class);
        }
        return logger;
    }

    public static void setLogger(Class<?> cls) {
        setLogger(Logger.getLogger(cls));
    }

    public static void setLogger(Logger logger) {
        AbstractReducer.logger = logger;
    }

    public VO decorate(VO value) {
        VO decorated;
        if (decorator != null) {
            decorated = decorate(value);
        } else {
            decorated = value;
        }
        return decorated;
    }

    protected Counters.ReducerCounter getCounter(Context context, String group, String name) {
        return new Counters.ReducerCounter(context, group, name);
    }

    protected Counters.ReducerCounter getCounter(Context context, CountersEnum countersEnum) {
        return getCounter(context, countersEnum.getClass().getName(), countersEnum.toString());
    }

    public void write(Context context, KO key, VO value) throws IOException, InterruptedException {
        context.write(key, decorate(value));
    }

}
