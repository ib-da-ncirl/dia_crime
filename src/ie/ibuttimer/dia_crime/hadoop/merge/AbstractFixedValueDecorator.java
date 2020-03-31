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

package ie.ibuttimer.dia_crime.hadoop.merge;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class AbstractFixedValueDecorator<KI, VI, KO, VO> implements IValueDecorator<VO> {

    private VO fixedValue;

    public AbstractFixedValueDecorator(VO fixedValue) {
        this.fixedValue = fixedValue;
    }

    public VO getFixedValue() {
        return fixedValue;
    }

    public void write(Mapper<KI, VI, KO, VO>.Context context, KO key, VO value) throws IOException, InterruptedException {
        context.write(key, decorate(value));
    }

    public void write(Reducer<KI, VI, KO, VO>.Context context, KO key, VO value) throws IOException, InterruptedException {
        context.write(key, decorate(value));
    }



    public static abstract class FixedTextValueDecorator<KI, VI, KO> extends AbstractFixedValueDecorator<KI, VI, KO, Text> {

        public FixedTextValueDecorator(Text fixedValue) {
            super(fixedValue);
        }

        public FixedTextValueDecorator(String fixedValue) {
            super(new Text(fixedValue));
        }
    }

}
