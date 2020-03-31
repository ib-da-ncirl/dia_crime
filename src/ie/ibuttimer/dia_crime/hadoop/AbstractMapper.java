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
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class AbstractMapper<KI, VI, KO, VO> extends Mapper<KI, VI, KO, VO> {

    public static final String CLASS_VAL_SEPARATOR = ":-";

    private IValueDecorator<VO> decorator;

    private PropertyWrangler propertyWrangler;

    public AbstractMapper() {
        this(null, null);
    }

    public AbstractMapper(IValueDecorator<VO> decorator, String propertyRoot) {
        this.decorator = decorator;
        setPropertyRoot(propertyRoot);
    }

    public void setDecorator(IValueDecorator<VO> decorator) {
        this.decorator = decorator;
    }

    public VO decorate(VO value) {
        VO decorated;
        if (decorator != null) {
            decorated = decorator.decorate(value);
        } else {
            decorated = value;
        }
        return decorated;
    }

    public void setPropertyRoot(String propertyRoot) {
        this.propertyWrangler = new PropertyWrangler(propertyRoot);
    }

    public void write(Context context, KO key, VO value) throws IOException, InterruptedException {
        context.write(key, decorate(value));
    }

    protected String getPropertyPath(String propertyName) {
        return propertyWrangler.getPropertyPath(propertyName);
    }

    public String getPropertyName(String propertyPath) {
        return propertyWrangler.getPropertyName(propertyPath);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // set the property root path
        setPropertyRoot(getEntryMapperCfg().getPropertyRoot());
    }

    protected abstract ICsvEntryMapperCfg getEntryMapperCfg();

}
