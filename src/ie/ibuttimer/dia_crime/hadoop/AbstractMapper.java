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

import ie.ibuttimer.dia_crime.hadoop.merge.IDecorator;
import ie.ibuttimer.dia_crime.hadoop.misc.Counters;
import ie.ibuttimer.dia_crime.misc.Constants;
import ie.ibuttimer.dia_crime.misc.DebugLevel;
import ie.ibuttimer.dia_crime.misc.PropertyWrangler;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class AbstractMapper<KI, VI, KO, VO> extends Mapper<KI, VI, KO, VO>
    implements IDecorator.IDecoratable<KO, VO>, DebugLevel.Debuggable {

    private static Logger logger = null;

    private IDecorator<KO, VO> decorator;
    private IDecorator.DecorMode decoratorMode;

    private PropertyWrangler propertyWrangler;

    private DebugLevel debugLevel;  // current debug level

    public AbstractMapper() {
        this(null, IDecorator.DecorMode.NONE, null);
    }

    public AbstractMapper(IDecorator<KO, VO> decorator, IDecorator.DecorMode decoratorMode, String propertyRoot) {
        setDecorator(decorator, decoratorMode);
        setPropertyRoot(propertyRoot);
        setDebugLevel(DebugLevel.OFF);
    }

    public static Logger getLogger() {
        if (logger == null) {
            setLogger(AbstractMapper.class);
        }
        return logger;
    }

    public static void setLogger(Class<?> cls) {
        setLogger(Logger.getLogger(cls));
    }

    public static void setLogger(Logger logger) {
        AbstractMapper.logger = logger;
    }

    @Override
    public void setDecorator(IDecorator<KO, VO> decorator, IDecorator.DecorMode decoratorMode) {
        this.decorator = decorator;
        this.decoratorMode = decoratorMode;
    }

    @Override
    public IDecorator.DecorMode getMode() {
        return decoratorMode;
    }

    @Override
    public IDecorator<KO, VO> getDecorator() {
        return decorator;
    }

    public void write(Context context, KO key, VO value) throws IOException, InterruptedException {
        Pair<KO, VO> decorated = decorate(key, value);
        context.write(decorated.getLeft(), decorated.getRight());

        if ((decoratorMode.hasTransform() && show(DebugLevel.VERBOSE))) {
            Pair<Object, Object> transformed = transform(decorated.getLeft(), decorated.getRight());
            getLogger().info(transformed.getLeft().toString() + " " + transformed.getRight().toString());
        }
    }

    protected Counters.MapperCounter getCounter(Mapper<?,?,?,?>.Context context, String group, String name) {
        return new Counters.MapperCounter(context, group, name);
    }

    protected Counters.MapperCounter getCounter(Mapper<?,?,?,?>.Context context, CountersEnum countersEnum) {
        return getCounter(context, countersEnum.getClass().getName(), countersEnum.toString());
    }

    public void setPropertyRoot(String propertyRoot) {
        this.propertyWrangler = new PropertyWrangler(propertyRoot);
    }

    public String getPropertyPath(String propertyName) {
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


    protected String getConfigProperty(Configuration conf, String name) {
        boolean required = getEntryMapperCfg().getRequiredProps().stream().anyMatch(p -> p.name.equals(name));
        String value = conf.get(getPropertyPath(name), "");
        if (required && TextUtils.isEmpty(value)) {
            throw new  IllegalStateException("Missing required configuration parameter: " + name);
        }
        return value;
    }


    public abstract ICsvEntryMapperCfg getEntryMapperCfg();


    /**
     * Check if the line is a comment line should be skipped
     * @param line  line
     * @return      True if line should be skipped
     */
    public boolean skipComment(Text line) {
        return line.toString().startsWith(Constants.COMMENT_PREFIX);
    }


    public DebugLevel getDebugLevel() {
        return debugLevel;
    }

    public void setDebugLevel(DebugLevel debugLevel) {
        this.debugLevel = debugLevel;
    }
}
