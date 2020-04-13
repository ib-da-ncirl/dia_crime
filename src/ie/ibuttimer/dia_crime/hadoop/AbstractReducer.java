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
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import ie.ibuttimer.dia_crime.misc.DebugLevel;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Base class for Reducer's
 * @param <KI>  Reducer key input class
 * @param <VI>  Reducer values input class
 * @param <KO>  Reducer key output class
 * @param <VO>  Reducer values output class
 */
public abstract class AbstractReducer<KI, VI, KO, VO> extends Reducer<KI, VI, KO, VO>
    implements IDecorator.IDecoratable<KO, VO>, ITagger, DebugLevel.Debuggable {

    private IDecorator<KO, VO> decorator;
    private IDecorator.DecorMode decoratorMode;

    private Logger logger = null;

    private DebugLevel debugLevel;  // current debug level

    private String section = null;

    public AbstractReducer() {
        this(null, IDecorator.DecorMode.NONE);
    }

    public AbstractReducer(IDecorator<KO, VO> decorator, IDecorator.DecorMode decoratorMode) {
        setDecorator(decorator, decoratorMode);
        setDebugLevel(DebugLevel.OFF);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // set debug level
        Configuration conf = context.getConfiguration();
        String section = conf.get(CONF_PROPERTY_ROOT, "");
        if (!TextUtils.isEmpty(section)) {
            setDebugLevel(DebugLevel.getSetting(conf, section));
        }
    }

    public void setSection(String section) {
        this.section = section;
    }

    public String getSection() {
        return this.section;
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

    public Logger getLogger() {
        if (logger == null) {
            setLogger(AbstractReducer.class);
        }
        return logger;
    }

    public void setLogger(Class<?> cls) {
        setLogger(Logger.getLogger(cls));
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    protected Counters.ReducerCounter getCounter(Context context, String group, String name) {
        return new Counters.ReducerCounter(context, group, name);
    }

    protected Counters.ReducerCounter getCounter(Context context, CountersEnum countersEnum) {
        return getCounter(context, countersEnum.getClass().getName(), countersEnum.toString());
    }

    public void write(Context context, KO key, VO value) throws IOException, InterruptedException {
        Pair<KO, VO> decorated = decorate(key, value);
        context.write(decorated.getLeft(), decorated.getRight());

        if ((decoratorMode.hasTransform() && show(DebugLevel.VERBOSE))) {
            Pair<Object, Object> transformed = transform(decorated.getLeft(), decorated.getRight());
            getLogger().info(transformed.getLeft().toString() + " " + transformed.getRight().toString());
        }
    }

    public DebugLevel getDebugLevel() {
        return debugLevel;
    }

    public void setDebugLevel(DebugLevel debugLevel) {
        this.debugLevel = debugLevel;
    }


    /**
     * Add a tags header to the output
     * @param context           Current context
     * @param counter           Counter to test
     * @param additionalTags    Parameter tags to add values for
     * @param rawStrings        Raw text to add
     */
    protected void addOutputHeader(Context context, Counters.ReducerCounter counter, List<String> additionalTags,
                                   List<String> rawStrings) {
        Optional<Long> inCount = counter.getCount();
        inCount.ifPresent(count -> {
            if (count == 0) {
                Configuration conf = context.getConfiguration();
                ConfigReader cfgReader = new ConfigReader(getSection());
                List<String> tags = new ArrayList<>(getTagStrings(conf, getSection()));

                additionalTags.forEach(p -> tags.add(
                    String.format("%s : %s", p, cfgReader.getConfigProperty(conf, p, ""))));

                tags.addAll(rawStrings);

                tags.forEach(tagLine -> {
                    try {
                        context.write(newKey(COMMENT_PREFIX), newValue(tagLine));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }
        });
    }

    protected abstract KO newKey(String key);

    protected abstract VO newValue(String value);
}
