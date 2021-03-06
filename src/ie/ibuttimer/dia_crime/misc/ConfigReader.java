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

package ie.ibuttimer.dia_crime.misc;

import com.google.common.base.Charsets;
import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.hadoop.crime.IOutputType;
import ie.ibuttimer.dia_crime.hadoop.io.FileUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.http.util.TextUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.MapStringifier.ElementStringify.HADOOP_KEY_VAL;

/**
 * Hadoop configuration reader
 */
public class ConfigReader implements IPropertyWrangler {

    private ICsvMapperCfg mapperCfg;
    private final PropertyWrangler propertyWrangler;

    public ConfigReader(ICsvMapperCfg mapperCfg) {
        this.mapperCfg = mapperCfg;
        this.propertyWrangler = new PropertyWrangler(mapperCfg.getRoot());
    }

    public ConfigReader(String section) {
        this.mapperCfg = null;
        this.propertyWrangler = new PropertyWrangler(section);
    }

    public void setSection(String section) {
        this.mapperCfg = null;
        this.propertyWrangler.setRoot(section);
    }

    public String getConfigProperty(Configuration conf, String name, String dfltValue) {
        boolean required = false;
        if (mapperCfg != null) {
            required = mapperCfg.getRequiredProps().stream().anyMatch(p -> p.getName().equals(name));
        }
        String value = conf.get(propertyWrangler.getPropertyPath(name), dfltValue);
        if (required && TextUtils.isEmpty(value)) {
            throw new IllegalStateException("Missing required configuration parameter: " + name);
        }
        return value;
    }

    public String getConfigProperty(Configuration conf, String name) {
        return getConfigProperty(conf, name, "");
    }

    public Number getConfigProperty(Configuration conf, String name, Number dfltValue) {
        AtomicReference<Number> value = new AtomicReference<>(dfltValue);
        String setting = getConfigProperty(conf, name, "");

        if (!TextUtils.isEmpty(setting)) {
            Value.of(setting, dfltValue.getClass()).ifPresent(v -> value.set((Number)v));
        }
        return value.get();
    }

    @Override
    public String getPropertyPath(String propertyName) {
        return propertyWrangler.getPropertyPath(propertyName);
    }

    @Override
    public String getPropertyName(String propertyPath) {
        return propertyWrangler.getPropertyName(propertyPath);
    }

    @Override
    public String getRoot() {
        return propertyWrangler.getRoot();
    }

    public List<String> readSeparatedProperty(Configuration conf, String name, String separator) {
        String setting = getConfigProperty(conf, name);
        return Arrays.stream(setting.split(separator))
            .map(String::trim)
            .collect(Collectors.toList());
    }

    public List<String> readCommaSeparatedProperty(Configuration conf, String name) {
        return readSeparatedProperty(conf, name, ",");
    }

    /**
     * Read a separated key/value property such as 'key1:value1,key2:value2'
     * @param conf          Current configuration
     * @param name          Property name
     * @param separator     Term separator
     * @param kvSeparator   Key/value separator
     * @return  Map of keys & values
     */
    public Map<String, String> readSeparatedKeyValueProperty(Configuration conf, String name, String separator, String kvSeparator) {
        Map<String, String> map = new HashMap<>();
        MapStringifier.ElementStringify kvSplitter = new MapStringifier.ElementStringify(kvSeparator);
        List<String> list = readSeparatedProperty(conf, name, separator);

        list.stream()
            .map(kvSplitter::destringifyElement)
            .forEach((pair -> {
                String key = pair.getLeft();
                String value = pair.getRight();
                if ((key != null) && (value != null)) {
                    map.put(key, value);
                }
            }));

        return map;
    }

    /**
     * Read a separated key/value property such as 'key1:value1,key2:value2'
     * @param conf          Current configuration
     * @param name          Property name
     * @return  Map of keys & values
     */
    public Map<String, String> readCommaSeparatedKeyColonValueProperty(Configuration conf, String name) {
        return readSeparatedKeyValueProperty(conf, name, ",", ":");
    }

    /**
     * Read a list of type classes from a Hadoop output file
     * e.g. '#	01A,Double,crime'
     * @param conf
     * @param property  Property containing path to file to read
     * @param classes
     * @return
     */
    public Map<String, IOutputType.OpTypeEntry> readOutputTypeClasses(Configuration conf, String property, List<Class<?>> classes) {
        String typesPath = getConfigProperty(conf, property);

        Map<String, Pair<String, String>> entries = new HashMap<>();
        FileUtil fileUtil = new FileUtil(new Path(typesPath), conf);

        try (FSDataInputStream stream = fileUtil.fileReadOpen();
             InputStreamReader inputStream = new InputStreamReader(stream, Charsets.UTF_8);
             BufferedReader reader = new BufferedReader(inputStream)) {

            reader.lines()
                .filter(l -> !TextUtils.isEmpty(l))
                .map(l -> HADOOP_KEY_VAL.destringifyElement(l).getRight())
                .map(l -> l.split(","))
                .forEach(p -> entries.put(p[0], Pair.of(p[1], p[2])));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return convertOutputTypeClasses(entries, classes);
    }

    public Map<String, IOutputType.OpTypeEntry> convertOutputTypeClasses(Map<String, Pair<String, String>> map, List<Class<?>> classes) {
        Map<String, IOutputType.OpTypeEntry> outputTypes = new TreeMap<>();

        map.forEach((key, pair) -> {
            classes.stream()
                .filter(cls -> pair.getLeft().equals(cls.getSimpleName()))
                .findFirst()
                .ifPresent(cls -> outputTypes.put(key, IOutputType.OpTypeEntry.of(cls, pair.getRight())));
        });
        if (outputTypes.size() != map.size()) {
            throw new IllegalStateException("Unmatched output type class");
        }
        return outputTypes;
    }

    /**
     * Read a list of comma separated values from a Hadoop output file
     * e.g. '#	300,0'
     * @param conf
     * @param property  Property containing path to file to read
     * @return
     */
    public List<List<String>> readCommaSeparatedFile(Configuration conf, String property) {
        String filePath = getConfigProperty(conf, property);

        List<List<String>> entries = new ArrayList<>();
        FileUtil fileUtil = new FileUtil(new Path(filePath), conf);

        try (FSDataInputStream stream = fileUtil.fileReadOpen();
             InputStreamReader inputStream = new InputStreamReader(stream, Charsets.UTF_8);
             BufferedReader reader = new BufferedReader(inputStream)) {

            entries = reader.lines()
                .filter(l -> !TextUtils.isEmpty(l))
                .map(l -> HADOOP_KEY_VAL.destringifyElement(l).getRight())
                .map(l -> List.of(l.split(",")))
                .collect(Collectors.toList());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return entries;
    }


    /**
     * Read a date time formatter property
     * @param conf
     * @param property
     * @param dflt      Default, if property not specified
     * @return
     */
    public DateTimeFormatter getDateTimeFormatter(Configuration conf, String property, DateTimeFormatter dflt) {
        DateTimeFormatter formatter;
        String dateTimeFmt = conf.get(getPropertyPath(property), "");
        if (TextUtils.isEmpty(dateTimeFmt)) {
            formatter = dflt;
        } else {
            formatter = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .appendPattern(dateTimeFmt)
                .toFormatter();
        }
        return formatter;
    }
}
