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
import ie.ibuttimer.dia_crime.hadoop.io.FileUtil;
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
import java.util.stream.Collectors;

/**
 * Hadoop configuration reader
 */
public class ConfigReader {

    private ICsvMapperCfg mapperCfg;
    private PropertyWrangler propertyWrangler;

    public ConfigReader(ICsvMapperCfg mapperCfg) {
        this.mapperCfg = mapperCfg;
        this.propertyWrangler = new PropertyWrangler(mapperCfg.getPropertyRoot());
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

    public String getPropertyPath(String propertyName) {
        return propertyWrangler.getPropertyPath(propertyName);
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

    public Map<String, String> readCommaSeparatedKeyColonValueProperty(Configuration conf, String name) {
        return readSeparatedKeyValueProperty(conf, name, ",", ":");
    }

    public Map<String, Class<?>> readOutputTypeClasses(Configuration conf, String property, List<Class<?>> classes) {
        Map<String, Class<?>> outputTypes = new HashMap<>();
        MapStringifier.ElementStringify commaStringify = new MapStringifier.ElementStringify(",");
        String typesPath = getConfigProperty(conf, property);

        Map<String, String> entries = new HashMap<>();
        FileUtil fileUtil = new FileUtil(new Path(typesPath), conf);

        try (FSDataInputStream stream = fileUtil.fileReadOpen();
             InputStreamReader inputStream = new InputStreamReader(stream, Charsets.UTF_8);
             BufferedReader reader = new BufferedReader(inputStream)) {

            reader.lines()
                .map(commaStringify::destringifyElement)
                .filter(p -> !TextUtils.isEmpty(p.getLeft()) && !TextUtils.isEmpty(p.getRight()))
                .forEach(p -> entries.put(p.getLeft(), p.getRight()));

        } catch (IOException e) {
            e.printStackTrace();
        }

        entries.forEach((key, name) -> {
            classes.stream()
                .filter(cls -> name.equals(cls.getSimpleName()))
                .findFirst()
                .ifPresent(cls -> outputTypes.put(key, cls));
        });
        if (outputTypes.size() != entries.size()) {
            throw new IllegalStateException("Unmatched output type class");
        }
        return outputTypes;
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
