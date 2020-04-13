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

package ie.ibuttimer.dia_crime.hadoop.stats;

import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import org.apache.hadoop.conf.Configuration;

import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Predicate;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Statistics specific configuration reader
 */
public class StatsConfigReader extends ConfigReader {

    private static List<Class<?>> CLASSES = List.of(Integer.class, Long.class, Float.class, Double.class, String.class,
        LocalDate.class);

    private Map<String, Class<?>> outputTypes;

    private List<String> variables;
    private List<String> debugMapperToFile;

    private WeakReference<Configuration> confRef;


    public StatsConfigReader(ICsvMapperCfg mapperCfg) {
        super(mapperCfg);
    }

    public StatsConfigReader(String section) {
        super(section);
    }

    private Configuration getConf(Configuration conf) {
        boolean save = true;
        if (Objects.nonNull(confRef)) {
            save = Objects.requireNonNull(confRef.get()).equals(conf);
        }
        if (save) {
            confRef = new WeakReference<>(conf);
            outputTypes = null;
            variables = null;
        }
        return conf;
    }

    /**
     * Read list of variable to perform statistics for
     * @param conf
     * @return
     */
    public List<String> readVariables(Configuration conf) {
        Configuration confLocal = getConf(conf);

        String setting = getConfigProperty(confLocal, VARIABLES_PROP);
        if (setting.equals(VARIABLES_NUMERIC)) {

            // not the most efficient way of handing 'numeric' but ...
            outputTypes = readOutputTypes(confLocal, OutputType.NUMERIC, null);

            variables = new ArrayList<>(outputTypes.keySet());
        } else if (setting.equals(VARIABLES_ALL)) {

            // not the most efficient way of handing 'all' but ...
            outputTypes = readOutputTypes(confLocal, OutputType.ALL, null);

            variables = new ArrayList<>(outputTypes.keySet());
        } else {
            variables = readCommaSeparatedProperty(confLocal, VARIABLES_PROP);
            if (variables.size() == 0) {
                throw new IllegalStateException("No variables specified");
            }
        }
        return variables;
    }

    /**
     * Read the output types configuration
     * @param conf
     * @return
     */
    public Map<String, Class<?>> readOutputTypes(Configuration conf) {
        Configuration confLocal = getConf(conf);

        if (Objects.isNull(variables)) {
            readVariables(confLocal);
        }

        outputTypes = readOutputTypes(confLocal, OutputType.CFG, variables);

        return outputTypes;
    }

    enum OutputType { CFG, NUMERIC, ALL }

    /**
     * Read the output types configuration
     * @param conf
     * @param type
     * @param vars
     * @return
     */
    private Map<String, Class<?>> readOutputTypes(Configuration conf, OutputType type, List<String> vars) {
        outputTypes = new HashMap<>();

        Predicate<? super Map.Entry<String, Class<?>>> predicate;
        switch (type) {
            case CFG:       predicate = e -> vars.stream().anyMatch(e.getKey()::equals);    break;
            case NUMERIC:   predicate = e -> isNumericClass(e.getValue());                  break;
            default:        predicate = e -> true;                                          break;
        }
        readOutputTypeClasses(conf, OUTPUTTYPES_PATH_PROP, CLASSES).entrySet().stream()
            .filter(predicate)
            .forEach(entry -> {
                outputTypes.put(entry.getKey(), entry.getValue());
            });

        return outputTypes;
    }

    public Map<String, Class<?>> convertOutputTypeClasses(Map<String, String> map) {
        return convertOutputTypeClasses(map, CLASSES);
    }

    public Map<String, Class<?>> getNumericOutputTypes(Configuration conf) {
        Configuration confLocal = getConf(conf);

        if (Objects.isNull(outputTypes)) {
            readOutputTypes(confLocal);
        }

        Map<String, Class<?>> numericTypes = new HashMap<>();

        outputTypes.forEach((name, cls) -> {
            if (isNumericClass(cls)) {
                numericTypes.put(name, cls);
            }
        });

        return numericTypes;
    }

    /**
     * Numeric class test
     * @param cls
     * @return
     */
    private boolean isNumericClass(Class<?> cls) {
        return (cls.equals(Integer.class) || cls.equals(Long.class) || cls.equals(Float.class) ||
            cls.equals(Double.class) || cls.equals(BigInteger.class) || cls.equals(BigDecimal.class));
    }

    /**
     * Get only numeric property field names from configuration
     * @param conf
     * @return
     */
    public List<String> getNumericFields(Configuration conf) {
        Configuration confLocal = getConf(conf);

        if (Objects.isNull(outputTypes)) {
            readOutputTypes(confLocal);
        }

        List<String> numericTypes = new ArrayList<>();

        outputTypes.forEach((name, cls) -> {
            if (isNumericClass(cls)) {
                numericTypes.add(name);
            }
        });

        return numericTypes;
    }
}
