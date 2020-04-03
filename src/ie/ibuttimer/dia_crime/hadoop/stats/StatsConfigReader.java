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

import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import ie.ibuttimer.dia_crime.misc.ConfigReader;
import org.apache.hadoop.conf.Configuration;

import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

public class StatsConfigReader extends ConfigReader {

    private static List<Class<?>> CLASSES = List.of(Integer.class, Long.class, Float.class, Double.class, String.class,
        LocalDate.class);

    private ConfigReader cfgReader;

    private Map<String, Class<?>> outputTypes;

    private List<String> variables;

    private WeakReference<Configuration> confRef;


    public StatsConfigReader(ICsvEntryMapperCfg mapperCfg) {
        super(mapperCfg);
        this.cfgReader = new ConfigReader(mapperCfg);
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

    public List<String> readVariables(Configuration conf) {
        Configuration confLocal = getConf(conf);

        String setting = cfgReader.getConfigProperty(confLocal, VARIABLES_PROP);
        if (setting.equals(VARIABLES_NUMERIC)) {

            // not the most efficient way of handing 'numeric' but ...
            outputTypes = readOutputTypes(confLocal, true, null);

            variables = new ArrayList<>(outputTypes.keySet());
        } else {
            variables = cfgReader.readCommaSeparatedProperty(getConf(conf), VARIABLES_PROP);
            if (variables.size() == 0) {
                throw new IllegalStateException("No variables specified");
            }
        }
        return variables;
    }

    public Map<String, Class<?>> readOutputTypes(Configuration conf) {
        Configuration confLocal = getConf(conf);

        if (Objects.isNull(variables)) {
            readVariables(confLocal);
        }

        outputTypes = readOutputTypes(confLocal, false, variables);

        return outputTypes;
    }

    private Map<String, Class<?>> readOutputTypes(Configuration conf, boolean allNumeric, List<String> vars) {
        outputTypes = new HashMap<>();

        Predicate<? super Map.Entry<String, Class<?>>> predicate = e -> {
            boolean required;
            if (allNumeric) {
                required = isNumericClass(e.getValue());
            } else {
                required = vars.stream().anyMatch(e.getKey()::equals);
            }
            return required;
        };
        cfgReader.readOutputTypeClasses(conf, OUTPUTTYPES_PATH_PROP, CLASSES).entrySet().stream()
            .filter(predicate)
            .forEach(entry -> {
                outputTypes.put(entry.getKey(), entry.getValue());
            });

        return outputTypes;
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

    private boolean isNumericClass(Class<?> cls) {
        return (cls.equals(Integer.class) || cls.equals(Long.class) || cls.equals(Float.class) ||
            cls.equals(Double.class) || cls.equals(BigInteger.class) || cls.equals(BigDecimal.class));
    }

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
