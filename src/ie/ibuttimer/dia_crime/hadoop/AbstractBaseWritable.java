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

import com.google.common.util.concurrent.AtomicDouble;
import ie.ibuttimer.dia_crime.hadoop.stats.IStatOps;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.io.Writable;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static ie.ibuttimer.dia_crime.misc.Constants.DATE_PROP;

/**
 * Base class for custom Writables
 */
public abstract class AbstractBaseWritable<W extends AbstractBaseWritable<?>> implements Writable, IStatOps<W> {

    private LocalDateTime localDateTime;

    public static List<String> FIELDS = Collections.singletonList(DATE_PROP);

    // Default constructor to allow (de)serialization
    public AbstractBaseWritable() {
        this.localDateTime = LocalDateTime.MIN;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(localDateTime.toEpochSecond(ZoneOffset.UTC));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.localDateTime = LocalDateTime.ofEpochSecond(dataInput.readLong(), 0, ZoneOffset.UTC);
    }

    public static <W extends AbstractBaseWritable<?>> void writeNullable(DataOutput dataOutput, W toWrite) throws IOException {
        boolean notNull = (toWrite != null);
        dataOutput.writeBoolean(notNull);
        if (notNull) {
            toWrite.write(dataOutput);
        }
    }

    public static <W extends AbstractBaseWritable<?>> W readNullable(DataInput dataInput, W newInstance) throws IOException {
        boolean notNull = dataInput.readBoolean();
        W read = null;
        if (notNull) {
            newInstance.readFields(dataInput);
            read = newInstance;
        }
        return read;
    }

    public LocalDateTime getLocalDateTime() {
        return localDateTime;
    }

    public LocalDate getLocalDate() {
        return localDateTime.toLocalDate();
    }

    public LocalTime getLocalTime() {
        return localDateTime.toLocalTime();
    }

    public void setLocalDateTime(LocalDateTime localDateTime) {
        this.localDateTime = localDateTime;
    }

    public void setLocalDate(LocalDate localDate) {
        this.localDateTime = LocalDateTime.of(localDate, LocalTime.MIN);
    }

    @Override
    public void set(W other) {
        this.localDateTime = ((AbstractBaseWritable<?>)other).localDateTime;
    }

    public Map<String, Object> toMap() {
        return addToMap(new HashMap<>());
    }

    public Map<String, Object> addToMap(Map<String, Object> map) {
        getFieldsList().forEach(p -> {
            getField(p).ifPresent(v -> map.put(p, v.value()));
        });
        return map;
    }

    public W fromMap(Map<String, Object> map) {
        W obj = getInstance();
        List<String> fields = getFieldsList();
        map.forEach((key, value) -> {
            if (fields.contains(key)) {
                obj.setField(key, value);
            }
        });
        return obj;
    }

    public abstract W getInstance();

    public List<String> getFieldsList() {
        return FIELDS;
    }

    public Optional<Value> getField(String field) {
        Optional<Value> value;
        if (DATE_PROP.equals(field)) {
            value = Value.ofOptional(getLocalDate());
        } else {
            value = Value.empty();
        }
        return value;
    }

    public boolean setField(String field, Object value) {
        boolean set = true;
        switch (field) {
            case DATE_PROP:
                if (Value.isLocalDateTime(value)) {
                    setLocalDateTime((LocalDateTime)value);
                } else if (value instanceof LocalDate) {
                    setLocalDate((LocalDate)value);
                } else {
                    set = false;
                }
                break;
            default:
                set = false;
                break;
        }
        return set;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            "localDateTime=" + localDateTime +
            '}';
    }


    /**
     * Base class for custom Writable builders
     * @param <B>   The builder class
     * @param <W>   The Writable class
     */
    public abstract static class AbstractBaseWritableBuilder<B, W extends AbstractBaseWritable<?>>
                                    implements IBaseWritableBuilder<B, W> {

        private Logger logger;

        private Map<String, Double> factors;

        private W entry;

        public AbstractBaseWritableBuilder(Logger logger) {
            this(logger, new HashMap<>());
        }

        public AbstractBaseWritableBuilder(Logger logger, Map<String, Double> factors) {
            this.logger = logger;
            setFactors(factors);
            clear();
        }

        public void setFactors(Map<String, Double> factors) {
            this.factors = factors;
        }

        @Override
        public B clear() {
            entry = getNewWritable();
            return getThis();
        }

        public Optional<Double> getFactor(String property) {
            if (factors.containsKey(property)) {
                return Optional.of(factors.get(property));
            } else {
                return Optional.empty();
            }
        }

        @Override
        public B setLocalDateTime(String localDateTime, DateTimeFormatter formatter) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(localDateTime, formatter);
                setLocalDateTime(ldt);
            } catch (DateTimeParseException dpte) {
                logger.error("Cannot parse '" + localDateTime + "' using format " + formatter.toString(), dpte);
            }
            return getThis();
        }

        @Override
        public B setLocalDateTime(LocalDateTime localDateTime) {
            entry.setLocalDateTime(localDateTime);
            return getThis();
        }

        @Override
        public B setLocalDate(LocalDate localDate) {
            entry.setLocalDate(localDate);
            return getThis();
        }

        @Override
        public W getWritable() {
            return entry;
        }

        protected double getDouble(String text) {
            double value = 0;
            if (!TextUtils.isEmpty(text)) {
                try {
                    value = Double.parseDouble(text);
                } catch (NumberFormatException nfe) {
                    logger.error("Unable to parse " + text + " as double", nfe);
                }
            }
            return value;
        }

        protected double getDouble(String text, String factor) {
            AtomicDouble value = new AtomicDouble(getDouble(text));
            getFactor(factor).ifPresent(f -> {
                value.set(value.get() / f);
            });
            return value.get();
        }

        protected float getFloat(String text) {
            float value = 0;
            if (!TextUtils.isEmpty(text)) {
                try {
                    value = Float.parseFloat(text);
                } catch (NumberFormatException nfe) {
                    logger.error("Unable to parse " + text + " as float", nfe);
                }
            }
            return value;
        }

        protected int getInt(String text) {
            int value = 0;
            if (!TextUtils.isEmpty(text)) {
                try {
                    value = Integer.parseInt(text);
                } catch (NumberFormatException nfe) {
                    logger.error("Unable to parse " + text + " as int", nfe);
                }
            }
            return value;
        }

        protected long getLong(String text) {
            long value = 0;
            if (!TextUtils.isEmpty(text)) {
                try {
                    value = Long.parseLong(text);
                } catch (NumberFormatException nfe) {
                    logger.error("Unable to parse " + text + " as long", nfe);
                }
            }
            return value;
        }

        protected long getLong(String text, String factor) {
            AtomicLong value = new AtomicLong(getLong(text));
            getFactor(factor).ifPresent(f -> {
                value.set((long)(value.get() / f));
            });
            return value.get();
        }

        protected BigDecimal getBigDecimal(String text) {
            BigDecimal value = BigDecimal.ZERO;
            if (!TextUtils.isEmpty(text)) {
                try {
                    value = new BigDecimal(text);
                } catch (NumberFormatException nfe) {
                    logger.error("Unable to parse " + text + " as BigDecimal", nfe);
                }
            }
            return value;
        }

        protected BigInteger getBigInteger(String text) {
            BigInteger value = BigInteger.ZERO;
            if (!TextUtils.isEmpty(text)) {
                try {
                    value = new BigInteger(text);
                } catch (NumberFormatException nfe) {
                    logger.error("Unable to parse " + text + " as BigInteger", nfe);
                }
            }
            return value;
        }
    }

    /**
     * Base writable builder interface
     * @param <B>   Builder class
     * @param <W>   Writable class
     */
    public interface IBaseWritableBuilder<B, W extends AbstractBaseWritable<?>> {

        B clear();

        B setLocalDateTime(String localDateTime, DateTimeFormatter formatter);

        B setLocalDateTime(LocalDateTime localDateTime);

        B setLocalDate(LocalDate localDate);

        B getThis();

        W getNewWritable();

        W getWritable();

        W build();
    }

}
