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

import ie.ibuttimer.dia_crime.hadoop.stock.StockEntryWritable;
import org.apache.hadoop.io.Writable;
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

/**
 * Base class for customer Writables
 */
public abstract class AbstractBaseWritable implements Writable {

    private LocalDateTime localDateTime;

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

    public <W extends AbstractBaseWritable> void set(W other) {
        this.localDateTime = ((AbstractBaseWritable)other).localDateTime;
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
    public abstract static class AbstractBaseWritableBuilder<B, W extends AbstractBaseWritable>
                                    implements IBaseWritableBuilder<B, W> {

        private Logger logger;

        private W entry;

        public AbstractBaseWritableBuilder(Logger logger) {
            this.logger = logger;
            clear();
        }

        @Override
        public B clear() {
            entry = getNewWritable();
            return getThis();
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
            try {
                value = Double.parseDouble(text);
            } catch (NumberFormatException nfe) {
                logger.error("Unable to parse " + text + " as double", nfe);
            }
            return value;
        }

        protected int getInt(String text) {
            int value = 0;
            try {
                value = Integer.parseInt(text);
            } catch (NumberFormatException nfe) {
                logger.error("Unable to parse " + text + " as int", nfe);
            }
            return value;
        }

        protected long getLong(String text) {
            long value = 0;
            try {
                value = Long.parseLong(text);
            } catch (NumberFormatException nfe) {
                logger.error("Unable to parse " + text + " as long", nfe);
            }
            return value;
        }

        protected BigDecimal getBigDecimal(String text) {
            BigDecimal value = BigDecimal.ZERO;
            try {
                value = new BigDecimal(text);
            } catch (NumberFormatException nfe) {
                logger.error("Unable to parse " + text + " as BigDecimal", nfe);
            }
            return value;
        }

        protected BigInteger getBigInteger(String text) {
            BigInteger value = BigInteger.ZERO;
            try {
                value = new BigInteger(text);
            } catch (NumberFormatException nfe) {
                logger.error("Unable to parse " + text + " as BigInteger", nfe);
            }
            return value;
        }
    }

    public interface IBaseWritableBuilder<B, W extends AbstractBaseWritable> {

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
