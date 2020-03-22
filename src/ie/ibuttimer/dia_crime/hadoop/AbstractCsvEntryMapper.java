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

import ie.ibuttimer.dia_crime.misc.Constants;
import ie.ibuttimer.dia_crime.misc.DateFilter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.shaded.org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Base mapper for a csv files:
 * - input key : csv file line number
 * - input value : csv file line text
 * @param <K>   output key type
 * @param <V>   output value type
 */
public abstract class AbstractCsvEntryMapper<K, V> extends Mapper<LongWritable, Text, K, V> {

    public static final String DEFAULT_SEPARATOR = ",";
    public static final boolean DEFAULT_HAS_HEADER = false;
    public static final String DEFAULT_DATE_TIME_FMT = DateTimeFormatter.ISO_LOCAL_DATE_TIME.toString();

    /** Separator for csv file */
    private String separator = DEFAULT_SEPARATOR;
    /** Csv file has a header line flag */
    private boolean hasHeader = DEFAULT_HAS_HEADER;
    /** java.time.format.DateTimeFormatter pattern for format of dates */
    private String dateTimeFmt = DEFAULT_DATE_TIME_FMT;
    /** Number of columns in csv file */
    private int numIndices = 0;

    private DateTimeFormatter dateTimeFormatter;

    public static final List<String> DATE_FILTER_PROPS = Arrays.asList(
        FILTER_START_DATE_PROP, FILTER_END_DATE_PROP
    );

    private DateFilter dateFilter = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        separator = conf.get(SEPARATOR_PROP, DEFAULT_SEPARATOR);
        hasHeader = conf.getBoolean(HAS_HEADER_PROP, DEFAULT_HAS_HEADER);
        dateTimeFmt = conf.get(DATE_FORMAT_PROP, DEFAULT_DATE_TIME_FMT);
        numIndices = conf.getInt(NUM_INDICES_PROP, 0);

        dateTimeFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern(dateTimeFmt)
            .toFormatter();

        dateFilter = new DateFilter(conf.get(FILTER_START_DATE_PROP, ""),
                conf.get(FILTER_END_DATE_PROP, ""));
    }

    /**
     * Get the separator for csv file
     * @return separator
     */
    public String getSeparator() {
        return separator;
    }

    /**
     * Get the csv file has a header line flag
     * @return header line flag
     */
    public boolean isHasHeader() {
        return hasHeader;
    }

    /**
     * Get the number of columns in csv file
     * @return number of columns
     */
    public int getNumIndices() {
        return numIndices;
    }


    /**
     * Check if the specified key is a header line and if it should be skipped
     * @param key   Key; line number
     * @return      True if line should be skipped
     */
    public boolean skipHeader(LongWritable key) {
        boolean skip = false;
        if (key.get() == 0) {
            skip = isHasHeader();
        }
        return skip;
    }

    /**
     * Get the DateTimeFormatter pattern for format of dates
     * @return DateTimeFormatter
     */
    public DateTimeFormatter getDateTimeFormatter() {
        return dateTimeFormatter;
    }

    /**
     * Get the date and time
     * @param dateTime  String of the date and time
     * @return  Converted date and time
     */
    public LocalDateTime getDateTime(String dateTime) {
        LocalDateTime ldt = LocalDateTime.MIN;
        try {
            ldt = LocalDateTime.parse(dateTime, getDateTimeFormatter());
        } catch (DateTimeParseException dpte) {
            getLogger().error("Cannot parse '" + dateTime + "' using format " + dateTimeFormatter.toString(), dpte);
        }
        return ldt;
    }

    /**
     * Get the date
     * @param date  String of the date and time
     * @return  Converted date and time
     */
    public LocalDate getDate(String date) {
        LocalDate ld = LocalDate.MIN;
        try {
            ld = LocalDate.parse(date, getDateTimeFormatter());
        } catch (DateTimeParseException dpte) {
            getLogger().error("Cannot parse '" + date + "' using format " + dateTimeFormatter.toString(), dpte);
        }
        return ld;
    }

    /**
     * Get the date and time and check if it is filtered
     * @param dateTime  String of the date and time
     * @return  Pair of filter result (TRUE if passes filter) and converted date and time
     */
    public Pair<Boolean, LocalDateTime> getDateTimeAndFilter(String dateTime) {
        LocalDateTime ldt = getDateTime(dateTime);
        return Pair.of(dateFilter.filter(ldt), ldt);
    }

    /**
     * Get the date and check if it is filtered
     * @param date  String of the date and time
     * @return  Pair of filter result (TRUE if passes filter) and converted date and time
     */
    public Pair<Boolean, LocalDate> getDateAndFilter(String date) {
        LocalDate ld = getDate(date);
        return Pair.of(dateFilter.filter(ld), ld);
    }

    /**
     * Get the logger for the class
     * @return  Logger object
     */
    protected abstract Logger getLogger();

    public abstract static class AbstractCsvEntryMapperCfg implements ICsvEntryMapperCfg {
        @Override
        public HashMap<String, String> getPropertyDefaults() {
            // create map of possible keys and default values
            HashMap<String, String> propDefault = new HashMap<>();
            for (String key : ALL_PATH_PROP_LIST) {
                propDefault.put(key, "");
            }
            propDefault.put(STOCK_TAG_PROP, "");
            propDefault.put(SEPARATOR_PROP, DEFAULT_SEPARATOR);
            propDefault.put(HAS_HEADER_PROP, Boolean.toString(DEFAULT_HAS_HEADER));
            propDefault.put(DATE_FORMAT_PROP, DEFAULT_DATE_TIME_FMT);
            propDefault.put(NUM_INDICES_PROP, "0");
            for (String key : DATE_FILTER_PROPS) {
                propDefault.put(key, "");
            }

            for (String key : getPropertyIndices()) {
                propDefault.put(key, "-1");
            }

            return propDefault;
        }

        @Override
        public Pair<Integer, List<String>> checkConfiguration(Configuration conf) {
            int resultCode = ECODE_SUCCESS;
            List<String> errors = new ArrayList<>();

            // check for tag & input/output path config
            Pair<String, String>[] props = new Pair[] {
                Pair.of(STOCK_TAG_PROP, "stack tag"),
                Pair.of(IN_PATH_PROP, "input path"),
                Pair.of(OUT_PATH_PROP, "output path")
            };
            for (Pair<String, String> pair : props) {
                String prop = pair.getLeft();
                if (TextUtils.isEmpty(conf.get(prop))) {
                    errors.add("Error: No " + pair.getRight() + " specified, set '" + prop + "'.");
                    resultCode = ECODE_CONFIG_ERROR;
                }
            }

            // check for date filtering
            LocalDate startDate = null;
            LocalDate endDate = null;
            for (String key : DATE_FILTER_PROPS) {
                String dateStr = conf.get(key, "");
                if (!TextUtils.isEmpty(dateStr)) {
                    try {
                        LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
                        if (key.equals(FILTER_START_DATE_PROP)) {
                            startDate = date;
                        } else {
                            endDate = date;
                        }
                    } catch (DateTimeParseException dpte) {
                        errors.add("Error: Invalid '" + key + "' specified, '" + dateStr + "'");
                        resultCode = ECODE_CONFIG_ERROR;
                    }
                }
            }
            if (resultCode != Constants.ECODE_CONFIG_ERROR) {
                if ((startDate != null) && (endDate != null)) {
                    Period period = Period.between(startDate, endDate.plusDays(1)); // start inclusive, end exclusive
                    if (period.isZero() || period.isNegative()) {
                        errors.add("Error: '" + FILTER_END_DATE_PROP + "' before '" + FILTER_START_DATE_PROP + "'");
                        resultCode = ECODE_CONFIG_ERROR;
                    }
                }
            }

            // check property indices
            for (String key : getPropertyIndices()) {
                if (conf.getInt(key, -1) < 0) {
                    errors.add("Error: '" + key + "' not specified.");
                    resultCode = ECODE_CONFIG_ERROR;
                }
            }

            return Pair.of(resultCode, errors);
        }
    };
}



