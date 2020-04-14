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

import ie.ibuttimer.dia_crime.hadoop.misc.DateWritable;
import ie.ibuttimer.dia_crime.misc.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.http.util.TextUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

import static ie.ibuttimer.dia_crime.misc.Constants.*;

/**
 * Base mapper for a csv file:
 * - input key : csv file line number
 * - input value : csv file line text
 * @param <K>   output key type
 * @param <V>   output value type
 */
public abstract class AbstractCsvMapper<K, V> extends AbstractMapper<LongWritable, Text, K, V> {

    public static final String DEFAULT_SEPARATOR = ",";
    public static final boolean DEFAULT_HAS_HEADER = false;

    /** Separator for csv file */
    private String separator = DEFAULT_SEPARATOR;
    /** Csv file has a header line flag */
    private boolean hasHeader = DEFAULT_HAS_HEADER;
    /** Number of columns in csv file */
    private int numIndices = 0;

    // date time formatter for reading input
    private DateTimeFormatter dateTimeFormatter;
    // date time formatter for outputting date keys
    private DateTimeFormatter keyOutDateTimeFormatter;

    public static final List<String> DATE_FILTER_PROPS = Arrays.asList(
        FILTER_START_DATE_PROP, FILTER_END_DATE_PROP
    );

    private DateFilter dateFilter = null;

    private final Map<String, Integer> indices = new HashMap<>();
    private int maxIndex = -1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        super.setup(context);

        // read basic properties
        separator = conf.get(getPropertyPath(SEPARATOR_PROP), DEFAULT_SEPARATOR);
        hasHeader = conf.getBoolean(getPropertyPath(HAS_HEADER_PROP), DEFAULT_HAS_HEADER);
        numIndices = conf.getInt(getPropertyPath(NUM_INDICES_PROP), 0);

        /* set date time formatter properties */
        ConfigReader cfgReader = new ConfigReader(getMapperCfg());
        dateTimeFormatter = cfgReader.getDateTimeFormatter(conf, DATE_FORMAT_PROP, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        keyOutDateTimeFormatter = cfgReader.getDateTimeFormatter(conf, OUT_KEY_DATE_FORMAT_PROP, DateTimeFormatter.ISO_LOCAL_DATE);

        // get date filter
        dateFilter = new DateFilter(conf.get(getPropertyPath(FILTER_START_DATE_PROP), ""),
                conf.get(getPropertyPath(FILTER_END_DATE_PROP), ""));

        setDebugLevel(DebugLevel.getSetting(conf, getMapperCfg()));

        if (show(DebugLevel.MEDIUM)) {
            getMapperCfg().dumpConfiguration(getLogger(), conf);
        }
    }

    /**
     * Initialise property indices
     * @param context
     * @param propertyIndices
     */
    protected void initIndices(Context context, List<String> propertyIndices) {
        Configuration conf = context.getConfiguration();

        // read the element indices from the configuration
        for (String prop : propertyIndices) {
            int index = conf.getInt(getPropertyPath(prop), -1);
            if (index > maxIndex) {
                maxIndex = index;
            }
            indices.put(prop, index);
        }
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


    public Map<String, Integer> getIndices() {
        return indices;
    }

    public int getMaxIndex() {
        return maxIndex;
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
     * Check if the specified key is a header line or comment line and if it should be skipped
     * @param key   Key; line number
     * @param value Line
     * @return      True if line should be skipped
     */
    public boolean skip(LongWritable key, Text value) {
        return skipHeader(key) || skipComment(value);
    }

    /**
     * Get the DateTimeFormatter pattern for format of dates
     * @return DateTimeFormatter
     */
    public DateTimeFormatter getDateTimeFormatter() {
        return dateTimeFormatter;
    }

    /**
     * Get the DateTimeFormatter pattern for format of date output keys
     * @return DateTimeFormatter
     */
    public DateTimeFormatter getKeyOutDateTimeFormatter() {
        return keyOutDateTimeFormatter;
    }

    /**
     * Get the zoned date and time
     * @param dateTime  String of the date and time
     * @param formatter Formatter to use
     * @return  Converted date and time
     */
    public ZonedDateTime getZonedDateTime(String dateTime, DateTimeFormatter formatter) {
        ZonedDateTime zdt = ZonedDateTime.of(LocalDateTime.MIN, ZoneId.systemDefault());
        try {
            zdt = ZonedDateTime.parse(dateTime, formatter);
        } catch (DateTimeParseException dpte) {
            getLogger().error("Cannot parse '" + dateTime + "' using format " + formatter.toString(), dpte);
        }
        return zdt;
    }

    /**
     * Get the zoned date and time
     * @param dateTime  String of the date and time
     * @return  Converted date and time
     */
    public ZonedDateTime getZonedDateTime(String dateTime) {
        return Utils.getZonedDateTime(dateTime, getDateTimeFormatter(), getLogger());
    }

    /**
     * Get the date and time
     * @param dateTime  String of the date and time
     * @return  Converted date and time
     */
    public LocalDateTime getDateTime(String dateTime) {
        return Utils.getDateTime(dateTime, getDateTimeFormatter(), getLogger());
    }

    /**
     * Get the date
     * @param date  String of the date
     * @return  Converted date
     */
    public LocalDate getDate(String date) {
        return Utils.getDate(date, getDateTimeFormatter(), getLogger());
    }

    /**
     * Get the date and time and check if it is filtered
     * @param dateTime  String of the date and time
     * @return  Pair of filter result (TRUE if passes filter) and converted date and time
     */
    public Pair<Boolean, LocalDateTime> getZonedDateTimeAndFilter(String dateTime) {
        LocalDateTime ldt = getZonedDateTime(dateTime).toLocalDateTime();
        return Pair.of(dateFilter.filter(ldt), ldt);
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
     * @param dateFilter Date filter to use
     * @return  Pair of filter result (TRUE if passes filter) and converted date and time
     */
    public Pair<Boolean, LocalDate> getDateAndFilter(String date, DateFilter dateFilter) {
        LocalDate ld = getDate(date);
        return Pair.of(dateFilter.filter(ld), ld);
    }

    /**
     * Get the date and check if it is filtered
     * @param date  String of the date and time
     * @return  Pair of filter result (TRUE if passes filter) and converted date and time
     */
    public Pair<Boolean, LocalDate> getDateAndFilter(String date) {
        return getDateAndFilter(date, dateFilter);
    }

    /**
     * Return the text to use as an output key
     * @param date
     * @return
     */
    public DateWritable getDateOutKey(LocalDate date) {
        return DateWritable.ofDate(date, keyOutDateTimeFormatter);
    }

    /**
     * Read a separated string to a list
     * @param value     String to split
     * @param separator Separator to use
     * @return
     */
    public List<String> readSeparatedString(String value, String separator) {
        return Arrays.stream(value.split(separator))
            .map(String::trim)
            .collect(Collectors.toList());
    }

    public List<String> readCommaSeparatedString(String value) {
        return readSeparatedString(value, ",");
    }

    /**
     * Base configuration class
     */
    public abstract static class AbstractCsvMapperCfg implements ICsvMapperCfg {

        public static final Map<String, Property> COMMON_PROPERTIES;
        static {
            COMMON_PROPERTIES = new HashMap<>();
            List.of(
                Property.of(OUTPUTTYPES_PATH_PROP, "path to factor types input file", ""),
                Property.of(OUTPUTTYPES_FILE_PROP, "file to save factor types to", ""),
                Property.of(FACTOR_PROP, "list of factors to apply to values", ""),
                Property.of(STATS_INPUT_PATH_PROP, "path to statistics file", ""),
                Property.of(VARIABLES_PROP, "list of variables to use", ""),
                Property.of(DEPENDENT_PROP, "dependent variable to use", "")
            ).forEach(p -> COMMON_PROPERTIES.put(p.name, p));
        }

        private final PropertyWrangler propertyWrangler;

        private final String propertyRoot;

        public AbstractCsvMapperCfg(String propertyRoot) {
            this.propertyRoot = propertyRoot;
            this.propertyWrangler = new PropertyWrangler(propertyRoot);
        }

        @Override
        public String getRoot() {
            return propertyRoot;
        }

        @Override
        public String getPropertyName(String propertyPath) {
            return propertyWrangler.getPropertyName(propertyPath);
        }

        @Override
        public String getPropertyPath(String propertyName) {
            return propertyWrangler.getPropertyPath(propertyName);
        }

        @Override
        public HashMap<String, String> getPropertyDefaults() {
            // create map of possible keys and default values
            HashMap<String, String> propDefault = new HashMap<>();

            propDefault.put(DEBUG_PROP, DebugLevel.OFF.name());
            propDefault.put(SEPARATOR_PROP, DEFAULT_SEPARATOR);
            propDefault.put(HAS_HEADER_PROP, Boolean.toString(DEFAULT_HAS_HEADER));
            propDefault.put(DATE_FORMAT_PROP, "");
            propDefault.put(NUM_INDICES_PROP, "0");
            DATE_FILTER_PROPS.forEach((p -> propDefault.put(p, "")));
            propDefault.put(OUT_KEY_DATE_FORMAT_PROP, "");
            getPropertyIndices().forEach(p -> propDefault.put(p, "-1"));
            getRequiredProps().forEach(p -> propDefault.put(p.name, p.defaultValue));
            getAdditionalProps().forEach(p -> propDefault.put(p.name, p.defaultValue));

            return propDefault;
        }

        @Override
        public Pair<Integer, List<String>> checkConfiguration(Configuration conf) {
            int resultCode = ECODE_SUCCESS;
            List<String> errors = new ArrayList<>();

            // check required properties in config
            for (Property prop : getRequiredProps()) {
                if (TextUtils.isEmpty(conf.get(getPropertyPath(prop.name)))) {
                    errors.add("Error: No " + prop.description + " specified, set '" + prop.name + "'.");
                    resultCode = ECODE_CONFIG_ERROR;
                }
            }

            // check for date filtering
            Pair<Integer, List<String>> dateRes = checkDatePairConfiguration(
                                                    conf, FILTER_START_DATE_PROP, FILTER_END_DATE_PROP);
            if (dateRes.getLeft() != ECODE_SUCCESS) {
                errors.addAll(dateRes.getRight());
                resultCode = dateRes.getLeft();
            }

            // check property indices
            for (String key : getPropertyIndices()) {
                if (conf.getInt(getPropertyPath(key), -1) < 0) {
                    errors.add("Error: '" + key + "' not specified.");
                    resultCode = ECODE_CONFIG_ERROR;
                }
            }

            return Pair.of(resultCode, errors);
        }

        public Pair<Integer, List<String>> checkDatePairConfiguration(Configuration conf, String start, String end) {
            int resultCode = ECODE_SUCCESS;
            List<String> errors = new ArrayList<>();

            LocalDate startDate = null;
            LocalDate endDate = null;
            for (String key : List.of(start, end)) {
                String dateStr = conf.get(getPropertyPath(key), "");
                if (!TextUtils.isEmpty(dateStr)) {
                    try {
                        LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
                        if (key.equals(start)) {
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
                        errors.add("Error: '" + start + "' before '" + end + "'");
                        resultCode = ECODE_CONFIG_ERROR;
                    }
                }
            }
            return Pair.of(resultCode, errors);
        }

        @Override
        public Optional<Property> getProperty(String propertyName) {
            Optional<Property> property;
            if (COMMON_PROPERTIES.containsKey(propertyName)) {
                property = Optional.of(COMMON_PROPERTIES.get(propertyName));
            } else {
                property = Optional.empty();
            }
            return property;
        }


        public void dumpConfiguration(Logger logger, Configuration conf) {
            // use info as its the default level
            getPropertyDefaults().forEach((p, d) -> {
                logger.info(
                    String.format("%s - %s [%s]", getRoot(), p, conf.get(getPropertyPath(p), "")));
            });
        }

    }
}



