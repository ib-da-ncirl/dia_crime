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

import ie.ibuttimer.dia_crime.hadoop.ICsvEntryMapperCfg;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Constants {

    // stock index ids
    public static final String NASDAQ_ID = "IXIC";
    public static final String DOWJONES_ID = "DJI";
    public static final String SP500_ID = "GSPC";


    /* properties are stored in the format "<section>.<property>" */
    public static final String PROPERTY_SEPARATOR = ".";

    public static final String CONF_PROPERTY_ROOT = "conf_property_root";

    public static final String GLOBAL_PROP_SECTION = "global";
    public static final String CRIME_PROP_SECTION = "crime";
    public static final String STOCK_PROP_SECTION = "stock";    // common section for stocks
    public static final String NASDAQ_PROP_SECTION = "nasdaq";
    public static final String DOWJONES_PROP_SECTION = "dowjones";
    public static final String SP500_PROP_SECTION = "sp500";
    public static final String WEATHER_PROP_SECTION = "weather";

    // common properties
    public static final String DEBUG_PROP = "debug";
    public static final String STOCK_TAG_PROP = "stock_tag";
    public static final String IN_PATH_PROP = "in_path";
    public static final String OUT_PATH_PROP = "out_path";
    public static final String SEPARATOR_PROP = "separator";
    public static final String HAS_HEADER_PROP = "has_header";
    public static final String NUM_INDICES_PROP = "num_indices";

    public static final String FILTER_START_DATE_PROP = "filter_start_date";
    public static final String FILTER_END_DATE_PROP = "filter_end_date";

    public static final String DATE_FORMAT_PROP = "date_format";
    public static final String DATE_PROP = "date";

    public static final String STATS_PATH_PROP = "stats_path";  // path for stats output file

    // required path properties
    public static final List<String> PATH_PROP_LIST = Arrays.asList(
        IN_PATH_PROP, OUT_PATH_PROP
    );
    public static final List<String> ALL_PATH_PROP_LIST;
    static {
        ALL_PATH_PROP_LIST = Stream.of(PATH_PROP_LIST, Collections.singletonList(STATS_PATH_PROP))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    // crime specific properties
    public static final String PRIMARYTYPE_PROP = "primarytype";
    public static final String DESCRIPTION_PROP = "description";
    public static final String LOCATIONDESCRIPTION_PROP = "locationdescription";
    public static final String IUCR_PROP = "iucr";
    public static final String FBICODE_PROP = "fbicode";
    public static final String TOTAL_PROP = "total";    // total count, generated from data

    public static final String CATEGORIES_PATH_PROP = "categories_path";  // path for categories output file

    // stock specific properties
    public static final String OPEN_PROP = "open";
    public static final String HIGH_PROP = "high";
    public static final String LOW_PROP = "low";
    public static final String CLOSE_PROP = "close";
    public static final String ADJCLOSE_PROP = "adjclose";
    public static final String VOLUME_PROP = "volume";
    public static final String ID_PROP = "id";

    public static final String COUNT_PROP = "count";

    // weather specific properties
    public static final String TEMP_PROP = "temp";
    public static final String FEELS_LIKE_PROP = "feels_like";
    public static final String TEMP_MIN_PROP = "temp_min";
    public static final String TEMP_MAX_PROP = "temp_max";
    public static final String PRESSURE_PROP = "pressure";
    public static final String HUMIDITY_PROP = "humidity";
    public static final String WIND_SPEED_PROP = "wind_speed";
    public static final String WIND_DEG_PROP = "wind_deg";
    public static final String RAIN_1H_PROP = "rain_1h";
    public static final String RAIN_3H_PROP = "rain_3h";
    public static final String SNOW_1H_PROP = "snow_1h";
    public static final String SNOW_3H_PROP = "snow_3h";
    public static final String CLOUDS_ALL_PROP = "clouds_all";
    public static final String WEATHER_ID_PROP = "weather_id";
    public static final String WEATHER_MAIN_PROP = "weather_main";
    public static final String WEATHER_DESC_PROP = "weather_description";

    public static final int ECODE_CONFIG_ERROR = -1;
    public static final int ECODE_SUCCESS = 0;
    public static final int ECODE_FAIL = 1;

    public enum DebugLevel { OFF, LOW, MEDIUM, HIGH, VERBOSE;

        public static DebugLevel getSetting(Configuration conf, ICsvEntryMapperCfg sCfgChk) {
            String setting = conf.get(sCfgChk.getPropertyPath(DEBUG_PROP), OFF.name());
            return DebugLevel.valueOf(setting);
        }

        public static boolean show(DebugLevel setting, DebugLevel level) {
            return (setting != OFF) && (level.ordinal() <= setting.ordinal());
        }

        public boolean showMe(DebugLevel setting) {
            return show(setting, this);
        }

        public static boolean show(Configuration conf, ICsvEntryMapperCfg sCfgChk, DebugLevel level) {
            DebugLevel setting = getSetting(conf, sCfgChk);
            return show(setting, level);
        }
    }

    /**
     * Private default constructor so class can't be instantiated
     */
    private Constants() {
        // no op
    }
}