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

/**
 * Application constants
 */
public class Constants {

    public static final String COMMENT_PREFIX = "#";

    // stock index ids
    public static final String NASDAQ_ID = "IXIC";
    public static final String DOWJONES_ID = "DJI";
    public static final String SP500_ID = "GSPC";

    /* properties are stored in the format "<section>.<property>" */
    public static final String PROPERTY_SEPARATOR = ".";

    /* property aliases are stored in the format "<section>.<property> = property-alias:<other section>.<other property>" */
    public static final String PROPERTY_ALIAS = "property-alias";
    public static final String PROPERTY_ALIAS_SEPARATOR = ":";

    public static final String CONF_PROPERTY_ROOT = "conf_property_root";

    public static final String GLOBAL_PROP_SECTION = "global";
    public static final String CRIME_PROP_SECTION = "crime";
    public static final String STOCK_PROP_SECTION = "stock";    // common section for stocks
    public static final String NASDAQ_PROP_SECTION = "nasdaq";
    public static final String DOWJONES_PROP_SECTION = "dowjones";
    public static final String SP500_PROP_SECTION = "sp500";
    public static final String WEATHER_PROP_SECTION = "weather";
    public static final String REGRESSION_PROP_SECTION = "regression";
    public static final String VERIFICATION_PROP_SECTION = "verification";
    public static final String STATS_PROP_SECTION = "stats";
    public static final String NORMALISE_PROP_SECTION = "normalise";
    public static final String MATRIX_PROP_1_SECTION = "matrix1";
    public static final String MATRIX_PROP_2_SECTION = "matrix2";

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

    public static final String OUT_KEY_DATE_FORMAT_PROP = "out_key_date_format";

    public static final String DATE_FORMAT_PROP = "date_format";
    public static final String DATE_PROP = "date";

    public static final String STATS_PATH_PROP = "stats_path";  // path for stats output file

    // crime specific properties
    public static final String PRIMARYTYPE_PROP = "primarytype";
    public static final String DESCRIPTION_PROP = "description";
    public static final String LOCATIONDESCRIPTION_PROP = "locationdescription";
    public static final String IUCR_PROP = "iucr";
    public static final String FBICODE_PROP = "fbicode";
    public static final String TOTAL_PROP = "total";    // total count, generated from data

    public static final String OUTPUTTYPES_PATH_PROP = "outputtypes_path";  // path for output types file (for input)

    // stock specific properties
    public static final String OPEN_PROP = "open";
    public static final String HIGH_PROP = "high";
    public static final String LOW_PROP = "low";
    public static final String CLOSE_PROP = "close";
    public static final String ADJCLOSE_PROP = "adjclose";
    public static final String VOLUME_PROP = "volume";
    public static final String ID_PROP = "id";

    public static final String ID_LIST_PROP = "id_list";
    public static final String COUNT_PROP = "count";
    public static final String FACTOR_PROP = "factors";

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

    public static final String WIDS_PATH_PROP = "wids_path";  // path for weather ids file (for input)

    // regression specific properties
    public static final String INDEPENDENTS_PROP = "independents";
    public static final String DEPENDENT_PROP = "dependent";
    public static final String TRAIN_OUTPUT_PATH_PROP = "train_output_path";
    public static final String STATS_INPUT_PATH_PROP = "stats_input_path";
    public static final String LEARNING_RATE_PROP = "learning_rate";
    public static final String EPOCH_LIMIT_PROP = "epoch_limit";
    public static final String TARGET_COST_PROP = "target_cost";
    public static final String STEADY_TARGET_PROP = "steady_target";
    public static final String STEADY_LIMIT_PROP = "steady_limit";
    public static final String TARGET_TIME_PROP = "target_time";
    public static final String INCREASE_LIMIT_PROP = "increase_limit";
    public static final String WEIGHT_PROP = "weight";
    public static final String BIAS_PROP = "bias";
    public static final String CURRENT_EPOCH_PROP = "current_epoch";

    public static final String TRAIN_START_DATE_PROP = "train_start_date";
    public static final String TRAIN_END_DATE_PROP = "train_end_date";
    public static final String VALIDATE_START_DATE_PROP = "validate_start_date";
    public static final String VALIDATE_END_DATE_PROP = "validate_end_date";
    public static final String VALIDATE_MODEL_PATH_PROP = "model_path";

    // stats specific properties
    public static final String VARIABLES_PROP = "variables";

    public static final String VARIABLES_NUMERIC = "numeric";
    public static final String VARIABLES_ALL = "all";

    // matrix specific properties
    public static final String SPEC_PROP = "spec";
    public static final String SPEC_OTHER_PROP = "spec_other";

    // normalisation specific properties
    public static final String CSW_IN_PATH_PROP = "csw_in_path";
    public static final String CS_IN_PATH_PROP = "cs_in_path";
    public static final String CW_IN_PATH_PROP = "cw_in_path";



    public static final String TYPES_NAMED_OP = "types";
    public static final String WEATHER_ID_NAMED_OP = "wids";


    public static final int ECODE_CONFIG_ERROR = -1;
    public static final int ECODE_SUCCESS = 0;
    public static final int ECODE_FAIL = 1;
    public static final int ECODE_RUNNING = 2;

    private static MapStringifier.ElementStringify propertyNamer = new MapStringifier.ElementStringify(PROPERTY_SEPARATOR);

    public static String generatePropertyName(String section, String property) {
        return propertyNamer.stringifyElement(section, property);
    }

    /**
     * Private default constructor so class can't be instantiated
     */
    private Constants() {
        // no op
    }
}
