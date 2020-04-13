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

/**
 * Enum of Hadoop counters
 */
public enum CountersEnum {

    // Stock
    STOCK_MAPPER_COUNT,
    SP500_STOCK_MAPPER_COUNT,
    NASDAQ_STOCK_MAPPER_COUNT,
    DOWJONES_STOCK_MAPPER_COUNT,
    STOCK_REDUCER_COUNT,

    // Crime
    CRIME_MAPPER_COUNT,
    CRIME_REDUCER_COUNT,

    // Merge
    MERGE_REDUCER_COUNT,
    MERGE_REDUCER_GROUP_IN_COUNT,
    MERGE_REDUCER_GROUP_OUT_COUNT,

    // Regression
    WEATHER_MAPPER_COUNT,
    WEATHER_REDUCER_COUNT,

    // Regression
    REGRESSION_MAPPER_COUNT,
    REGRESSION_REDUCER_COUNT,

    // Matrix
    MATRIX_MAPPER_COUNT,
    MATRIX_REDUCER_COUNT,

    // Normalise
    NORMALISE_MAPPER_COUNT,
    NORMALISE_REDUCER_COUNT,

    // Stats
    STATS_MAPPER_COUNT,
    STATS_REDUCER_COUNT,
    STATS_REDUCER_GROUP_IN_COUNT,
    STATS_REDUCER_GROUP_OUT_COUNT
}
