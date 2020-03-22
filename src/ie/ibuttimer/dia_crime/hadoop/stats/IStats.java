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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public interface IStats {

    String SUM_KEY_TAG = "_SUM";
    String SQUARE_KEY_TAG = "_SQ";
    String MIN_KEY_TAG = "_MIN";
    String MAX_KEY_TAG = "_MAX";
    String COUNT_KEY_TAG = "_CNT";

    List<String> KEY_TAGS = Arrays.asList(SUM_KEY_TAG, SQUARE_KEY_TAG, MIN_KEY_TAG, MAX_KEY_TAG, COUNT_KEY_TAG);

    default String getSumKeyTag(String key) {
        return key + SUM_KEY_TAG;
    }

    default String getSquareKeyTag(String key) {
        return key + SQUARE_KEY_TAG;
    }

    default String getMinKeyTag(String key) {
        return key + MIN_KEY_TAG;
    }

    default String getMaxKeyTag(String key) {
        return key + MAX_KEY_TAG;
    }

    default String getCountKeyTag(String key) {
        return key + COUNT_KEY_TAG;
    }

    default boolean isSumKey(String key) {
        return key.endsWith(SUM_KEY_TAG);
    }

    default boolean isSquareKey(String key) {
        return key.endsWith(SQUARE_KEY_TAG);
    }

    default boolean isMinKey(String key) {
        return key.endsWith(MIN_KEY_TAG);
    }

    default boolean isMaxKey(String key) {
        return key.endsWith(MAX_KEY_TAG);
    }

    default boolean isCountKey(String key) {
        return key.endsWith(COUNT_KEY_TAG);
    }

    default boolean isStandardKey(String key) {
        return KEY_TAGS.stream().noneMatch(key::endsWith);
    }

}
