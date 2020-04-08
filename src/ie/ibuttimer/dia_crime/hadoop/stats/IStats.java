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

import ie.ibuttimer.dia_crime.misc.MapStringifier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Interface to be implemented to provide standardised output for a statistics job
 */
public interface IStats {

    /* key tag can take the following forms:
        'key-tag' : where 'key' is the property name & 'tag' is the metric
        'key1+key2-tag' : where 'key1' & 'key2' are the property names & 'tag' is the metric
     */
    String KEY_TAG_SPLIT = "-";
    String KEY_KEY_SPLIT = "+";

    String SUM_KEY_TAG = "SUM";
    String SQUARE_KEY_TAG = "SQ";
    String MIN_KEY_TAG = "MIN";
    String MAX_KEY_TAG = "MAX";
    String COUNT_KEY_TAG = "CNT";
    String PRODUCT_KEY_TAG = "PRD";
    String ZERO_KEY_TAG = "ZERO";

    MapStringifier.ElementStringify TAG_WRANGLER = new MapStringifier.ElementStringify(KEY_TAG_SPLIT);
    MapStringifier.ElementStringify KEY_PAIR_WRANGLER = new MapStringifier.ElementStringify(KEY_KEY_SPLIT, "\\" + KEY_KEY_SPLIT);

    List<String> KEY_TAGS = Arrays.asList(SUM_KEY_TAG, SQUARE_KEY_TAG, MIN_KEY_TAG, MAX_KEY_TAG,
        COUNT_KEY_TAG, PRODUCT_KEY_TAG, ZERO_KEY_TAG);

    default String getSumKeyTag(String key) {
        return TAG_WRANGLER.stringifyElement(key, SUM_KEY_TAG);
    }

    default String getSquareKeyTag(String key) {
        return TAG_WRANGLER.stringifyElement(key, SQUARE_KEY_TAG);
    }

    default String getMinKeyTag(String key) {
        return TAG_WRANGLER.stringifyElement(key, MIN_KEY_TAG);
    }

    default String getMaxKeyTag(String key) {
        return TAG_WRANGLER.stringifyElement(key, MAX_KEY_TAG);
    }

    default String getCountKeyTag(String key) {
        return TAG_WRANGLER.stringifyElement(key, COUNT_KEY_TAG);
    }

    default String getProductKeyTag(String key) {
        return TAG_WRANGLER.stringifyElement(key, PRODUCT_KEY_TAG);
    }

    default String getZeroKeyTag(String key) {
        return TAG_WRANGLER.stringifyElement(key, ZERO_KEY_TAG);
    }

    default String getKeyPair(String key1, String key2) {
        return KEY_PAIR_WRANGLER.stringifyElement(key1, key2);
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

    default boolean isProductKeyTag(String key) {
        return key.endsWith(PRODUCT_KEY_TAG);
    }

    default boolean isZeroKeyTag(String key) {
        return key.endsWith(ZERO_KEY_TAG);
    }

    default boolean isStandardKey(String key) {
        return KEY_TAGS.stream().noneMatch(key::endsWith) && !isKeyPair(key);
    }

    default boolean isKeyPair(String key) {
        return key.contains(KEY_KEY_SPLIT);
    }

    default Pair<String, String> splitKeyTag(String key) {
        return TAG_WRANGLER.destringifyElement(key);
    }

    default Pair<String, String> splitKeyPair(String key) {
        return KEY_PAIR_WRANGLER.destringifyElement(key);
    }

    default Triple<Optional<String>, Optional<String>, Optional<String>> split(String key) {
        Optional<String> key1 = Optional.empty();
        Optional<String> key2 = Optional.empty();
        Optional<String> tag = Optional.empty();
        if (isStandardKey(key)) {
            key1 = Optional.of(key);
        } else {
            Pair<String, String> pair = splitKeyTag(key);
            String keyBase = pair.getLeft();

            tag = Optional.of(pair.getRight());

            if (isKeyPair(keyBase)) {
                pair = splitKeyPair(keyBase);
                key1 = Optional.of(pair.getLeft());
                key2 = Optional.of(pair.getRight());
            } else {
                key1 = Optional.of(keyBase);
            }
        }
        return Triple.of(key1, key2, tag);
    }
}
