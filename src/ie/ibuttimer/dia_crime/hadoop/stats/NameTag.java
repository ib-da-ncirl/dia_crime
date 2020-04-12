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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public enum NameTag {
    SUM,
    SQ,
    MIN,
    MAX,
    CNT,    // count
    MEAN,
    PRD,    // product
    ZERO,
    ERR,    // error
    PDW,    // partial derivative weight
    PDB,    // partial derivative bias
    YHAT;   // predicted value

    /* key tag can take the following forms:
        'key-tag' : where 'key' is the property name & 'tag' is the metric
        'key1+key2-tag' : where 'key1' & 'key2' are the property names & 'tag' is the metric
     */
    public static final String KEY_TAG_SPLIT = "-";
    public static final String KEY_KEY_SPLIT = "+";

    private static final MapStringifier.ElementStringify TAG_WRANGLER = new MapStringifier.ElementStringify(KEY_TAG_SPLIT);
    private static final MapStringifier.ElementStringify KEY_PAIR_WRANGLER = new MapStringifier.ElementStringify(KEY_KEY_SPLIT, "\\" + KEY_KEY_SPLIT);

    public static String getKeyTagChain(String key, List<NameTag> tags) {
        AtomicReference<String> taggedKey = new AtomicReference<>(key);
        tags.forEach(t -> taggedKey.set(getKeyTag(taggedKey.get(), t)));
        return taggedKey.get();
    }

    public static String getKeyTag(String key, NameTag tag) {
        return TAG_WRANGLER.stringifyElement(key, tag.name());
    }

    public String getKeyTag(String key) {
        return getKeyTag(key, this);
    }

    public static String getKeyPair(String key1, String key2) {
        return KEY_PAIR_WRANGLER.stringifyElement(key1, key2);
    }

    public static boolean isKeyPair(String key) {
        return key.contains(KEY_KEY_SPLIT);
    }

    public static boolean isStandardKey(String key) {
        return List.of(values()).stream().map(NameTag::name).noneMatch(key::endsWith) && !isKeyPair(key);
    }

    public static boolean is(String key, NameTag tag) {
        return key.endsWith(tag.name());
    }

    public boolean is(String key) {
        return is(key, this);
    }

    public static Pair<String, String> splitKeyTag(String keyTag) {
        return TAG_WRANGLER.destringifyElement(keyTag);
    }

    public static Pair<String, String> splitKeyPair(String keyPair) {
        return KEY_PAIR_WRANGLER.destringifyElement(keyPair);
    }

    public static Triple<Optional<String>, Optional<String>, Optional<String>> split(String key) {
        Optional<String> key1;
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
