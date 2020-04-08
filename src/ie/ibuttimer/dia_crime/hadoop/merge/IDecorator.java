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

package ie.ibuttimer.dia_crime.hadoop.merge;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Decorator interface for key/values
 * @param <K> Key class
 * @param <V> Value class
 */
public interface IDecorator<K, V> {

    enum DecorMode { NONE, DECORATE, TRANSFORM, DECORATE_TRANSFORM;

        public boolean hasDecorate() {
            return equals(DECORATE) || this.equals(DECORATE_TRANSFORM);
        }

        public boolean hasTransform() {
            return equals(TRANSFORM) || this.equals(DECORATE_TRANSFORM);
        }
    }

    /**
     * Decorate the specified key
     * @param key
     * @return
     */
    default K decorateKey(K key) {
        return key;
    }

    /**
     * Transform the specified key to a new object
     * @param key
     * @return
     */
    default Object transformKey(K key) {
        return key;
    }

    /**
     * Decorate the specified value
     * @param value
     * @return
     */
    default V decorateValue(V value) {
        return value;
    }

    /**
     * Transform the specified value to a new object
     * @param value
     * @return
     */
    default Object transformValue(V value) {
        return value;
    }


    /**
     * Interface for classes which may decorate key/values
     * @param <K>
     * @param <V>
     */
    interface IDecoratable<K, V> {

        DecorMode getMode();

        IDecorator<K, V> getDecorator();

        void setDecorator(IDecorator<K, V> decorator, IDecorator.DecorMode decoratorMode);

        default Pair<K, V> decorate(K key, V value) {
            Pair<K, V> decorated;
            IDecorator<K, V> decorator = getDecorator();
            if (decorator != null && getMode().hasDecorate()) {
                decorated = Pair.of(decorator.decorateKey(key), decorator.decorateValue(value));
            } else {
                decorated = Pair.of(key, value);
            }
            return decorated;
        }

        default Pair<Object, Object> transform(K key, V value) {
            Pair<Object, Object> transformed;
            IDecorator<K, V> decorator = getDecorator();
            if (decorator != null && getMode().hasTransform()) {
                transformed = Pair.of(decorator.transformKey(key), decorator.transformValue(value));
            } else {
                transformed = Pair.of(key, value);
            }
            return transformed;
        }

    }


    /**
     * Base class for IDecoratable classes
     * @param <K>
     * @param <V>
     */
    abstract class AbstractDecoratable<K, V> implements IDecoratable<K, V> {

        private IDecorator<K, V> decorator;
        private IDecorator.DecorMode decoratorMode;

        public AbstractDecoratable() {
            setDecorator(null, decoratorMode);
        }

        @Override
        public void setDecorator(IDecorator<K, V> decorator, DecorMode decoratorMode) {
            this.decorator = decorator;
            this.decoratorMode = decoratorMode;
        }

        @Override
        public DecorMode getMode() {
            return decoratorMode;
        }

        @Override
        public IDecorator<K, V> getDecorator() {
            return decorator;
        }
    }
}
