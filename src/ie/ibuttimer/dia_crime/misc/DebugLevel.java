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

import ie.ibuttimer.dia_crime.hadoop.ICsvMapperCfg;
import org.apache.hadoop.conf.Configuration;

import static ie.ibuttimer.dia_crime.misc.Constants.DEBUG_PROP;

/**
 * Debug level control
 */
public enum DebugLevel {

    OFF, LOW, MEDIUM, HIGH, VERBOSE;

    public static DebugLevel getSetting(Configuration conf, ICsvMapperCfg sCfgChk) {
        String setting = conf.get(sCfgChk.getPropertyPath(DEBUG_PROP), OFF.name());
        return DebugLevel.valueOf(setting);
    }

    public static DebugLevel getSetting(Configuration conf, String section) {
        String setting = conf.get(PropertyWrangler.of(section).getPropertyPath(DEBUG_PROP), OFF.name());
        return DebugLevel.valueOf(setting);
    }

    public static DebugLevel getSetting(Configuration conf, IPropertyWrangler wrangler) {
        String setting = conf.get(wrangler.getPropertyPath(DEBUG_PROP), OFF.name());
        return DebugLevel.valueOf(setting);
    }

    public static boolean show(DebugLevel setting, DebugLevel level) {
        return (setting != OFF) && (level.ordinal() <= setting.ordinal());
    }

    public boolean showMe(DebugLevel setting) {
        return show(setting, this);
    }

    public static boolean show(Configuration conf, ICsvMapperCfg sCfgChk, DebugLevel level) {
        DebugLevel setting = getSetting(conf, sCfgChk);
        return show(setting, level);
    }

    public interface Debuggable {

        DebugLevel getDebugLevel();

        void setDebugLevel(DebugLevel debugLevel);

        default boolean show(DebugLevel level) {
            return level.showMe(getDebugLevel());
        }
    }

    public static abstract class AbstractDebuggable implements Debuggable {

        private DebugLevel debugLevel;  // current debug level

        public AbstractDebuggable(DebugLevel debugLevel) {
            this.debugLevel = debugLevel;
        }

        @Override
        public DebugLevel getDebugLevel() {
            return debugLevel;
        }

        @Override
        public void setDebugLevel(DebugLevel debugLevel) {
            this.debugLevel = debugLevel;
        }
    }
}
