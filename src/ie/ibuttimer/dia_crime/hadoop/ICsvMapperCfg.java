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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static ie.ibuttimer.dia_crime.misc.Constants.IN_PATH_PROP;
import static ie.ibuttimer.dia_crime.misc.Constants.OUT_PATH_PROP;

/**
 * Interface to be implemented by Mapper configurations
 */
public interface ICsvMapperCfg extends ITagger {

    /**
     * Get the property names and their default values
     * @return Map with property name as the key and default property value as the value
     */
    HashMap<String, String> getPropertyDefaults();

    /**
     * Check if the specified configuration is valid
     * @param conf  Configuration to check
     * @return  Pair with ECODE_SUCCESS if configuration valid or ECODE_CONFIG_ERROR otherwise, and a list of errors
     *          if applicable
     */
    Pair<Integer, List<String>> checkConfiguration(Configuration conf);

    /**
     * Get additional properties
     * @return List of Pairs with left of property name and right of default value
     */
    default List<Property> getAdditionalProps() {
        return List.of();
    }

    /**
     * Get required properties
     * @return List of Pairs with left of property name and right of description
     */
    default List<Property> getRequiredProps() {
        List<Property> props = new ArrayList<>();
        props.add(Property.of(IN_PATH_PROP, "input path", ""));
        props.add(Property.of(OUT_PATH_PROP, "output path", ""));
        return props;
    }

    /**
     * Return a list of the names of indices properties
     * @return List of names
     */
    List<String> getPropertyIndices();

    /**
     * Return the property root, i.e. the configuration file section
     * @return
     */
    String getPropertyRoot();

    /**
     * Construct a full property path for the specified name
     * @param propertyName
     * @return
     */
    String getPropertyPath(String propertyName);

    /**
     * Dump the configuration using the specified logger
     * @param logger
     * @param conf
     */
    void dumpConfiguration(Logger logger, Configuration conf);


    /**
     * Property details
     */
    class Property {
        String name;
        String description;
        String defaultValue;

        public Property(String name, String description, String defaultValue) {
            this.name = name;
            this.description = description;
            this.defaultValue = defaultValue;
        }

        public static Property of(String name, String description, String defaultValue) {
            return new Property(name, description, defaultValue);
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public String getDefaultValue() {
            return defaultValue;
        }
    }
}
