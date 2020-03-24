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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public interface ICsvEntryMapperCfg {

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

    default List<Pair<String, String>> getRequiredProps() {
        return List.of();
    }

    /**
     * Return a list of the names of properties
     * @return List of names
     */
    List<String> getPropertyIndices();
}
