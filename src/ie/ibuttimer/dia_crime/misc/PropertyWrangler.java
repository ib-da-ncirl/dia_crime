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

import org.apache.hadoop.shaded.org.apache.http.util.TextUtils;

import static ie.ibuttimer.dia_crime.misc.Constants.PROPERTY_SEPARATOR;

/**
 * Configuration property manipulation
 */
public class PropertyWrangler implements IPropertyWrangler {

    private String root;

    public PropertyWrangler() {
        this(null);
    }

    public PropertyWrangler(String root) {
        setRoot(root);
    }

    public static PropertyWrangler of(String root) {
        return new PropertyWrangler(root);
    }

    public void setRoot(String root) {
        this.root = makePropertyRoot(root);
    }

    public String getRoot() {
        String rawRoot;
        if (root.endsWith(PROPERTY_SEPARATOR)) {
            rawRoot = root.substring(0, root.indexOf(PROPERTY_SEPARATOR));
        } else {
            rawRoot = root;
        }
        return rawRoot;
    }

    public static String makePropertyRoot(String propRoot) {
        String root;
        if (TextUtils.isEmpty(propRoot) || TextUtils.isBlank(propRoot)) {
            root = "";
        } else {
            root = propRoot + PROPERTY_SEPARATOR;
        }
        return root;
    }

    @Override
    public String getPropertyPath(String propertyName) {
        return root + propertyName;
    }

    public boolean hasRoot(String propertyPath) {
        return propertyPath.startsWith(root);
    }

    @Override
    public String getPropertyName(String propertyPath) {
        String name;
        if (hasRoot(propertyPath)) {
            name = propertyPath.substring(root.length());
        } else {
            name = propertyPath;
        }
        return name;
    }

}
