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

package ie.ibuttimer.dia_crime.hadoop.regression;


import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;

public class LinearRegressor {

    private Map<String, Double> coefficients;
//    private double weight;
    private double bias;
    private double learningRate;

    public LinearRegressor(Map<String, Double> setting, double bias, double learningRate) {
        this.bias = bias;
        this.coefficients = new HashMap<>();
        setCoefficients(setting);
        this.learningRate = learningRate;
    }

    public void setCoefficient(String name, Double value) {
        coefficients.put(name, value);
    }

    public void setCoefficients(Map<String, Double> setting) {
        coefficients.putAll(setting);
    }



    /* cost function for mean squared error is:
         MSE = 1/N * sum( (yi - ((weight * xi) + bias))^2 )
     */
    public double cost(double sqErrorSum, long count) {
        return sqErrorSum / count;
    }

    public double predict(Map<String, Double> independents) {
        AtomicDouble sum = new AtomicDouble(bias);
        coefficients.forEach((key, value) -> {
            sum.addAndGet(independents.get(key) * value);
        });
        return sum.get();
    }

    public double error(double yi, Map<String, Double> independents) {
        /* the error for one observation is:
            ei = (yi - ((weight * xi) + bias))
         */
        return yi - predict(independents);
    }

    public double sqError(double ei) {
        /* the squared error for one observation is:
            sei = (yi - ((weight * xi) + bias))^2
         */
        return Math.pow(ei, 2);
    }

    /* the gradient of the cost function is:
         f'(weight, bias) = [ 1/N * sum( -2 * xi * (yi - ((weight * xi) + bias)) ),
                                1/N * sum( -2 * (yi - ((weight * xi) + bias) ) ]
     */

    public Pair<Map<String, Double>, Double> calcUpdatedWeights(Map<String, Double> pdWeightSum, double pdBiasSum, long count) {
        Map<String, Double> newCoef = new HashMap<>();

        // subtract because the derivatives point in direction of steepest ascent
        coefficients.forEach((key, value) -> {
            double weight = value - ((pdWeightSum.get(key) / count) * learningRate);
            newCoef.put(key, weight);
        });
        bias -= ((pdBiasSum / count) * learningRate);
        return Pair.of(newCoef, bias);
    }

    public Map<String, Double> partialDerivativeWeight(Map<String, Double> independents, double ei) {
        /* the partial derivative for weight is:
             -2 * xi * (yi - ((weight * xi) + bias))
         */
        Map<String, Double> pdWeight = new HashMap<>();
        independents.forEach((indo, value) -> {
            pdWeight.put(indo, -2 * value * ei);
        });
        return pdWeight;
    }

    public double partialDerivativeBias(double ei) {
        /* the partial derivative for weight is:
             -2 * (yi - ((weight * xi) + bias))
         */
        return -2 * ei;
    }


    /**
     * Calculated the term for the Regression Sum of Squares (SSR)
     * @param yhati     predicted value
     * @param ymean     mean value
     * @return
     */
    public double regressionSum(double yhati, double ymean) {
        /* the SSR is:
            sum((yhati - ymean)^2)
         */
        return Math.pow(yhati - ymean, 2);
    }

    /**
     * Calculated the term for the Error Sum of Squares (SSE)
     * @param yi        actual value
     * @param yhati     predicted value
     * @return
     */
    public double errorSum(double yi, double yhati) {
        /* the SSR is:
            sum((yi - yhati)^2)
         */
        return Math.pow(yi - yhati, 2);
    }

    /**
     * Calculated the term for the Total Sum of Squares (SST)
     * @param yi        actual value
     * @param ymean     mean value
     * @return
     */
    public double totalSum(double yi, double ymean) {
        /* the SSR is:
            sum((yi - ymean)^2)
         */
        return Math.pow(yi - ymean, 2);
    }

    /**
     * Calculated the Coefficient of Determination (R squared)
     * @param ssr     Regression Sum of Squares
     * @param sst     Total Sum of Squares
     * @return
     */
    public double calcRSquared(double ssr, double sst) {
        /* R squared is:
            SSR/SST
         */
        return ssr / sst;
    }

    /**
     * Calculated the Adjusted Coefficient of Determination (R bar squared)
     * @param rSquared  Coefficient of Determination
     * @param n         sample size
     * @param k         number of independent variables
     * @return
     */
    public double calcRBarSquared(double rSquared, long n, long k) {
        /* R bar squared is:
            1 - ((n - 1)/(n-k-1)) * (1 - R squared)
         */
        return 1 - (((double)(n - 1)/(n - k - 1)) * (1 - rSquared));
    }





    @Override
    public String toString() {
        return "LinearRegressor{" +
            "coefficients=" + coefficients +
            ", bias=" + bias +
            ", learningRate=" + learningRate +
            '}';
    }
}
