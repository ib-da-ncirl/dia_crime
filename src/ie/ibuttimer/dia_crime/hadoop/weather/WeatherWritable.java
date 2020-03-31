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

package ie.ibuttimer.dia_crime.hadoop.weather;

import ie.ibuttimer.dia_crime.hadoop.AbstractBaseWritable;
import ie.ibuttimer.dia_crime.misc.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static ie.ibuttimer.dia_crime.misc.Constants.*;


public class WeatherWritable extends AbstractBaseWritable<WeatherWritable> implements Writable {

    public static List<String> FIELDS;
    public static List<String> FLOAT_FIELDS = Arrays.asList(TEMP_PROP, FEELS_LIKE_PROP, TEMP_MIN_PROP, TEMP_MAX_PROP,
        WIND_SPEED_PROP, RAIN_1H_PROP, RAIN_3H_PROP, SNOW_1H_PROP, SNOW_3H_PROP);
    public static List<String> INT_FIELDS = Arrays.asList(PRESSURE_PROP, HUMIDITY_PROP, WIND_DEG_PROP, CLOUDS_ALL_PROP,
        WEATHER_ID_PROP);
    public static List<String> STR_FIELDS = Arrays.asList(WEATHER_MAIN_PROP, WEATHER_DESC_PROP);
    static {
        FIELDS = new ArrayList<>(AbstractBaseWritable.FIELDS);
        FIELDS.addAll(FLOAT_FIELDS);
        FIELDS.addAll(INT_FIELDS);
        FIELDS.addAll(STR_FIELDS);
    }

    private float temp;
    private float feelsLike;
    private float tempMin;
    private float tempMax;
    private float windSpeed;
    private float rain1h;
    private float rain3h;
    private float snow1h;
    private float snow3h;

    private int pressure;
    private int humidity;
    private int windDeg;
    private int clouds;
    private int weatherId;

    private String weatherMain;
    private String weatherDescription;

    /* dt,dt_iso,timezone,city_name,lat,lon,temp,feels_like,temp_min,temp_max,pressure,sea_level,grnd_level,humidity,
        wind_speed,wind_deg,rain_1h,rain_3h,snow_1h,snow_3h,clouds_all,weather_id,weather_main,weather_description,weather_icon
     */

    // Default constructor to allow (de)serialization
    public WeatherWritable() {
        super();
        this.temp = 0;
        this.feelsLike = 0;
        this.tempMin = 0;
        this.tempMax = 0;
        this.windSpeed = 0;
        this.rain1h = 0;
        this.rain3h = 0;
        this.snow1h = 0;
        this.snow3h = 0;

        this.pressure = 0;
        this.humidity = 0;
        this.windDeg = 0;
        this.clouds = 0;
        this.weatherId = 0;

        this.weatherMain = "";
        this.weatherDescription = "";
    }

    @Override
    public WeatherWritable getInstance() {
        return new WeatherWritable();
    }

    public static WeatherWritable read(DataInput dataInput) throws IOException {
        WeatherWritable writable = new WeatherWritable();
        writable.readFields(dataInput);
        return writable;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeFloat(temp);
        dataOutput.writeFloat(feelsLike);
        dataOutput.writeFloat(tempMin);
        dataOutput.writeFloat(tempMax);
        dataOutput.writeFloat(windSpeed);
        dataOutput.writeFloat(rain1h);
        dataOutput.writeFloat(rain3h);
        dataOutput.writeFloat(snow1h);
        dataOutput.writeFloat(snow3h);

        dataOutput.writeInt(pressure);
        dataOutput.writeInt(humidity);
        dataOutput.writeInt(windDeg);
        dataOutput.writeInt(clouds);
        dataOutput.writeInt(weatherId);

        Text.writeString(dataOutput, weatherMain);
        Text.writeString(dataOutput, weatherDescription);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        this.temp = dataInput.readFloat();
        this.feelsLike = dataInput.readFloat();
        this.tempMin = dataInput.readFloat();
        this.tempMax = dataInput.readFloat();
        this.windSpeed = dataInput.readFloat();
        this.rain1h = dataInput.readFloat();
        this.rain3h = dataInput.readFloat();
        this.snow1h = dataInput.readFloat();
        this.snow3h = dataInput.readFloat();

        this.pressure = dataInput.readInt();
        this.humidity = dataInput.readInt();
        this.windDeg = dataInput.readInt();
        this.clouds = dataInput.readInt();
        this.weatherId = dataInput.readInt();

        this.weatherMain = Text.readString(dataInput);
        this.weatherDescription = Text.readString(dataInput);
    }

    public float getTemp() {
        return temp;
    }

    public void setTemp(float temp) {
        this.temp = temp;
    }

    public float getFeelsLike() {
        return feelsLike;
    }

    public void setFeelsLike(float feelsLike) {
        this.feelsLike = feelsLike;
    }

    public float getTempMin() {
        return tempMin;
    }

    public void setTempMin(float tempMin) {
        this.tempMin = tempMin;
    }

    public float getTempMax() {
        return tempMax;
    }

    public void setTempMax(float tempMax) {
        this.tempMax = tempMax;
    }

    public float getWindSpeed() {
        return windSpeed;
    }

    public void setWindSpeed(float windSpeed) {
        this.windSpeed = windSpeed;
    }

    public float getRain1h() {
        return rain1h;
    }

    public void setRain1h(float rain1h) {
        this.rain1h = rain1h;
    }

    public float getRain3h() {
        return rain3h;
    }

    public void setRain3h(float rain3h) {
        this.rain3h = rain3h;
    }

    public float getSnow1h() {
        return snow1h;
    }

    public void setSnow1h(float snow1h) {
        this.snow1h = snow1h;
    }

    public float getSnow3h() {
        return snow3h;
    }

    public void setSnow3h(float snow3h) {
        this.snow3h = snow3h;
    }

    public int getPressure() {
        return pressure;
    }

    public void setPressure(int pressure) {
        this.pressure = pressure;
    }

    public int getHumidity() {
        return humidity;
    }

    public void setHumidity(int humidity) {
        this.humidity = humidity;
    }

    public int getWindDeg() {
        return windDeg;
    }

    public void setWindDeg(int windDeg) {
        this.windDeg = windDeg;
    }

    public int getClouds() {
        return clouds;
    }

    public void setClouds(int clouds) {
        this.clouds = clouds;
    }

    public String getWeatherMain() {
        return weatherMain;
    }

    public int getWeatherId() {
        return weatherId;
    }

    public void setWeatherId(int weatherId) {
        this.weatherId = weatherId;
    }

    public void setWeatherMain(String weatherMain) {
        this.weatherMain = weatherMain;
    }

    public String getWeatherDescription() {
        return weatherDescription;
    }

    public void setWeatherDescription(String weatherDescription) {
        this.weatherDescription = weatherDescription;
    }

    @Override
    public void set(WeatherWritable other) {
        super.set(other);
        this.temp = other.temp;
        this.feelsLike = other.feelsLike;
        this.tempMin = other.tempMin;
        this.tempMax = other.tempMax;
        this.windSpeed = other.windSpeed;
        this.rain1h = other.rain1h;
        this.rain3h = other.rain3h;
        this.snow1h = other.snow1h;
        this.snow3h = other.snow3h;

        this.pressure = other.pressure;
        this.humidity = other.humidity;
        this.windDeg = other.windDeg;
        this.clouds = other.clouds;
        this.weatherId = other.weatherId;

        this.weatherMain = other.weatherMain;
        this.weatherDescription = other.weatherDescription;
    }

    @Override
    public void add(WeatherWritable other) {
        super.add(other);
        this.temp += other.temp;
        this.feelsLike += other.feelsLike;
        this.tempMin += other.tempMin;
        this.tempMax += other.tempMax;
        this.windSpeed += other.windSpeed;
        this.rain1h += other.rain1h;
        this.rain3h += other.rain3h;
        this.snow1h += other.snow1h;
        this.snow3h += other.snow3h;

        this.pressure += other.pressure;
        this.humidity += other.humidity;
        this.windDeg += other.windDeg;
        this.clouds += other.clouds;

//        this.weatherId = other.weatherId;
//        this.main = other.main;
//        this.description = other.description;
    }

    @Override
    public void subtract(WeatherWritable other) {
        super.subtract(other);
        this.temp -= other.temp;
        this.feelsLike -= other.feelsLike;
        this.tempMin -= other.tempMin;
        this.tempMax -= other.tempMax;
        this.windSpeed -= other.windSpeed;
        this.rain1h -= other.rain1h;
        this.rain3h -= other.rain3h;
        this.snow1h -= other.snow1h;
        this.snow3h -= other.snow3h;

        this.pressure -= other.pressure;
        this.humidity -= other.humidity;
        this.windDeg -= other.windDeg;
        this.clouds -= other.clouds;

//        this.weatherId = other.weatherId;
//        this.main = other.main;
//        this.description = other.description;
    }

    @Override
    public void multiply(WeatherWritable other) {
        super.multiply(other);
        this.temp *= other.temp;
        this.feelsLike *= other.feelsLike;
        this.tempMin *= other.tempMin;
        this.tempMax *= other.tempMax;
        this.windSpeed *= other.windSpeed;
        this.rain1h *= other.rain1h;
        this.rain3h *= other.rain3h;
        this.snow1h *= other.snow1h;
        this.snow3h *= other.snow3h;

        this.pressure *= other.pressure;
        this.humidity *= other.humidity;
        this.windDeg *= other.windDeg;
        this.clouds *= other.clouds;

//        this.weatherId = other.weatherId;
//        this.main = other.main;
//        this.description = other.description;
    }

    @Override
    public void divide(WeatherWritable other) {
        super.divide(other);
        this.temp /= other.temp;
        this.feelsLike /= other.feelsLike;
        this.tempMin /= other.tempMin;
        this.tempMax /= other.tempMax;
        this.windSpeed /= other.windSpeed;
        this.rain1h /= other.rain1h;
        this.rain3h /= other.rain3h;
        this.snow1h /= other.snow1h;
        this.snow3h /= other.snow3h;

        this.pressure /= other.pressure;
        this.humidity /= other.humidity;
        this.windDeg /= other.windDeg;
        this.clouds /= other.clouds;

//        this.weatherId = other.weatherId;
//        this.main = other.main;
//        this.description = other.description;
    }

    @Override
    public void add(Number num) {
        super.add(num);
        float floatValue = num.floatValue();
        this.temp += floatValue;
        this.feelsLike += floatValue;
        this.tempMin += floatValue;
        this.tempMax += floatValue;
        this.windSpeed += floatValue;
        this.rain1h += floatValue;
        this.rain3h += floatValue;
        this.snow1h += floatValue;
        this.snow3h += floatValue;

        float intValue = num.intValue();
        this.pressure += intValue;
        this.humidity += intValue;
        this.windDeg += intValue;
        this.clouds += intValue;

//        this.weatherId = other.weatherId;
//        this.main = other.main;
//        this.description = other.description;
    }

    @Override
    public void subtract(Number num) {
        super.subtract(num);
        float floatValue = num.floatValue();
        this.temp -= floatValue;
        this.feelsLike -= floatValue;
        this.tempMin -= floatValue;
        this.tempMax -= floatValue;
        this.windSpeed -= floatValue;
        this.rain1h -= floatValue;
        this.rain3h -= floatValue;
        this.snow1h -= floatValue;
        this.snow3h -= floatValue;

        float intValue = num.intValue();
        this.pressure -= intValue;
        this.humidity -= intValue;
        this.windDeg -= intValue;
        this.clouds -= intValue;

//        this.weatherId = other.weatherId;
//        this.main = other.main;
//        this.description = other.description;
    }

    @Override
    public void multiply(Number num) {
        super.multiply(num);
        float floatValue = num.floatValue();
        this.temp *= floatValue;
        this.feelsLike *= floatValue;
        this.tempMin *= floatValue;
        this.tempMax *= floatValue;
        this.windSpeed *= floatValue;
        this.rain1h *= floatValue;
        this.rain3h *= floatValue;
        this.snow1h *= floatValue;
        this.snow3h *= floatValue;

        float intValue = num.intValue();
        this.pressure *= intValue;
        this.humidity *= intValue;
        this.windDeg *= intValue;
        this.clouds *= intValue;

//        this.weatherId = other.weatherId;
//        this.main = other.main;
//        this.description = other.description;
    }

    @Override
    public void divide(Number num) {
        super.divide(num);
        float floatValue = num.floatValue();
        this.temp /= floatValue;
        this.feelsLike /= floatValue;
        this.tempMin /= floatValue;
        this.tempMax /= floatValue;
        this.windSpeed /= floatValue;
        this.rain1h /= floatValue;
        this.rain3h /= floatValue;
        this.snow1h /= floatValue;
        this.snow3h /= floatValue;

        float intValue = num.intValue();
        this.pressure /= intValue;
        this.humidity /= intValue;
        this.windDeg /= intValue;
        this.clouds /= intValue;

//        this.weatherId = other.weatherId;
//        this.main = other.main;
//        this.description = other.description;

    }

    @Override
    public void min(WeatherWritable other) {

    }

    @Override
    public void max(WeatherWritable other) {

    }

    @Override
    public WeatherWritable copyOf() {
        WeatherWritable other = new WeatherWritable();
        other.set(this);
        return other;
    }

    @Override
    public Optional<Value> getField(String field) {
        Optional<Value> value = super.getField(field);
        if (value.isEmpty()) {
            switch (field) {
                case TEMP_PROP:          value = Value.ofOptional(temp);        break;
                case FEELS_LIKE_PROP:    value = Value.ofOptional(feelsLike);   break;
                case TEMP_MIN_PROP:      value = Value.ofOptional(tempMin);     break;
                case TEMP_MAX_PROP:      value = Value.ofOptional(tempMax);     break;
                case WIND_SPEED_PROP:    value = Value.ofOptional(windSpeed);   break;
                case RAIN_1H_PROP:       value = Value.ofOptional(rain1h);      break;
                case RAIN_3H_PROP:       value = Value.ofOptional(rain3h);      break;
                case SNOW_1H_PROP:       value = Value.ofOptional(snow1h);      break;
                case SNOW_3H_PROP:       value = Value.ofOptional(snow3h);      break;

                case PRESSURE_PROP:      value = Value.ofOptional(pressure);    break;
                case HUMIDITY_PROP:      value = Value.ofOptional(humidity);    break;
                case WIND_DEG_PROP:      value = Value.ofOptional(windDeg);     break;
                case CLOUDS_ALL_PROP:    value = Value.ofOptional(clouds);      break;
                case WEATHER_ID_PROP:    value = Value.ofOptional(weatherId);   break;

                case WEATHER_MAIN_PROP:  value = Value.ofOptional(weatherMain);        break;
                case WEATHER_DESC_PROP:  value = Value.ofOptional(weatherDescription); break;
                default:                 value = Value.empty();                 break;
            }
        }
        return value;
    }

    @Override
    public boolean setField(String field, Object value) {
        AtomicBoolean set = new AtomicBoolean(super.setField(field, value));
        if (!set.get()) {
            Value.ifFloat(value, v -> {
                set.set(true);
                switch (field) {
                    case TEMP_PROP:          setTemp(v);        break;
                    case FEELS_LIKE_PROP:    setFeelsLike(v);   break;
                    case TEMP_MIN_PROP:      setTempMin(v);     break;
                    case TEMP_MAX_PROP:      setTempMax(v);     break;
                    case WIND_SPEED_PROP:    setWindSpeed(v);   break;
                    case RAIN_1H_PROP:       setRain1h(v);      break;
                    case RAIN_3H_PROP:       setRain3h(v);      break;
                    case SNOW_1H_PROP:       setSnow1h(v);      break;
                    case SNOW_3H_PROP:       setSnow3h(v);      break;
                    default:                 set.set(false);    break;
                }
            });
            if (!set.get()) {
                Value.ifInteger(value, v -> {
                    set.set(true);
                    switch (field) {
                        case PRESSURE_PROP:      setPressure(v);    break;
                        case HUMIDITY_PROP:      setHumidity(v);    break;
                        case WIND_DEG_PROP:      setWindDeg(v);     break;
                        case CLOUDS_ALL_PROP:    setClouds(v);      break;
                        case WEATHER_ID_PROP:    setWeatherId(v);   break;
                        default:                 set.set(false);    break;
                    }
                });
                if (!set.get()) {
                    Value.ifString(value, v -> {
                        set.set(true);
                        switch (field) {
                            case WEATHER_MAIN_PROP:
                                setWeatherMain(v);
                                break;
                            case WEATHER_DESC_PROP:
                                setWeatherDescription(v);
                                break;
                            default:
                                set.set(false);
                                break;
                        }
                    });
                }
            }
        }
        return set.get();
    }

    @Override
    public List<String> getFieldsList() {
        return FIELDS;
    }

    public static void ifInstance(Object value, Consumer<WeatherWritable> action) {
        if (isInstance(value)) {
            action.accept((WeatherWritable)value);
        }
    }

    public static boolean isInstance(Object value) {
        return (value instanceof WeatherWritable);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
            super.toString() +
            ", temp=" + temp +
            ", feelsLike=" + feelsLike +
            ", tempMin=" + tempMin +
            ", tempMax=" + tempMax +
            ", pressure=" + pressure +
            ", humidity=" + humidity +
            ", windSpeed=" + windSpeed +
            ", windDeg=" + windDeg +
            ", rain1h=" + rain1h +
            ", rain3h=" + rain3h +
            ", snow1h=" + snow1h +
            ", snow3h=" + snow3h +
            ", clouds=" + clouds +
            ", weatherId=" + weatherId +
            ", weatherMain='" + weatherMain + '\'' +
            ", weatherDescription='" + weatherDescription + '\'' +
            '}';
    }

    public static WeatherWritableBuilder getBuilder() {
        return new WeatherWritableBuilder();
    }


    public static class WeatherWritableBuilder extends AbstractBaseWritableBuilder<WeatherWritableBuilder, WeatherWritable> {

        private static final Logger logger = Logger.getLogger(WeatherWritableBuilder.class);

        protected WeatherWritableBuilder() {
            super(logger);
        }

        public WeatherWritableBuilder setTemp(String temp) {
            getWritable().setTemp(getFloat(temp));
            return getThis();
        }

        public WeatherWritableBuilder setFeelsLike(String feelsLike) {
            getWritable().setFeelsLike(getFloat(feelsLike));
            return getThis();
        }

        public WeatherWritableBuilder setTempMin(String tempMin) {
            getWritable().setTempMin(getFloat(tempMin));
            return getThis();
        }

        public WeatherWritableBuilder setTempMax(String tempMax) {
            getWritable().setTempMax(getFloat(tempMax));
            return getThis();
        }

        public WeatherWritableBuilder setPressure(String pressure) {
            getWritable().setPressure(getInt(pressure));
            return getThis();
        }

        public WeatherWritableBuilder setHumidity(String humidity) {
            getWritable().setHumidity(getInt(humidity));
            return getThis();
        }

        public WeatherWritableBuilder setWindSpeed(String windSpeed) {
            getWritable().setWindSpeed(getFloat(windSpeed));
            return getThis();
        }

        public WeatherWritableBuilder setWindDeg(String windDeg) {
            getWritable().setWindDeg(getInt(windDeg));
            return getThis();
        }

        public WeatherWritableBuilder setRain1h(String rain1h) {
            getWritable().setRain1h(getFloat(rain1h));
            return getThis();
        }

        public WeatherWritableBuilder setRain3h(String rain3h) {
            getWritable().setRain3h(getFloat(rain3h));
            return getThis();
        }

        public WeatherWritableBuilder setSnow1h(String snow1h) {
            getWritable().setSnow1h(getFloat(snow1h));
            return getThis();
        }

        public WeatherWritableBuilder setSnow3h(String snow3h) {
            getWritable().setSnow3h(getFloat(snow3h));
            return getThis();
        }

        public WeatherWritableBuilder setClouds(String clouds) {
            getWritable().setClouds(getInt(clouds));
            return getThis();
        }

        public WeatherWritableBuilder setWeatherId(String weatherId) {
            getWritable().setWeatherId(getInt(weatherId));
            return getThis();
        }

        public WeatherWritableBuilder setWeatherMain(String main) {
            getWritable().setWeatherMain(main);
            return getThis();
        }

        public WeatherWritableBuilder setWeatherDescription(String description) {
            getWritable().setWeatherDescription(description);
            return getThis();
        }

        @Override
        public WeatherWritableBuilder getThis() {
            return this;
        }

        @Override
        public WeatherWritable getNewWritable() {
            return new WeatherWritable();
        }

        @Override
        public WeatherWritable build() {
            return getWritable();
        }
    }


}
