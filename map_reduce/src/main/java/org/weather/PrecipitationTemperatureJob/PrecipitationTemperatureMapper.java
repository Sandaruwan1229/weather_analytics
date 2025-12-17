package org.weather.PrecipitationTemperatureJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;

import static org.weather.utils.WeatherJobUtils.getYearMonthKey;
import static org.weather.utils.WeatherJobUtils.getYearKey;
import static org.weather.utils.WeatherJobUtils.isValidValue;

public class PrecipitationTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("location_id")) return; // skip header
        String[] fields = line.split(",");

        if (fields.length < 14) {
            System.err.println("SKIPPED BAD LINE: " + line);
            return;
        }

        String locationId = fields[0];
        String date = fields[1];
        String tempMean = fields[5];
        String precipitationHours = fields[13];

        if(!isValidValue(locationId) ||  !isValidValue(date) ||
                !isValidValue(tempMean) || !isValidValue(precipitationHours)) {
            return;
        }

        try {
            String yearMonth = getYearMonthKey(date);
            int yearKey = getYearKey(date);

            if(yearKey >=2014){
                double precipitation = Double.parseDouble(precipitationHours);
                double temperature = Double.parseDouble(tempMean);

                outKey.set(locationId+","+yearMonth);
                outValue.set(precipitation+","+temperature+",1");
                context.write(outKey, outValue);
            }

        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
