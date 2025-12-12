package org.weather.HighestPrecipitationJob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;

import static org.weather.utils.WeatherJobUtils.getYearMonthKey;
import static org.weather.utils.WeatherJobUtils.isValidValue;

public class HighestPrecipitationMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException,  InterruptedException {

        String line = value.toString();
        if (line.startsWith("location_id")) return; // skip header
        String[] fields = line.split(",");

        String date = fields[1];
        String precipitationHours = fields[13];

        if(!isValidValue(date) || !isValidValue(precipitationHours)) {
            return;
        }

        try {
            String yearMonth = getYearMonthKey(date);
            int precipitation = Integer.parseInt(precipitationHours);

            outKey.set(yearMonth);
            outValue.set(String.valueOf(precipitation));
            context.write(outKey, outValue);

        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
