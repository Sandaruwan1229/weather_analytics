package org.weather.HighestPrecipitationJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestPrecipitationCombiner extends Reducer<Text, Text, Text, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int totalPrecipitationHours = 0;

        for (Text value : values) {
            try{
                int precipitationHours = Integer.parseInt(value.toString());
                totalPrecipitationHours += precipitationHours;
            } catch (NumberFormatException e){
                context.getCounter("REDUCE_ERROR", "PARSE_ERROR").increment(1);
            }
        }

        outValue.set(totalPrecipitationHours + "");
        context.write(key, outValue);
    }
}
