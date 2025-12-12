package org.weather.mapreduce.HighestPrecipitationJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighestPrecipitationReducer extends Reducer<Text, Text, Text, Text> {
    private int highestPrecipitationHours = Integer.MIN_VALUE;
    private String hightestMonthYear = "";
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int totalPrecipitation = 0;

        for (Text value : values) {
            try{
                int precipitation = Integer.parseInt(value.toString());
                totalPrecipitation += precipitation;
            } catch (NumberFormatException e){
                context.getCounter("REDUCE_ERROR", "PARSE_ERROR").increment(1);
            }
        }

        if (totalPrecipitation > highestPrecipitationHours) {
            highestPrecipitationHours = totalPrecipitation;
            hightestMonthYear = key.toString();
        }
        context.getCounter("REDUCE_PROGRESS", "MONTHS_PROCESSED").increment(1);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (highestPrecipitationHours != Integer.MIN_VALUE) {
            outKey.set(hightestMonthYear);
            outValue.set(highestPrecipitationHours + "");
            context.write(outKey, outValue);

            context.getCounter("FINAL_RESULT", "HIGHEST_PRECIPITATION_MONTH").increment(1);
        }
        super.cleanup(context);
    }
}
