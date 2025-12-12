package org.weather.mapreduce.PrecipitationTemperatureJob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class PrecipitationTemperatureReducer extends Reducer<Text, Text, Text, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double totalPrecipitation = 0;
        double totalTemperature = 0;
        int count = 0;

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            System.err.println("DEBUG: key: " + key.toString() + " fields: " + Arrays.toString(parts));

            if (parts.length == 3) {
                try {
                    double precipitation = Double.parseDouble(parts[0]);
                    double temperature = Double.parseDouble(parts[1]);
                    int recordCount = Integer.parseInt(parts[2]);

                    totalPrecipitation += precipitation;
                    totalTemperature += temperature;
                    count += recordCount;
                } catch (NumberFormatException e) {
                    context.getCounter("REDUCE_ERROR", "PARSE_ERROR").increment(1);
                }
            }
        }

        if (count > 0) {
            double meanTemperature = totalTemperature / count;

            outValue.set(String.format("%.2f,%.2f", totalPrecipitation,meanTemperature));
            context.write(key,outValue);
        }
    }

}
