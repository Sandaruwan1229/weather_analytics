package org.weather.PrecipitationTemperatureJob;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PrecipitationTemperatureWithLocationReducer extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();
    private Map<String, String> locationMap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        String location = context.getConfiguration().get("location.file.path");
        if (location == null) {
            throw new IOException("location.file.path is null");
        }

        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(location);

        try(BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length > 2) {
                    String locationId = fields[0].trim();
                    String districtName = fields[7].trim();
                    locationMap.put(locationId, districtName);
                    System.out.println("DEBUG: locationId: " + locationId + " districtName: " + districtName);
                }
            }
        }
        System.out.println("Loaded " + locationMap.size() + " districts");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double totalPrecipitation = 0.0;
        double totalTemperature = 0.0;
        int count = 0;

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            System.err.println("DEBUG: key: " + key.toString() + " fields: " + Arrays.toString(fields));

            if (fields.length == 2) {
                try{
                    double precipitation = Double.parseDouble(fields[0]);
                    double temperature = Double.parseDouble(fields[1]);
                    totalPrecipitation += precipitation;
                    totalTemperature += temperature;
                    count ++;
                } catch (NumberFormatException e){
                    context.getCounter("REDUCER_ERRORS", "PARSE_ERROR").increment(1);
                }
            }
        }

        if (count > 0) {
            String keyStr =  key.toString();
            String[] keyParts = keyStr.split(",");
            if (keyParts.length == 2) {
                String locationId = keyParts[0].trim();
                String yearMonth = keyParts[1];

                String districtName = locationMap.getOrDefault(locationId, "Unknown");
                double meanTemperature = totalTemperature / count;

                outKey.set(districtName+"|"+yearMonth);
                outValue.set(String.format("%.2f,%.2f", totalPrecipitation,meanTemperature));
                context.write(outKey,outValue);
            }
        }
    }
}
