package org.weather.mapreduce.PrecipitationTemperatureJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrecipitationTemperatureJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PrecipitationTemperatureJob <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("location.file.path", args[2]);

        Job job = Job.getInstance(conf, "Weather District Monthly");
        job.setJarByClass(PrecipitationTemperatureJob.class);

        job.setMapperClass(PrecipitationTemperatureMapper.class);
        job.setCombinerClass(PrecipitationTemperatureReducer.class);
        job.setReducerClass(PrecipitationTemperatureWithLocationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
