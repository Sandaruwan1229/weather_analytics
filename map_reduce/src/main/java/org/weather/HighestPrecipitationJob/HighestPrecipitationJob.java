package org.weather.HighestPrecipitationJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestPrecipitationJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HighestPrecipitationJob <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "Highest Precipitation");
        job.setJarByClass(HighestPrecipitationJob.class);

        job.setMapperClass(HighestPrecipitationMapper.class);
        job.setCombinerClass(HighestPrecipitationCombiner.class);
        job.setReducerClass(HighestPrecipitationReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        if(result) {
            System.out.println("Job Finished!");
        }else{
            System.out.println("Job Failed!");
        }
        System.exit(result ? 0 : 1);
    }
}
