package accidentsBorough;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import lethalAccidents.LethalAccidents;

public class AccidentsBorough extends Configured implements Tool {

	public int run(String[] args) throws Exception {
Configuration conf = getConf();
		
        JobConf job = new JobConf(conf, LethalAccidents.class);
        job.setJobName("AccidentsBorough");
        
        // input
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(AccidentsBoroughMapper.class);
        job.setReducerClass(AccidentsBoroughReducer.class);
        
        job.setInputFormat(TextInputFormat.class);
        
        job.setOutputFormat(TextOutputFormat.class);
        // week
        job.setOutputKeyClass(WeekBoroughWritable.class);
        // number of accidents
		job.setOutputValueClass(AvgAccidentsAndLetalWritable.class);

		JobClient.runJob(job);
		
		
		
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new AccidentsBorough(), args);
		System.exit(exitCode);
	}
	
}
