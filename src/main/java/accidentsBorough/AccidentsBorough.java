package accidentsBorough;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * number of accidents and average number of lethal accidents per week per
 * borough (e.g., let's take burough number 1 -> I want to know how many
 * accidents there were in burough 1 each week, as well as the average number of
 * lethal accidents that the burough had per week)
 * 
 * @author Simone Ripamonti
 *
 */
public class AccidentsBorough extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		// FIRST PART

		JobConf jobConf1 = new JobConf(conf, AccidentsBorough.class);
		jobConf1.setJobName("AccidentsBorough");

		// input
		FileInputFormat.setInputPaths(jobConf1, in);
		// output
		FileOutputFormat.setOutputPath(jobConf1, out);

		jobConf1.setMapperClass(AccidentsBoroughMapper.class);
		jobConf1.setReducerClass(AccidentsBoroughReducer.class);

		jobConf1.setInputFormat(TextInputFormat.class);

		jobConf1.setMapOutputKeyClass(WeekBoroughWritable.class);
		jobConf1.setMapOutputValueClass(IntWritable.class);

		jobConf1.setOutputFormat(TextOutputFormat.class);
		jobConf1.setOutputKeyClass(WeekBoroughWritable.class);
		jobConf1.setOutputValueClass(AccidentsAndLethalAccidents.class);

		JobClient.runJob(jobConf1);
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new AccidentsBorough(), args);
		System.exit(exitCode);
	}

}
