package accidentsBorough;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AccidentsBorough2 extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		

//		// SECOND PART

		JobConf jobConf2 = new JobConf(conf, AccidentsBorough2.class);
		jobConf2.setJobName("AccidentsBorough2");
		// input
		FileInputFormat.setInputPaths(jobConf2, in);
		// output
		FileOutputFormat.setOutputPath(jobConf2, out);
		jobConf2.setMapperClass(AccidentsBoroughMapper2.class);
		jobConf2.setReducerClass(AccidentsBoroughReducer2.class);

		jobConf2.setInputFormat(TextInputFormat.class);

		jobConf2.setMapOutputKeyClass(Text.class);
		jobConf2.setMapOutputValueClass(IntWritable.class);

		jobConf2.setOutputFormat(TextOutputFormat.class);
		jobConf2.setOutputKeyClass(Text.class);
		jobConf2.setOutputValueClass(DoubleWritable.class);


		JobClient.runJob(jobConf2);
		return 0;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new AccidentsBorough2(), args);
		System.exit(exitCode);
	}

}
