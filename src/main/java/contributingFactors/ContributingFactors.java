package contributingFactors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

/**
 * number of accidents and percentage of number of deaths per contributing
 * factor in the dataset (e.g., let's take drinking and driving -> I want to
 * know how many accidents were due to this cause and what percentage of these
 * accidents were also lethal)
 * 
 * @author Simone Ripamonti
 *
 */
public class ContributingFactors extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		JobConf job = new JobConf(conf, ContributingFactors.class);
		job.setJobName("ContributingFactors");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ContributingFactorsMapper.class);
		job.setReducerClass(ContributingFactorsReducer.class);

		job.setInputFormat(TextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IncidentsAndDeathsWritable.class);

		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new ContributingFactors(), args);
		System.exit(exitCode);
	}

}
