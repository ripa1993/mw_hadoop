package lethalAccidents;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import common.CSV;

/**
 * Maps [TEXT] -> [(Week, Year), 1]
 */
public class LethalAccidentsMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, WeekYearWritable, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<WeekYearWritable, IntWritable> output, Reporter reporter)
			throws IOException {
		// skip header of csv file
		if(key.get()==0)
			return;
		String line = value.toString();
		String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		// discard not legal lines
		if(parts.length < 29)
			return;
		// consider only lethal accidents
		int killed = Integer.valueOf(parts[CSV.NUMBER_OF_PERSONS_KILLED]);
		if (killed == 0)
			return;
		// obtain week number and year
		Date date = new Date();
		String format = "MM/dd/yyyy";
		SimpleDateFormat df = new SimpleDateFormat(format);
		try {
			date = df.parse(parts[CSV.DATE]);
		} catch (ParseException e) {
			// cannot understand date!
			return;
		}
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		int year = cal.get(Calendar.YEAR);
		output.collect(new WeekYearWritable(week, year), new IntWritable(1));
	}

}
