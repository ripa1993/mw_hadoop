package accidentsBorough;

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
 * Maps [TEXT] -> [(Week, Year, Borough), 1 (letal) or 0 (non letal)]
 * @author Simone Ripamonti
 *
 */
public class AccidentsBoroughMapper extends MapReduceBase
		implements Mapper<LongWritable, Text, WeekBoroughWritable, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<WeekBoroughWritable, IntWritable> output,
			Reporter reporter) throws IOException {
		// skip header of csv file
		if (key.get() == 0)
			return;
		String line = value.toString();
		String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		// discard not legal lines
		if (parts.length < 29)
			return;
		// count casualties
		int killed = Integer.valueOf(parts[CSV.NUMBER_OF_PERSONS_KILLED]);
		// get borough
		String borough = parts[CSV.BOROUGH];
		// discard missing borough
		if (borough.length()==0)
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
		output.collect(new WeekBoroughWritable(week, year, borough), new IntWritable(killed>0?1:0));
		// value is 1 if accident is letal, 0 otherwise
	}

}
