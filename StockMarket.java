package com.training.avro.mapreduce;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.training.avro.IntPair;
import com.training.avro.stock_market;

public class StockMarketDriver extends Configured implements Tool{

	public static class StockMarketMapper extends
	Mapper<AvroKey<stock_market>, NullWritable, Text, IntPair> {
		protected void map(AvroKey<student_marks> key, NullWritable value, Context context)
				throws IOException, InterruptedException {
			Text symbol = new Text(key.datum().getSymbol());
			IntPair one_price = new IntPair((int)(key.datum().getPrice()*10), 1);
			context.write(symbol, one_price);
		}
	} // end of mapper class

	public static class StockMarketCombiner extends
	Reducer<Text, IntPair, Text, IntPair> {
		IntPair p_sum_count = new IntPair();
		Integer p_sum = new Integer(0);
		Integer p_count = new Integer(0);
		protected void reduce(IntWritable key, Iterable<IntPair> values, Context context)
				throws IOException, InterruptedException {
			p_sum = 0;
			p_count = 0;
			for (IntPair value : values) {
				p_sum += value.getFirstInt();
				p_count += value.getSecondInt();
			}
			p_sum_count.set(p_sum, p_count);
			context.write(key, p_sum_count);
		}
	} // end of combiner class

	public static class StockMarketReducer extends
	Reducer<Text, IntPair, AvroKey<String>, AvroValue<Float>> {
		Integer f_sum = 0;
		Integer f_count = 0;

		protected void reduce(Text key, Iterable<IntPair> values, Context context)
				throws IOException, InterruptedException {
			f_sum = 0;
			f_count = 0;
			for (IntPair value : values) {
				f_sum += value.getFirstInt();
				f_count += value.getSecondInt();
			}
			Float average = (float)(f_sum/(f_count*10));
			Integer symbol = new Integer(key.toString());
			context.write(new AvroKey<String>(symbol), new AvroValue<Float>(average));
		}
	} // end of reducer class

	@Override
	public int run(String[] rawArgs) throws Exception {
		if (rawArgs.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = new Job(super.getConf());
		job.setJarByClass(StockMarketDriver.class);
		job.setJobName("Stock Market Prices Average");

		String[] args = new GenericOptionsParser(rawArgs).getRemainingArgs();
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1]);

		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(super.getConf()).delete(outPath, true);

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(StockMarketMapper.class);
		AvroJob.setInputKeySchema(job, student_marks.getClassSchema());
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntPair.class);

		job.setCombinerClass(StockMarketCombiner.class);

		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(StockMarketReducer.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.FLOAT));

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new StockMarketDriver(), args);
		System.exit(result);
	}
}