package com.autentia.tutoriales;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestReducer extends Reducer<Text, IntWritable, Text, Text> {

	private final DecimalFormat decimalFormat = new DecimalFormat("#.##");

	public void reduce(Text key, Iterable<IntWritable> coValues, Context context) throws IOException, InterruptedException {

			context.write(key, new Text(""));
	}
}