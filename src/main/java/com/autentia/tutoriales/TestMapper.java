package com.autentia.tutoriales;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TestMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final String SEPARATOR = ";";

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] values = value.toString().split(SEPARATOR);
		final String province = format(values[10]);

		context.write(new Text("AVILA"), new IntWritable(1));
	}

	private String format(String value) {
		return value.trim();
	}
}