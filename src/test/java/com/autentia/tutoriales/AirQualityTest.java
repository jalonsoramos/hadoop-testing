package com.autentia.tutoriales;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class AirQualityTest {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
	private MapReduceDriver mapReduceDriver;

	private final AirQualityMapper mapper = new AirQualityMapper();
	private final AirQualityReducer reducer = new AirQualityReducer();

	@Before
	public void setUp() {
		mapDriver = new MapDriver(mapper);
		reduceDriver = new ReduceDriver(reducer);
		mapReduceDriver = new MapReduceDriver(mapper, reducer);
	}

	@Test
	public void shouldReturnCOValueForProvinceOnlyWhenValueIsANumber() throws IOException {
		mapDriver.withInput(new LongWritable(0), new Text("01/01/1997;1.2;12;33;63;56;;;;19;ÁVILA;Ávila"));
		mapDriver.withOutput(new Text("ÁVILA"), new DoubleWritable(1.2));

		mapDriver.runTest();
	}

	@Test
	public void shouldReturnEmptyResultMapWhenValueIsNotNumber() throws IOException {
		mapDriver.withInput(new LongWritable(0), new Text("DIA;CO (mg/m3);NO (ug/m3);NO2 (ug/m3);O3 (ug/m3);PM10 (ug/m3);SH2 (ug/m3);PM25 (ug/m3);PST (ug/m3);SO2 (ug/m3);PROVINCIA;ESTACIÓN"));

		Assert.assertTrue(mapDriver.run().isEmpty());
	}

	@Test
	public void shouldReturnTheAverageOfValuesByProvince() throws IOException {
		reduceDriver.withInput(new Text("ÁVILA"), new ArrayList<DoubleWritable>() {
			{
				add(new DoubleWritable(1.5));
				add(new DoubleWritable(1.4));
				add(new DoubleWritable(1.6));
			}
		});

		reduceDriver.withOutput(new Text("ÁVILA"), new Text("1.5"));

		reduceDriver.runTest();
	}

	@Test
	public void shouldReturnEmptyResultReduceWhenValuesListIsEmpty() throws IOException {
		reduceDriver.withInput(new Text("ÁVILA"), new ArrayList<DoubleWritable>());

		Assert.assertTrue(reduceDriver.run().isEmpty());
	}

	@Test
	public void shouldMapInputLinesAndReduceByCOAndProvince() throws IOException {
		mapReduceDriver
				.withInput(new LongWritable(1), new Text("01/01/1997;1.2;12;33;63;56;;;;19;ÁVILA;Ávila"))
				.withInput(new LongWritable(2), new Text("22/04/1997;1.1;45;49;75;No cumple el anexo IV...;;;;16;ÁVILA;Ávila"))
				.withInput(new LongWritable(3), new Text("24/12/2003;1.6;27;22;45;25;4;;;6;BURGOS;Miranda de Ebro 1"))
				.withInput(new LongWritable(4), new Text("25/12/2003;2.2;35;13;53;28;21;;;7;BURGOS;Miranda de Ebro 1"))
				.withInput(
						new LongWritable(5),
						new Text(
								"26/12/2003;No cumple el anexo IV...;No cumple el anexo IV...;No cumple el anexo IV...;No cumple el anexo IV...;No cumple el anexo IV...;No cumple el anexo IV...;;;No cumple el anexo IV...;BURGOS;Miranda de Ebro 1"))
				.withInput(new LongWritable(6), new Text("27/12/2003; 0.1;18;15;45;30;24;;;7;BURGOS;Miranda de Ebro 1"))

				.withOutput(new Text("BURGOS"), new Text("1.3")).withOutput(new Text("ÁVILA"), new Text("1.15"))

				.runTest();
	}
}
