import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private final String testWRONG_NUMBER_OF_FIELDS = "24,hello,test";
    private final String testSEVERITY_ERROR = "8,Jul 27 08:33:39,root,pycharm:,starts";
    private final String testValidLog = "5,Jul 27 08:33:39,root,pycharm:,starts";
    private final String testValidLogOutput = "Jul 27 08, 5";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapperCounterWNFOne() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testWRONG_NUMBER_OF_FIELDS))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters().
                findCounter(CounterType.WRONG_NUMBER_OF_FIELDS).getValue());
    }

    @Test
    public void testMapperCounterWNFZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testValidLog))
                .withOutput(new Text(testValidLogOutput), new IntWritable(1))
                .runTest();
        assertEquals("Expected 0 counter increments", 0, mapDriver.getCounters()
                .findCounter(CounterType.WRONG_NUMBER_OF_FIELDS).getValue());
    }

    @Test
    public void testMapperCounterSeverityErrorOne() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testSEVERITY_ERROR))
                .runTest();
        assertEquals("Expexted 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.SEVERITY_ERROR).getValue());
    }

    @Test
    public void testMapperCounterSeverityErrorZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testValidLog))
                .withOutput(new Text(testValidLogOutput), new IntWritable(1))
                .runTest();
        assertEquals("Expected 0 counter increments", 0, mapDriver.getCounters()
                .findCounter(CounterType.SEVERITY_ERROR).getValue());
    }

    @Test
    public void testMapperCounterWNF() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testValidLog))
                .withInput(new LongWritable(), new Text(testSEVERITY_ERROR))
                .withInput(new LongWritable(), new Text(testWRONG_NUMBER_OF_FIELDS))
                .withInput(new LongWritable(), new Text(testWRONG_NUMBER_OF_FIELDS))
                .withOutput(new Text(testValidLogOutput), new IntWritable(1))
                .runTest();
        assertEquals("Expected 2 counter increments", 2, mapDriver.getCounters()
                .findCounter(CounterType.WRONG_NUMBER_OF_FIELDS).getValue());
    }

    @Test
    public void testMapperCounterSeverityError() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testValidLog))
                .withInput(new LongWritable(), new Text(testSEVERITY_ERROR))
                .withInput(new LongWritable(), new Text(testWRONG_NUMBER_OF_FIELDS))
                .withInput(new LongWritable(), new Text(testWRONG_NUMBER_OF_FIELDS))
                .withInput(new LongWritable(), new Text(testSEVERITY_ERROR))
                .withOutput(new Text(testValidLogOutput), new IntWritable(1))
                .runTest();
        assertEquals("Expected 2 counter increments", 2, mapDriver.getCounters()
                .findCounter(CounterType.SEVERITY_ERROR).getValue());
    }
}

