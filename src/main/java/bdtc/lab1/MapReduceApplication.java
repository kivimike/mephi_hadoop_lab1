package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {
        /**
        * Точка входа в программу. По умолчанию output формат - csv-файл. Для того, чтобы сменить формат на SequenceFile
         * со snappy сжатием необходимо при вызове программы задать третий параметр snappy
        * */
        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders!");
        }
        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Log count");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        if (args.length == 3 && args[2].equals("snappy") ){
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
            SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        } else {
            job.setOutputFormatClass(TextOutputFormat.class);
            Path outputDirectory = new Path(args[1]);
            FileOutputFormat.setOutputPath(job, outputDirectory);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter fields_counter = job.getCounters().findCounter(CounterType.WRONG_NUMBER_OF_FIELDS);
        Counter severity_counter = job.getCounters().findCounter(CounterType.SEVERITY_ERROR);
        log.info("=====================COUNTERS " + fields_counter.getName() + ": " + fields_counter.getValue() + "=====================");
        log.info("=====================COUNTERS " + severity_counter.getName() + ": " + severity_counter.getValue() + "=====================");
    }
}
