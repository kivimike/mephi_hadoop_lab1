package bdtc.lab1;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * Функция переопределяет функцию map.
         * Строка логов разделяется по запятым, после чего осуществляется проверка на число полей. В случае, если число
         * полей отлично от 5, то инкрементируется счетчик WRONG_NUMBER_OF_FIELDS.
         * Далее проверяется значение severity на валидность, в случае провала инкрментируется счетчик SEVERITY_ERROR
         * Затем осуществляется подмена ключа (роль которого играет дата и время лога, а также уровень severity) с MMM d hh:mm:ss на MMM d hh,
         * засчет чего достигается группировка логов по дате и часу.
         * на стадию Reduce передается ключ (пример: Jul 27 08, 5 для лога 5,Jul 27 08:33:39,root,pycharm:,starts")
        * */
        String line = value.toString();

        String[] log_fields = line.split(",");
        if (log_fields.length != 5){
            context.getCounter(CounterType.WRONG_NUMBER_OF_FIELDS).increment(1);
        } else {
            String level = log_fields[0];
            if (level.length() == 1) {
                int lvl = Integer.parseInt(level);
                if (lvl >= 0 && lvl < 8){
                    String raw_date = log_fields[1];
                    String from_format = "MMM d hh:mm:ss";
                    String to_format = "MMM d hh', '";
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(from_format);
                    SimpleDateFormat simpleDateFormat_to = new SimpleDateFormat(to_format);
                    Date date = null;
                    try {
                        date = simpleDateFormat.parse(raw_date);
                        String date_out = simpleDateFormat_to.format(date);
                        String new_key = date_out + level;
                        word.set(new_key);
                        context.write(word, one);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                } else {
                    context.getCounter(CounterType.SEVERITY_ERROR).increment(1);
                }
            } else {
                context.getCounter(CounterType.SEVERITY_ERROR).increment(1);
            }
        }
    }
}
