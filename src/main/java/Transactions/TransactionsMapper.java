package Transactions;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
public class TransactionsMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable revenueWriteable = new IntWritable(0);
    private Text cityText = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        System.out.println("This is the content of line");
        System.out.println(line);

        String[] tokens = line.split("\t");
        String insider_name = tokens[0].trim();
        if(tokens[6].matches("\\d+\\.\\d+")) {
            int values = (int) Double.parseDouble(tokens[6]);
            this.cityText.set(insider_name);
            this.revenueWriteable.set(values);
            output.collect(this.cityText, this.revenueWriteable);
        }
    }
}
