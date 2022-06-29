package Transactions;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TransactionsMapper extends Mapper<Object,
        Text, Text, LongWritable> {
    private TreeMap<Long, String> tmap;
    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
        tmap = new TreeMap<Long, String>();
    }
    @Override
    public void map(Object key, Text value,
                    Context context) throws IOException,
            InterruptedException
    {
        String[] tokens = value.toString().split("/");

        String insider_name = tokens[0];
        long values = Long.parseLong(tokens[6]);
        tmap.put(values, insider_name);
        if (tmap.size() > 10)
        {
            tmap.remove(tmap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException
    {
        for (Map.Entry<Long, String> entry : tmap.entrySet())
        {
            long count = entry.getKey();
            String name = entry.getValue();
            context.write(new Text(name), new LongWritable(count));
        }
    }
}