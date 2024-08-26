import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class KruskalMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parse input line to extract source vertex, destination vertex, and weight
        String[] tokens = value.toString().split("\\s+");
        String source = tokens[0];
        String destination = tokens[1];
        String weight = tokens[2];
        
        // Emit the weight as key and the edge (source-destination) as value
        context.write(new Text(weight), new Text(source + "-" + destination));
    }
}

