import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class WordAverageMapper extends Mapper<Object, BSONObject, Text, IntWritable> {
    @Override
    public void map(final Object key, final BSONObject value, final Context context)
            throws IOException, InterruptedException {
        String body = value.get("cleaned").toString();
        for (String word : body.split("\\s+")) {
            Text keyOut = new Text(word);
            IntWritable valueOut = new IntWritable(Integer.parseInt(value.get("grade").toString()));
            context.write(keyOut, valueOut);
        }
    }
}
