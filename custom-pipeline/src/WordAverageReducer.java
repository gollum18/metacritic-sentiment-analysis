import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

import java.io.IOException;

public class WordAverageReducer extends Reducer<Text, IntWritable, NullWritable, BSONWritable> {
    @Override
    public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws
            IOException, InterruptedException {
        long n = 0;
        long sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
            n++;
        }

        double avg = 0;
        if (n == 0) {
            avg = 0;
        } else {
            avg = sum / ((double) n);
        }

        BSONWritable valueOut = new BSONWritable();
        BSONObject outDoc = BasicDBObjectBuilder.start()
                .add("word", key.toString())
                .add("avg", avg)
                .add("n", n)
                .get();
        valueOut.setDoc(outDoc);

        context.write(null, valueOut);
    }
}
