import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;

public class SentimentAnalyzerReducer extends Reducer<Text, Text, NullWritable, MongoUpdateWritable> {

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException{
        BasicBSONObject query = new BasicBSONObject("_id", new ObjectId(key.toString()));
        BasicBSONObject modifiers = new BasicBSONObject();
        double score = 0;
        int n = 0;
        for (Text avgStr : values) {
            double x = Double.parseDouble(avgStr.toString());
            score = (x + n * score) / (n + 1);
            n++;
        }

        modifiers.put("$set", BasicDBObjectBuilder.start()
                                                  .add("sentiment_score", score)
                                                  .get());

        MongoUpdateWritable outputValue = new MongoUpdateWritable();
        outputValue.setQuery(query);
        outputValue.setModifiers(modifiers);

        context.write(null, outputValue);
    }
}
