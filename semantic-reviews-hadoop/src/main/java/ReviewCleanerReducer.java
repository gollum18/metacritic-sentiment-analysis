import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.io.IOException;

public class ReviewCleanerReducer extends Reducer<Text, Text, NullWritable, MongoUpdateWritable> {
    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
        BasicBSONObject query = new BasicBSONObject("_id", new ObjectId(key.toString()));
        BasicBSONObject modifiers = new BasicBSONObject();
        Text cleaned = values.iterator().next();
        modifiers.put("$set", BasicDBObjectBuilder.start().add("cleaned", cleaned.toString()).get());

        MongoUpdateWritable outputValue = new MongoUpdateWritable();
        outputValue.setQuery(query);
        outputValue.setModifiers(modifiers);

        context.write(null, outputValue);
    }
}
