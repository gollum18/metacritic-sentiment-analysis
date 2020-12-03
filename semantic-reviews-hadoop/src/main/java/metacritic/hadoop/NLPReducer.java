package metacritic.hadoop;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import java.io.IOException;

public class NLPReducer extends Reducer<Text, Text, NullWritable, MongoUpdateWritable> {
    private final NLPAnnotatorPipeline pipeline = new NLPAnnotatorPipeline();

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
        BasicBSONList sentiments = new BasicBSONList();
        for (Text valueIn : values) {
            String sentence = valueIn.toString();
            sentiments.addAll(pipeline.getSentiments(sentence));
        }

        BasicBSONObject query = new BasicBSONObject("_id", new ObjectId(key.toString()));
        BasicBSONObject modifiers = new BasicBSONObject();
        modifiers.put("$set", BasicDBObjectBuilder.start().add("snlp_sentiments", sentiments).get());

        MongoUpdateWritable valueOut = new MongoUpdateWritable();
        valueOut.setQuery(query);
        valueOut.setModifiers(modifiers);

        context.write(null, valueOut);
    }

}
