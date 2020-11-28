import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Iterator;

public class NLPReducer extends Reducer<Text, Text, NullWritable, BSONWritable> {

    private final NLPAnnotatorPipeline pipeline = new NLPAnnotatorPipeline();

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException{
        BasicBSONList sentiments = new BasicBSONList();
        for (Text value : values) {
            Dictionary<String, Integer> analysis = pipeline.performSentimentAnalysis(value.toString());
            for (Iterator<String> it = analysis.keys().asIterator(); it.hasNext(); ) {
                String sentence = it.next();
                int sentimentValue = analysis.get(sentence);
                sentiments.add(String.format("%s:%d", sentence, sentimentValue));
            }
        }

        BSONWritable reduceResult = new BSONWritable();

        BSONObject outDoc = BasicDBObjectBuilder.start()
                .add("review_id", key.toString())
                .add("sentiment_analysis", sentiments).get();
        reduceResult.setDoc(outDoc);

        context.write(null, reduceResult);
    }

}
