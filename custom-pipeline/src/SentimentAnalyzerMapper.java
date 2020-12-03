import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.Document;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SentimentAnalyzerMapper extends Mapper<Object, BSONObject, Text, Text> {

    private LoadingCache<String, String> mAverages;
    private MongoClient mClient;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        mAverages = CacheBuilder.newBuilder()
            .maximumSize(1024)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build(
                new CacheLoader<>() {
                    public String load(String key) throws Exception {
                        MongoDatabase db = mClient.getDatabase(conf.get("db"));
                        MongoCollection<Document> coll = db.getCollection(
                            conf.get("words_collection")
                        );
                        Document doc = coll.find(Filters.eq("word", key)).first();
                        return doc.get("avg").toString();
                    }
                });
        mClient = new MongoClient(new ServerAddress(conf.get("ip"), Integer.parseInt(conf.get("port"))));
    }

    @Override
    public void map(final Object key, final BSONObject value, final Context context)
            throws IOException, InterruptedException {
        Text keyOut = new Text(value.get("_id").toString());
        String cleaned = value.get("cleaned").toString();
        for (String word : cleaned.split("\\s+")) {
            try {
                context.write(keyOut, new Text(mAverages.get(word)));
            } catch (ExecutionException ex) {
                throw new IOException(ex.toString());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        mAverages.cleanUp();
        mClient.close();
    }
}
