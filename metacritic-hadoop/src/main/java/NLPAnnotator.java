import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class NLPAnnotator extends MongoTool {

    public static void main(String[] args) throws Exception {
        /*
        MultiCollectionSplitBuilder builder = new MultiCollectionSplitBuilder();
        builder.add(new MongoClientURI("mongodb://localhost:27017/metacritic.critic_reviews.in"),
                null, true, null, null, null,
                false, null)
               .add(new MongoClientURI("mongodb://localhost:27017/metacritic.user_reviews.in"),
                null, true, null, null, null,
                false, null);
        Configuration conf = new Configuration();
        conf.set(MultiMongoCollectionSplitter.MULTI_COLLECTION_CONF_KEY, builder.toJSON());
        conf.set("mongo.output.uri", "mongodb://localhost:27017/metacritic.annotations");
        System.exit(ToolRunner.run(conf, new NLPAnnotatorXMLConfig(conf), args));
         */
        if (args.length != 2) {
            System.out.println("usage: hadoop jar metacritic-hadoop.jar <input_uri> <output_uri>");
            System.out.println("<input_uri>: MongoDB input URI to read split input from.");
            System.out.println("<output_uri>: MongoDB output URI to write reduce results to.");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("mongo.input.uri", args[0]);
        conf.set("mongo.output.uri", args[1]);
        System.exit(ToolRunner.run(conf, new NLPAnnotatorXMLConfig(conf), args));
    }

}
