import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class SentimentAnalyzerRunner extends MongoTool {
    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.out.println();
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("mongo.input.uri", args[0]);
        conf.set("mongo.output.uri", args[1]);
        conf.set("ip", args[2]);
        conf.set("port", args[3]);
        conf.set("db", args[4]);
        conf.set("words_collection", args[5]);
        ToolRunner.run(conf, new SentimentAnalyzerXMLConfig(conf), args);
    }
}
