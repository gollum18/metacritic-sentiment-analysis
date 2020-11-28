import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class ReviewCleanerRunner extends MongoTool {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println();
            System.out.println();
            System.out.println();
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("mongo.input.uri", args[0]);
        conf.set("mongo.output.uri", args[1]);
        System.exit(ToolRunner.run(conf, new ReviewCleanerXMLConfig(conf), args));
    }
}
