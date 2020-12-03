import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class NLPAnnotator extends MongoTool {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: hadoop jar metacritic-hadoop.jar <input_uri> <output_uri>");
            System.out.println("<input_uri>: MongoDB input URI to read split input from.");
            System.out.println("<output_uri>: MongoDB output URI to write reduce results to.");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("mongo.input.uri", args[0]);
        conf.set("mongo.output.uri", args[1]);
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.output.fileoutputformat.compress", "false");
        conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapred.output.compression.type", "block");
        conf.set("mapreduce.task.io.sort.mb", "256");
        System.exit(ToolRunner.run(conf, new NLPAnnotatorXMLConfig(conf), args));
    }

}
