import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

public class NLPAnnotatorXMLConfig extends MongoTool {
    public NLPAnnotatorXMLConfig() {
        this(new Configuration());
    }

    public NLPAnnotatorXMLConfig(final Configuration conf) {
        setConf(conf);

        if (MongoTool.isMapRedV1()) {
            MapredMongoConfigUtil.setInputFormat(conf, com.mongodb.hadoop.mapred.MongoInputFormat.class);
            MapredMongoConfigUtil.setOutputFormat(conf, com.mongodb.hadoop.mapred.MongoOutputFormat.class);
        } else {
            MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
            MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);
        }
        MongoConfigUtil.setMapper(conf, NLPMapper.class);
        MongoConfigUtil.setMapperOutputKey(conf, Text.class);
        MongoConfigUtil.setMapperOutputValue(conf, Text.class);

        MongoConfigUtil.setReducer(conf, NLPReducer.class);
        MongoConfigUtil.setOutputKey(conf, Text.class);
        MongoConfigUtil.setOutputValue(conf, BSONWritable.class);
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new NLPAnnotatorXMLConfig(), args));
    }
}
