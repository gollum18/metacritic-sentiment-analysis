package metacritic.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class NLPMapper extends Mapper<Object, BSONObject, Text, Text> {

    @Override
    public void map(Object key, BSONObject val, final Context context)
            throws IOException, InterruptedException {
        Text keyOut = new Text(val.get("_id").toString());
        String body = val.get("body").toString();
        for (String sentence : body.split("(?<=[.!?:])\\s")) {
            context.write(keyOut, new Text(sentence));
        }
    }

}
