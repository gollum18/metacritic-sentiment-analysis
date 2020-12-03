import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

public class ReviewCleanerMapper extends Mapper<Object, BSONObject, Text, Text> {
    @Override
    public void map(final Object key, final BSONObject value, final Context context)
            throws IOException, InterruptedException{
        Text outputKey = new Text(value.get("_id").toString());
        Text outputValue = new Text(LuceneCleaner.cleanString(value.get("body").toString()));
        context.write(outputKey, outputValue);
    }
}
