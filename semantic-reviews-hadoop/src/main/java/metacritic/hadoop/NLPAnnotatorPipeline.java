package metacritic.hadoop;

import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.*;

/**
 * Implements a NLP Pipeline that performes sentiment analysis on input records.
 */
public class NLPAnnotatorPipeline {
    private static StanfordCoreNLP mPipeline = null;

    public NLPAnnotatorPipeline() {
        if (mPipeline == null) {
            Properties props = new Properties();
            props.setProperty("parse.model", "edu/stanford/nlp/models/srparser/englishSR.ser.gz");
            props.put("annotators", "tokenize, ssplit, pos, parse, sentiment");
            mPipeline = new StanfordCoreNLP(props);
        }
    }

    public synchronized List<String> getSentiments(String corpus) {
        CoreDocument doc = new CoreDocument(corpus);
        List<String> analysis = new ArrayList<>();

        mPipeline.annotate(doc);

        List<CoreSentence> sentences = doc.sentences();
        for (CoreSentence sentence : sentences) {
            String sentiment = sentence.sentiment();
            analysis.add(sentiment);
        }

        return analysis;
    }
}
