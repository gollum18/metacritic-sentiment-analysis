import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;

/**
 * Implements a NLP Pipeline that performes sentiment analysis on input records.
 */
public class NLPAnnotatorPipeline {
    private final StanfordCoreNLP mPipeline;

    public NLPAnnotatorPipeline() {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, parse, sentiment");
        mPipeline = new StanfordCoreNLP(props);
    }

    public synchronized Dictionary<String, Integer> performSentimentAnalysis(String corpus) {
        Annotation doc = new Annotation(corpus);
        Dictionary<String, Integer> analysis = new Hashtable<String, Integer>();

        mPipeline.annotate(doc);

        List<CoreMap> sentences = doc.get(CoreAnnotations.SentencesAnnotation.class);
        int sid = 0;
        for (CoreMap sentence : sentences) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);

            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            String text = sentence.toString();

            analysis.put(text, sentiment);
        }

        return analysis;
    }
}
