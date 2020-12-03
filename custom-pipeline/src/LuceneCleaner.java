import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

public class LuceneCleaner {

    public static String cleanString(String fullText) throws IOException {
        StringBuilder sb = new StringBuilder();
        TokenStream stream = null;

        try {
            fullText = fullText.replaceAll("-+", " ");

            fullText = fullText.replaceAll("[\\p{Punct}&&[^'-]]+", "");

            fullText = fullText.replaceAll("(?:'(?:[tdsm]|[vr]e|11))+\\b", "");

            StandardTokenizer tokenizer = new StandardTokenizer();
            tokenizer.setReader(new StringReader(fullText));

            stream = new StopFilter(
                    new ASCIIFoldingFilter(
                            new ClassicFilter(
                                    new LowerCaseFilter(tokenizer)
                            )
                    ), EnglishAnalyzer.getDefaultStopSet()
            );
            stream.reset();

            CharTermAttribute token = stream.getAttribute(CharTermAttribute.class);

            while (stream.incrementToken()) {
                String term = token.toString();
                String stem = getStemForm(term);

                if (stem != null) {
                    sb.append(stem).append(" ");
                }
            }

            return sb.toString();
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String getStemForm(String term) throws IOException {
        TokenStream stream = null;

        try {
            StandardTokenizer tokenizer = new StandardTokenizer();
            tokenizer.setReader(new StringReader(term));

            stream = new PorterStemFilter(tokenizer);
            stream.reset();

            Set<String> stems = new HashSet<>();

            CharTermAttribute token = stream.getAttribute(CharTermAttribute.class);

            while (stream.incrementToken()) {
                stems.add(token.toString());
            }

            if (stems.size() != 1) {
                return null;
            }

            String stem = stems.iterator().next();

            if (!stem.matches("[a-zA-Z0-9-]+")) {
                return null;
            }

            return stem;
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
