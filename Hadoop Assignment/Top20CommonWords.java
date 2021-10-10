import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import static java.lang.Math.min;

public class Top20CommonWords {

    //Mapper class to remove stopwords and retrieve lower frequency common word from both files.
    public static class CommonWordsMapper
            extends Mapper<Object, Text, Text, Text> {

        //Initialise variables.
        private static String filePath = new String("/severdirectory/stopwords.txt");
        private Text word = new Text();
        private final static Text file = new Text();
        public Set<String> stoppers = new HashSet<>();

        //Read stopwords file and add stopper words to set.
        @Override
        protected void setup(Context context) throws IOException {

            Path stoppersFile = new Path(filePath);
            FileSystem sys = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(sys.open(stoppersFile)));
            String line;

            while ((line = br.readLine()) != null) {
                stoppers.add(line);
            }
        }

        //Read both input files and generate (word, filename) key-value pairs.
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            //Splits input into words based on delimiters " \t\n\r\f"
            String[] splitInput = value.toString().trim().split("\\s+|\\t|\\n|\\r|\\f");

            for (int i = 0; i < splitInput.length; i++) {
                word.set(splitInput[i]);
                //Obtain filename of input file.
                FileSplit fileSplit = (FileSplit)context.getInputSplit();
                String filename = fileSplit.getPath().getName();
                file.set(filename);

                //Only generate key-value pairs (word, file) if word does not appear in stoppers file.
                if (!stoppers.contains(word.toString()) && !word.toString().equals("")) {
                    context.write(word, file);
                }
            }
        }
    }

    //Reducer class to sum all values associated to each key to generate (word, freq) key-value pairs.
    public static class SumCommonWordsReducer
            extends Reducer<Text, Text, Integer, Text> {

        //Initialise variables.
        Set<Text> numFiles = new HashSet<>();
        Map<Text, Integer> keyValuePairs = new HashMap<>();

        //Counts frequency of each word that appears in each input file.
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //Initialise variables.
            int file1Count = 0;
            int file2Count = 0;
            int lowerFreq = 0;

            //Read filename and track how many times word occurs in file.
            for (Text val : values) {
                if (val.toString().equals("task1-input1.txt")) {
                    file1Count++;
                    if (!numFiles.contains(val)) {
                        numFiles.add(val);
                    }
                }

                if (val.toString().equals("task1-input2.txt")) {
                    file2Count++;
                    if (!numFiles.contains(val)) {
                        numFiles.add(val);
                    }
                }
            }

            //Add key-value pairs to hashmap only if word occurs in both files.
            if (numFiles.size() == 2) {
                Text word = new Text(key);
                lowerFreq = min(file1Count, file2Count);
                keyValuePairs.put(word, lowerFreq); //Store lower of the two wordcount as value.

            }
            numFiles.clear();

        }

        //Sort (word, lowerFreq) key-value pairs by descending values and output top 20 words in (lowerFreq, word).
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            //Add key-value pairs to linked hashmap and sort in descending values.
            LinkedHashMap<Text, Integer> sortedResults = new LinkedHashMap<>();
            keyValuePairs.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .forEachOrdered(x -> sortedResults.put(x.getKey(), x.getValue()));

            //Iterate and output top 20 key-value pairs.
            Set<Text> keys = sortedResults.keySet();
            int counter = 0;

            for (Text key : keys) {
                if (counter < 20) {
                    context.write(sortedResults.get(key), key);
                    counter++;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] jobs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job output1 = Job.getInstance(conf, "Top20CommonWords");
        output1.setJarByClass(WordCount.class);
        FileInputFormat.setInputDirRecursive(output1, true); //Set multiple inputs to mapper.
        FileInputFormat.addInputPath(output1, new Path(args[0]));
        FileInputFormat.addInputPath(output1, new Path(args[1]));
        FileInputFormat.addInputPath(output1, new Path(args[2]));
        output1.setMapperClass(CommonWordsMapper.class);
        output1.setMapOutputKeyClass(Text.class);
        output1.setMapOutputValueClass(Text.class);
        output1.setReducerClass(SumCommonWordsReducer.class);
        output1.setOutputKeyClass(Integer.class);
        output1.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(output1, new Path(jobs[3]));
        System.exit(output1.waitForCompletion(true) ? 0 : 1);

    }
}
