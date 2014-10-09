import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount3 {


 public static class MapCount extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {

            /*char c = Character.toUpperCase(tokenizer.nextToken().charAt(0));
            if((c >= 'A' && c <= 'Z')) {
               word.set(String.valueOf(c));
               context.write(word, one);
            }*/

            String w = tokenizer.nextToken();
            if(w.length() == 7) {
               word.set(w);
               context.write(word, one);
            }
        }
    }
 }

 public static class ReduceCount extends Reducer<Text, LongWritable, Text, LongWritable> {
     private HashMap<Text, LongWritable> cnt = new HashMap<>();

     public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(LongWritable val : values) {
           sum += val.get();
        }

        //We do not write anything here
        cnt.put(new Text(key), new LongWritable(sum));
     }

     public void cleanup(Context context) throws IOException, InterruptedException {
        HashMap<Text, LongWritable> sorted = sortByValues(cnt);

        int c = 0;
	//we write everything in this part
        for(Text key : sorted.keySet()) {
          if (c++ == 100) break;
          context.write(key, sorted.get(key));
        }
    }
 }

 private static <K extends Comparable, V extends Comparable> HashMap<K, V> sortByValues(HashMap<K, V> map) {
    List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

    Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
       @Override
       public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
            return o2.getValue().compareTo(o1.getValue());
       }
    });

    HashMap<K, V> sorted = new LinkedHashMap<K, V>();
    for(Map.Entry<K, V> entry : entries) {
        sorted.put(entry.getKey(), entry.getValue());
    }

    return sorted;
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
        
    job.setMapperClass(MapCount.class);
    job.setReducerClass(ReduceCount.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(WordCount3.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
}

