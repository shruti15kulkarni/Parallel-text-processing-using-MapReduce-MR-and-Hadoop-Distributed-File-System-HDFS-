import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Pairs {

public static class TokenizerMapper extends Mapper<LongWritable, Text, WordPair, IntWritable>
{
private final static IntWritable one = new IntWritable(1);
private final WordPair pair = new WordPair();
//private int neighbors = 2;



public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException

{
int neighbors = context.getConfiguration().getInt("neighbors", 2);
String[] dic = value.toString().split("\\W+");
for(int i = 0; i < dic.length; i++)
{
 pair.setWord(dic[i]);

              int start = (i - neighbors < 0) ? 0 : i - neighbors;
              int end = (i + neighbors >= dic.length) ? dic.length - 1 : i + neighbors;
              for (int j = start; j <= end; j++) {
                  if (j == i) continue;
 pair.setNeighbor(dic[j]);
context.write(pair, one);
}
}
}
}
public static class IntSumReducer
   extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(WordPair key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Pairs");
    job.setJarByClass(Pairs.class);


    job.setMapperClass(TokenizerMapper.class);
     job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }


public static class WordPair implements Writable,WritableComparable<WordPair> {

  private Text word;
  private Text neighbor;

  public WordPair(Text word, Text neighbor) {
      this.word = word;
      this.neighbor = neighbor;
  }

  public WordPair(String word, String neighbor) {
    this(new Text(word),new Text(neighbor));
  }

  public WordPair() {
      this.word = new Text();
      this.neighbor = new Text();
  }

   public int compareTo(WordPair other) {
      int returnVal = this.word.compareTo(other.getWord());
      if(returnVal != 0){
          return returnVal;
      }
      if(this.neighbor.toString().equals("*")){
          return -1;
           }else if(other.getNeighbor().toString().equals("*")){
          return 1;
      }
      return this.neighbor.compareTo(other.getNeighbor());
  }

  public WordPair read(DataInput in) throws IOException {
      WordPair wordPair = new WordPair();
      wordPair.readFields(in);
      return wordPair;
  }

  public void write(DataOutput out) throws IOException {
      word.write(out);
      neighbor.write(out);
  }


  public void readFields(DataInput in) throws IOException {
      word.readFields(in);
 neighbor.readFields(in);
  }

  @Override
  public String toString() {
      return "{word=["+word+"]"+
             " neighbor=["+neighbor+"]}";
  }

  @Override
  public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      WordPair wordPair = (WordPair) o;

      if (neighbor != null ? !neighbor.equals(wordPair.neighbor) : wordPair.neighbor != null) return false;
      if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

      return true;
  }
@Override
  public int hashCode() {
      int result = word != null ? word.hashCode() : 0;
      result = 163 * result + (neighbor != null ? neighbor.hashCode() : 0);
      return result;
  }

  public void setWord(String word){
      this.word.set(word);
  }
  public void setNeighbor(String neighbor){
      this.neighbor.set(neighbor);
  }

  public Text getWord() {
      return word;
  }

  public Text getNeighbor() {
      return neighbor;
  }
}
}



