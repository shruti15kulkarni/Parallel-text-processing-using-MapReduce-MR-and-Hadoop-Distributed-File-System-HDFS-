
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
import java.util.*;  
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.File;

public class Latin {
  
  
public static class TokenizerMapper extends Mapper<Text, Text, Text, Text>

{
  Map<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
//private final static IntWritable one = new IntWritable(1);
//private final WordPair pair = new WordPair();
  public void generateMap() throws IOException{
        
        String key;
    ArrayList<String> values;
    BufferedReader br = new BufferedReader(new FileReader("new_lemmatizer.csv"));
    String line =  null;
    
    
    while((line=br.readLine())!=null){
    values = new ArrayList<String>();
    String str[] = line.split(",");
    key = str[0];
        for(int i=1;i<str.length;i++)
            {
            values.add(str[i]);
            }
            hm.put(key, values);
          }
}
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
     //String[] itr = value.toString().split(">\\s+");
      //if(itr.length==2){
      //String location = itr[0];
      String strn[] =  value.toString().split("\t");
      String valn = strn[0];
      String strn1[] = strn[1].split(" |\t|, |: |; |\\."); 
            for(int l=0;l<strn1.length;l++){
                strn1[l] = strn1[l].toLowerCase();
                strn1[l] = strn1[l].replace("?", "");
                strn1[l] = strn1[l].replace(",", "");
                strn1[l] = strn1[l].replace(":", "");
                strn1[l] = strn1[l].replace("-", "");
                strn1[l] = strn1[l].replace("j", "i");
                strn1[l] = strn1[l].replace("v", "u");
                strn1[l] = strn1[l].replace("\\W","");
                strn1[l] = strn1[l].replace("[^a-zA-Z]","");
                String word = strn1[l];
        ArrayList<String> listn;
        if(hm.containsKey(word)){
          context.write(new Text(word), new Text(valn));
          listn=hm.get(word);
        for(String s:listn){
          context.write(new Text(s), new Text(valn));
        }
        }
        else{
        context.write(new Text(word), new Text(valn));
        }
      }
  }
 }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String res="";
      for (Text val : values) {
        res=res+val.toString()+" ";
      }
      context.write(key, new Text(res));
    }
  }
  
public static void main(String[] args) throws Exception {
  

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Latin");
    job.setJarByClass(Latin.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


