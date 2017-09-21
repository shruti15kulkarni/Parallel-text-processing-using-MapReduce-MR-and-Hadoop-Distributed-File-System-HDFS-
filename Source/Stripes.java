//package pairs.prs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import java.io.IOException;
import java.util.Set;

public class Stripes{
	public static class MapTokenizer extends MapWritable{
		
		@Override
	    public String toString() {
	        StringBuilder result = new StringBuilder();
	        Set<Writable> keySet = this.keySet();

	        for (Object key : keySet) {
	            result.append("{" + key.toString() + " = " + this.get(key) + "}");
	        }
	        return result.toString();
	    }
	}
	public static class MapperStripes extends Mapper<LongWritable,Text,Text,MapTokenizer> {
	    private MapTokenizer mp = new MapTokenizer();
	    private Text word = new Text();

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        int neighbors = context.getConfiguration().getInt("neighbors", 2);
	        String[] tokens = value.toString().split("\\s+");
	        if (tokens.length > 1) {
	            for (int i = 0; i < tokens.length; i++) {
	                word.set(tokens[i]);
	                mp.clear();

	                int start = (i - neighbors < 0) ? 0 : i - neighbors;
	                int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
	                for (int j = start; j <= end; j++) {
	                    if (j == i) continue;
	                    Text neighbor = new Text(tokens[j]);
	                    if(mp.containsKey(neighbor)){
	                       IntWritable count = (IntWritable)mp.get(neighbor);
	                       count.set(count.get()+1);
	                    }else{
	                        mp.put(neighbor,new IntWritable(1));
	                    }
	                }
	              context.write(word,mp);
	            }
	        }
	    }
	}

	public static class ReducerStripes extends Reducer<Text, MapTokenizer, Text, MapTokenizer> {
	    private MapTokenizer mp1 = new MapTokenizer();

	    @Override
	    protected void reduce(Text key, Iterable<MapTokenizer> values, Context context) throws IOException, InterruptedException {
	        mp1.clear();
	        for (MapTokenizer value : values) {
	            addAll(value);
	        }
	        context.write(key, mp1);
	    }
	    @Override
	    public String toString(){
			return null;
	    	
	    }


	    private void addAll(MapTokenizer MapTokenizer) {
	        Set<Writable> keys = MapTokenizer.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) MapTokenizer.get(key);
	            if (mp1.containsKey(key)) {
	                IntWritable count = (IntWritable) mp1.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                mp1.put(key, fromCount);
	            }
	        }
	    }
	}



public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "Stripes");
  job.setJarByClass(Stripes.class);
  job.setMapperClass(MapperStripes.class);
  job.setCombinerClass(ReducerStripes.class);
  job.setReducerClass(ReducerStripes.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(MapTokenizer.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
