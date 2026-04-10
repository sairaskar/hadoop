import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Sales 
{

    public static class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
    {
      private Text month = new Text();
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
      {
        String line = value.toString();
        String[] words = line.split("\t"); 
        month.set(words[0]);
        int total = Integer.parseInt(words[6]);
        context.write(month, new IntWritable(total));
          
        
    }
}


  public static void main(String args []) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Sales");
        
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(SalesMapper.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
    }
}
