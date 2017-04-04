import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.swing.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by haiki on 4/4/17.
 */
public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] transition = value.toString().trim().split("\t");
            String[] toWeb = transition[1].split(",");
            int num = toWeb.length;
            for(int i = 0; i < num; i++){
                String outputValue = toWeb[i] + "=1/" + Integer.toString(num);
                context.write(new Text(transition[0]), new Text(outputValue));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text>{
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            //pr.txt 1\t1/6012
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            //key=value <2=1/4, 7=1/4, 8=1/4, 29=1/4, 1/6012>
            //separate transition cell from pagerank cell
            List<String> transitionCell = new ArrayList<String>();
            double prCell = 0;
            for(Text value : values){
                if(value.toString().contains("=")){
                    transitionCell.add(value.toString().trim());
                }
                else{
                    prCell = Double.parseDouble(value.toString().trim());
                }
            }

            //multiply
            for(String cell : transitionCell){
                String outputKey = cell.split("=")[0];
                double relation = Double.parseDouble(cell.split("=")[1]);
                String outputValue = String.valueOf(relation * prCell);
                context.write(new Text(outputKey), new Text(outputValue));

            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, false, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, false, conf);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
