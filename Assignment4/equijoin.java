import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;


public class equijoin {
    // Custom Mapper class
    public static class JoinColumnMapper
            extends Mapper<Object, Text, DoubleWritable, Text>{
        // overriding the map method
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // preprocess the text by removing all the spaces to feed to the separator
            String val = value.toString().replaceAll("\\s","");
            // use the second string as the double key and map all the text to the join column key
            if(!val.isEmpty()) {
                context.write(new DoubleWritable(Double.parseDouble(val.split(",")[1])), value);
            }
        }
    }

    // custom Reducer class
    public static class EquiJoinReducer
            extends Reducer<DoubleWritable,Text, NullWritable,Text> {
        //overriding the reduce method
        public void reduce(DoubleWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            // dictionary for separating table1 and table2 entries to be joined later
            HashMap<String, HashSet<String>> hmap = new HashMap<>();

            for(Text row: values){
                String strRow = row.toString();
                hmap.computeIfAbsent(strRow.split(",")[0].trim(), k -> new HashSet<>()).add(strRow);
            }

            // get the dictionary's key set into a list
            List<String> tableList = new ArrayList<>(hmap.keySet());

            // if there is only single table values for a column key, we do not join anything since it is an inner join
            if (tableList.size()==2) {
                for (String table1Val : hmap.get(tableList.get(0))) {
                    for (String table2Val : hmap.get(tableList.get(1))) {
                        String output = table1Val + ", " + table2Val;
                        context.write(NullWritable.get(), new Text(output));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "equijoin");
        job.setJarByClass(equijoin.class);
        job.setMapperClass(JoinColumnMapper.class);
        job.setReducerClass(EquiJoinReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}