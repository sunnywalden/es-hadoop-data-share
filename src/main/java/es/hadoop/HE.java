package es.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class HE {
    private static Logger LOG = LoggerFactory.getLogger(HE.class);
    public class HeMapper extends Mapper<Writable, Text, NullWritable, LinkedMapWritable> {
        @Override
        protected void map(Writable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException, InterruptedException {
            LinkedMapWritable doc = new LinkedMapWritable();
            context.write(NullWritable.get(), doc);
        }
    }




    public static void main(String[] args) {
        try {
            if (args.length != 3) {
                System.out.println("Usage: MaxTemperature <es.nodes> <es.resource> <input path>");
                System.exit(-1);
            }

            long start_time = System.currentTimeMillis();

            Configuration conf = new Configuration();

//            String[] oArgs = new GenericOptionsParser(conf , args).getRemainingArgs();
//
//
//
//
//
//            if (oArgs.length != 1) {
//                LOG.error("error,Usage: MaxTemperature <es.nodes> <es.resource> <input path>\"");
//                System.exit(2);
//            }

            conf.setBoolean("mapred.map.tasks.speculative.execution", false);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
            conf.set("es.input.json", "yes");

            conf.set("es.nodes", args[0]);
            conf.set("es.resource", args[1]);

            conf.setBoolean("mapreduce.map.output.compress", true);
            conf.set("mapreduce.map.output.compress.codec", "com.hadoop.compression.lzo.LzoCodec");


            Job job = Job.getInstance(conf, "hadoop to elasticsearch");

//            job.setJarByClass(HE.class);
            job.setMapperClass(HeMapper.class);

//        job.setInputFormatClass(LzoTextInputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(EsOutputFormat.class);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(LinkedMapWritable.class);


            // 设置输入路径
            FileInputFormat.setInputPaths(job, new Path(args[2]));
            System.out.println(job.waitForCompletion(true));

            System.out.println(System.currentTimeMillis() - start_time);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
