/*
 * SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.notmysock.tpcds;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;

import org.apache.commons.cli.*;
import org.apache.commons.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.net.*;
import java.math.*;
import java.security.*;


public class GenTable extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new GenTable(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        CommandLineParser parser = new BasicParser();
        getConf().setInt("io.sort.mb", 4);
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("s","scale", true, "scale");
        options.addOption("t","table", true, "table");
        options.addOption("d","dir", true, "dir");
        options.addOption("p", "parallel", true, "parallel");
        options.addOption("r", "range", true, "child range in one data generation run");
        options.addOption("o", "overwrite", false, "overwrite existing data");
        options.addOption("u", "update", true, "generate data for Data Maintenance");
        CommandLine line = parser.parse(options, remainingArgs);

        if(!(line.hasOption("scale") && line.hasOption("dir"))) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("GenTable", options);
          return 1;
        }
        
        int scale = Integer.parseInt(line.getOptionValue("scale"));
        String table = "all";
        if(line.hasOption("table")) {
          table = line.getOptionValue("table");
        }
        Path out = new Path(line.getOptionValue("dir"));

        int parallel = scale;

        if(line.hasOption("parallel")) {
          parallel = Integer.parseInt(line.getOptionValue("parallel"));
        }

        int rangeStart = 1;
        int rangeEnd = parallel;

        if(line.hasOption("range")) {
            String[] range = line.getOptionValue("range").split(",");
            if (range.length == 1) {
                System.err.println("Please provide range with comma for both range start and range end.");
                return 1;
            }
            rangeStart = Integer.parseInt(range[0]);
            rangeEnd = Integer.parseInt(range[1]);
            if (rangeStart < 1 || rangeStart > rangeEnd || rangeEnd > parallel) {
                System.err.println("Please provide correct child range: 1 <= rangeStart <= rangeEnd <= parallel");
                return 1;
            }
        }

        Integer update = null;
        if(line.hasOption("update")) {
          update = Integer.parseInt(line.getOptionValue("update"));
          if(update < 0) {
            // TPC-DS will error if update is < 0
            System.err.println("The update value cannot be less than 0, your input: " + update);
            return 1;
          }
        }

        

        if(parallel == 1 || scale == 1) {
          System.err.println("The MR task does not work for scale=1 or parallel=1");
          return 1;
        }

        Path in = genInput(table, scale, parallel, rangeStart, rangeEnd, update);

        Path dsdgen = copyJar(new File("target/lib/dsdgen.jar"));
        URI dsuri = dsdgen.toUri();
        URI link = new URI(dsuri.getScheme(),
                    dsuri.getUserInfo(), dsuri.getHost(), 
                    dsuri.getPort(),dsuri.getPath(), 
                    dsuri.getQuery(),"dsdgen");
        Configuration conf = getConf();
        conf.setInt("mapred.task.timeout",0);
        conf.setInt("mapreduce.task.timeout",0);
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        DistributedCache.addCacheArchive(link, conf);
        DistributedCache.createSymlink(conf);
        Job job = new Job(conf, "GenTable+"+table+"_"+scale);
        job.setJarByClass(getClass());
        job.setNumReduceTasks(0);
        job.setMapperClass(DSDGen.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, 1);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);


        FileSystem fs = FileSystem.get(getConf());
        // delete existing files if "overwrite" is set
        if(line.hasOption("overwrite")) {
            if (fs.exists(out)) {
                fs.delete(out, true);
            }
        }

        // use multiple output to only write the named files
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "text", 
          TextOutputFormat.class, LongWritable.class, Text.class);

        boolean success = job.waitForCompletion(true);

        // cleanup
        fs.delete(in, false);
        fs.delete(dsdgen, false);

        return 0;
    }

    public Path copyJar(File jar) throws Exception {
      MessageDigest md = MessageDigest.getInstance("MD5");
      InputStream is = new FileInputStream(jar);
      try {
        is = new DigestInputStream(is, md);
        // read stream to EOF as normal...
      }
      finally {
        is.close();
      }
      BigInteger md5 = new BigInteger(md.digest()); 
      String md5hex = md5.toString(16);
      Path dst = new Path(String.format("/tmp/%s.jar",md5hex));
      Path src = new Path(jar.toURI());
      FileSystem fs = FileSystem.get(getConf());
      fs.copyFromLocalFile(false, /*overwrite*/true, src, dst);
      return dst; 
    }

    public Path genInput(String table, int scale, int parallel, int rangeStart, int rangeEnd, Integer update) throws Exception {
        long epoch = System.currentTimeMillis()/1000;

        Path in = new Path("/tmp/"+table+"_"+scale+"-"+epoch);
        FileSystem fs = FileSystem.get(getConf());
        FSDataOutputStream out = fs.create(in);
        String cmd = "";
        for(int i = rangeStart; i <= rangeEnd; i++) {
          if(table.equals("all")) {
            cmd += String.format("./dsdgen -dir $DIR -force Y -scale %d -parallel %d -child %d", scale, parallel, i);
          } else {
            cmd += String.format("./dsdgen -dir $DIR -table %s -force Y -scale %d -parallel %d -child %d", table, scale, parallel, i);
          }
          if(update != null) {
            cmd += String.format(" -update %d", update);
          }
          cmd += "\n";
          out.writeBytes(cmd);
        }
        out.close();
        return in;
    }

    static String readToString(InputStream in) throws IOException {
      InputStreamReader is = new InputStreamReader(in);
      StringBuilder sb=new StringBuilder();
      BufferedReader br = new BufferedReader(is);
      String read = br.readLine();

      while(read != null) {
        //System.out.println(read);
        sb.append(read);
        read =br.readLine();
      }
      return sb.toString();
    }

    static final class DSDGen extends Mapper<LongWritable,Text, Text, Text> {
      private MultipleOutputs mos;
      protected void setup(Context context) throws IOException {
        mos = new MultipleOutputs(context);
      }
      protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
      }
      protected void map(LongWritable offset, Text command, Mapper.Context context) 
        throws IOException, InterruptedException {
        String parallel="1";
        String child="1";

        String[] cmd = command.toString().split(" ");

        for(int i=0; i<cmd.length; i++) {
          if(cmd[i].equals("$DIR")) {
            cmd[i] = (new File(".")).getAbsolutePath();
          }
          if(cmd[i].equals("-parallel")) {
            parallel = cmd[i+1];
          }
          if(cmd[i].equals("-child")) {
            child = cmd[i+1];
          }
        }

        Process p = Runtime.getRuntime().exec(cmd, null, new File("dsdgen/tools/"));
        int status = p.waitFor();
        if(status != 0) {
          String err = readToString(p.getErrorStream());
          throw new InterruptedException("Process failed with status code " + status + "\n" + err);
        }

        File cwd = new File(".");
        final String suffix = String.format("_%s_%s.dat", child, parallel);

        FilenameFilter tables = new FilenameFilter() {
          public boolean accept(File dir, String name) {
            return name.endsWith(suffix) || name.startsWith("delete") || name.startsWith("inventory_delete");
          }
        };

        for(File f: cwd.listFiles(tables)) {
          BufferedReader br = new BufferedReader(new FileReader(f));          
          String line;
          while ((line = br.readLine()) != null) {
            // process the line.
            mos.write("text", line, null, f.getName().replace(suffix, String.format("/data_%s_%s", child, parallel)));
          }
          br.close();
          f.deleteOnExit();
        }
      }
    }
}
