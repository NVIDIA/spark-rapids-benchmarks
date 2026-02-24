/*
 * SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package org.nvidia.nds_h;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.commons.cli.*;

import java.io.*;
import java.net.*;
import java.math.*;
import java.security.*;
import java.util.Objects;


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
        Options options = getOptions();
        CommandLine line = parser.parse(options, remainingArgs);

        if(!line.hasOption("scale")) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("GenTable", options);
          return 1;
        }
        Path out = new Path(line.getOptionValue("dir"));
        
        int scale = Integer.parseInt(line.getOptionValue("scale"));

        String table = "all";
        if(line.hasOption("table")) {
          table = line.getOptionValue("table");
        }
        
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

        if(parallel == 1 || scale == 1) {
          System.err.println("The MR task does not work for scale=1 or parallel=1");
          return 1;
        }

        Path in = genInput(table, scale, parallel, rangeStart, rangeEnd);

        Path dbgen = copyJar(new File("target/dbgen.jar"));

        // Extracting the dbgen jar location and adding as a symlink as part of
        // Mapred Cache hence enabling access by all mappers running
        URI dsuri = dbgen.toUri();
        URI link = new URI(dsuri.getScheme(),
                    dsuri.getUserInfo(), dsuri.getHost(), 
                    dsuri.getPort(),dsuri.getPath(), 
                    dsuri.getQuery(),"dbgen");
        
        Configuration conf = getConf();
        conf.setInt("mapred.task.timeout",0);
        conf.setInt("mapreduce.task.timeout",0);
        conf.setBoolean("mapreduce.map.output.compress", true);
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");

        DistributedCache.addCacheArchive(link, conf);
        DistributedCache.createSymlink(conf);
        Job job = new Job(conf, "GenTable+"+table+"_"+scale);
        job.setJarByClass(getClass());

        // No reducers since no reduction task involved post data gen
        // Updating mapper class
        // Output will be a text file ( key(file_name) -> output )
        job.setNumReduceTasks(0);
        job.setMapperClass(Dbgen.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Using NLineInputFormat mapper for parsing each line of input
        // file as separate task
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

        job.waitForCompletion(true);

        // cleanup
        fs.delete(in, false);
        fs.delete(dbgen, false);

        return 0;
    }

    private static Options getOptions() {
        Options options = new Options();
        /*
         * These are the various options being passed to the class
         *  -s scale
         *  -d output directory
         *  -t specific table data
         *  -p number of parallel files to be generated
         *  -o overwrite output directory if exists
         */
        options.addOption("s","scale", true, "scale");
        options.addOption("d","dir", true, "dir");
        options.addOption("t","table", true, "table");
        options.addOption("p", "parallel", true, "parallel");
        options.addOption("o", "overwrite", false, "overwrite existing data");
        options.addOption("r", "range", true, "child range in one data generation run");
        return options;
    }

    /*
     * This function just copies the jar from the local to hdfs temp
     * location for access by the mappers
     */
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

    /*
     * This function generates the various commands to be run
     * parallely as part of the mapper for the job.
     * Each command runs the data generation for a specific part
     * for a table
     */
    public Path genInput(String table, int scale, int parallel, int rangeStart, int rangeEnd) throws Exception {        
        // Assigning epoch based name to the temporary files
        // Will be cleaned later
        long epoch = System.currentTimeMillis()/1000;
        Path in = new Path("/tmp/"+table+"_"+scale+"-"+epoch);
        FileSystem fs = FileSystem.get(getConf());
        FSDataOutputStream out = fs.create(in);

        // This is for passing the various params to the command
        // for individual tables
        String[ ] tables = {"c","O","L","P","S","s"};

        for(int i = rangeStart; i <= rangeEnd; i++) {
          String baseCmd = String.format("./dbgen -s %d -C %d -S %d ",scale,parallel,i);
          // In case of no specific table, data is generated for all
          // Separate commands for each table is generated for more parallelism
          // running multiple mappers
          if(table.equals("all")) {
            for(String t: tables){
              String cmd = baseCmd + String.format("-T %s",t);
              out.writeBytes(cmd+"\n");
            }
          }
          else{
            // TODO - update using map based approach for a cleaner implementation
            if(table.equalsIgnoreCase("customer")){
              String cmd = baseCmd + "-T c";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("nation")){
              String cmd = baseCmd + "-T n";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("region")){
              String cmd = baseCmd + "-T r";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("lineItem")){
              String cmd = baseCmd + "-T L";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("orders")){
              String cmd = baseCmd + "-T O";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("parts")){
              String cmd = baseCmd + "-T P";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("partsupp")){
              String cmd = baseCmd + "-T S";
              out.writeBytes(cmd + "\n");
            }
            else if(table.equalsIgnoreCase("supplier")){
              String cmd = baseCmd + "-T s";
              out.writeBytes(cmd + "\n");
            }
          }
        }
        // nation and region tables are static tables hence adding
        // a single command for both
        if(table.equals("all")){
          String cmdL = String.format("./dbgen -s %d -T l",scale);
          out.writeBytes(cmdL + "\n");
        }
        // Writing the command file in temporary folder for being read by the mapper
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

    static final class Dbgen extends Mapper<LongWritable,Text, Text, Text> {
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
        String table="";
        String suffix = "";
        String[] cmd = command.toString().split(" ");

        for(int i=0; i<cmd.length; i++) {
          if(cmd[i].equals("-C")) {
            parallel = cmd[i+1];
          }
          if(cmd[i].equals("-S")) {
            child = cmd[i+1];
          }
          if(cmd[i].equals("-T")) {
            table = cmd[i+1];
          }
        }

        System.out.println("Executing command: "+ String.join(" ", cmd));

        // Running the command using ProcessBuilder. The dbgen/dbgen is the jar files
        // of the makefile output jar
        Process p = Runtime.getRuntime().exec(cmd, null, new File("dbgen/dbgen/"));
        int status = p.waitFor();
        if(status != 0) {
          String err = readToString(p.getErrorStream());
          throw new InterruptedException("Process failed with status code " + status + "\n" + err);
        }

        // This is important for segragating files in separate folder
        // per each table
        File cwd = new File("./dbgen/dbgen");
        if(table.equals("l") || table.equals("n") || table.equals("r"))
          suffix = ".tbl";
        else if(table.equals("c"))
          suffix = String.format("customer.tbl.%s", child);
        else if(table.equals("L"))
          suffix = String.format("lineitem.tbl.%s",child);
        else if(table.equals("O"))
          suffix = String.format("orders.tbl.%s",child);
        else if(table.equals("P"))
          suffix = String.format("part.tbl.%s",child);
        else if(table.equals("S"))
          suffix = String.format("partsupp.tbl.%s",child);
        else if(table.equals("s"))
          suffix = String.format("supplier.tbl.%s",child);

        final String suffixNew = suffix;

        FilenameFilter tables = (dir, name) -> name.endsWith(suffixNew);

        for(File f: Objects.requireNonNull(cwd.listFiles(tables))) {
          if(f != null)
          {
            System.out.println("Processing file: "+f.getName());
          }
          final String baseOutputPath = f.getName().replace(suffix.substring(suffix.indexOf('.')), String.format("/data_%s_%s", child, parallel));
          BufferedReader br = new BufferedReader(new FileReader(f));
          String line;
          while ((line = br.readLine()) != null) {
            // process the line.
            mos.write("text", line, null, baseOutputPath);
          }
          br.close();
          f.deleteOnExit();
        }
        System.out.println("Processing complete");
      }
    }
}
