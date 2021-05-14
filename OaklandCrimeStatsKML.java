/**
 * Author: Wangning Shao
 * Last Modified: April 22nd 2021
 *
 * We use MapReduce to count crime type aggravated assault in CrimeLatLonXYTabs.txt file within 300 meters range with specified location
 */
package org.myorg;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class OaklandCrimeStatsKML extends Configured implements Tool {
    //this is our Mapper class
    public static class CrimeCountMap extends Mapper<LongWritable, Text, Text, Text>
    {
        // add count to each word
        private Text word = new Text(); // this will hold word read from file
        private final static double oaklandX = 1354326.897; // this will be the x coordinate we want to compare
        private final static double oaklandY = 411447.7828; // this will be the y coordinate we want to compare
        /**
         * @param key, value, context
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString(); //convert Text to String
            //Since each column is separated by a tab we will use "\t" to split them into a arraylist called columns
            String[] columns = line.split("\t");
            //skipping the first row since they are column names
            if (!columns[0].equals("X"))
            {
                //the offense column is located in the 5th element in the columns arraylist with index 4
                String offenseType = columns[4];
                double x = Double.parseDouble(columns[0]); // get each crime's x coordinate
                double y = Double.parseDouble(columns[1]); // get each crime's y coordinate
                //we will use Euclidean distance formula to get the distance between two sites and convert it to meters
                double distance = Math.sqrt(Math.pow(x - oaklandX, 2) + Math.pow(y - oaklandY, 2)) * 0.3048;
                //check if the value of  offsenseType == "AGGRAVATED ASSAULT" and within 300 meters range
                if(offenseType.equals("AGGRAVATED ASSAULT") && distance < 300)
                {
                    word.set("assualtAtOakland"); //create a word with key "assualtAtOakland"
                    context.write(word, new Text(line)); // put word into context with entire row information
                }
            }

        }
    }
    //this is our Reducer class
    public static class CrimeCountReducer extends Reducer<Text, Text, Text, Text>
    {
        /**
         * @param key, values, context
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            // initialize a StringBuilder to store information about the kml file we will construct
            StringBuilder sb = new StringBuilder();
            // header will contain partial information needed to construct kml file
            String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n" +
                    "  <Document>\n" +
                    "    <name>Oakland Aggravated Assault near 3803 Forbes Avenue</name>\n" +
                    "    <description>Project5 Task8</description>";
            sb.append(header); //append to StringBuilder

            //loop through each row and building the fml
            for(Text value: values)
            {
                //initialize a StringBuilder to store content information
                StringBuilder content = new StringBuilder();
                content.append("      <Placemark>\n" +
                        "        <name>");
                //get entire row information into row
                String row = value.toString();
                //Since each column is separated by a tab we will use "\t" to split them into a arraylist called columns
                String[] columns = row.split("\t");
                //The fourth column is a street address which is index 3 in our case
                // this will help differentiate each crime sense
                String address = columns[3];
                //The eight column specifies the latitude index 7. The ninth column specifies the longitude index 8
                double latitude = Double.parseDouble(columns[7]);
                double longitude = Double.parseDouble(columns[8]);
                // building up kml file with information we get from each row
                content.append(address + "</name>\n");
                content.append("        <Point>\n");
                content.append("          <coordinates>" + longitude + "," + latitude + "</coordinates>\n"
                        + "        </Point>\n" + "      </Placemark>\n");
                //append it to our final StringBuilder
                sb.append(content.toString());
            }
            // footer needed to indicate closing kml file
            String footer = "  </Document>\n" +
                    "</kml>";
            sb.append(footer);
            context.write(null, new Text(sb.toString())); // write result to context
        }

    }

    public int run(String[] args) throws Exception  {
        //start the job
        Job job = new Job(getConf());
        job.setJarByClass(OaklandCrimeStatsKML.class); //Match our class name RapeAndRobberCount
        job.setJobName("oaklandassaultcount"); // set an arbitrary job name "oaklandassaultcount"

        job.setOutputKeyClass(Text.class); // set outputKeyClass to Text
        job.setOutputValueClass(Text.class);// set outputValueClass to IntWritable

        job.setMapperClass(CrimeCountMap.class); // set MapperClass to CrimeCountMap
        job.setReducerClass(CrimeCountReducer.class); // set ReducerClass to CrimeCountReducer


        job.setInputFormatClass(TextInputFormat.class);// set inputFormatClass to TextInputFormat
        job.setOutputFormatClass(TextOutputFormat.class);// set outputFormatClass to TextOutputFormat


        FileInputFormat.setInputPaths(job, new Path(args[0])); //set input file path  to first parameter user provided
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//set input file path  to second parameter user provided

        boolean success = job.waitForCompletion(true); //get result of map reduce 0 or 1
        return success ? 0: 1;
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int result = ToolRunner.run(new OaklandCrimeStatsKML(), args);
        System.exit(result);
    }

}
