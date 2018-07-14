import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SPDistance {
    public static Adjacency ad;
    public static String inputPath;
    public static String inputPathForQuery;
    public static String outputPath;
    public static class Node implements Writable {
        private int n;
        private int value;
        private boolean visited = false;
        private ArrayList<Node> edges;
        private IntWritable distance ;

        public Node(int value){
            this.value = value;
            this.edges = new ArrayList<Node>();
            this.distance = new IntWritable(0);
        }

        public void setN(int n){
            this.n = n;
        }

        public  int getN(){
            return this.n;
        }
        public int getValue(){
            return this.value;
        }

        public void addEdges(Node e){
            this.edges.add(e);
        }

        public IntWritable getDistance(){
            return this.distance;
        }

        public  boolean isVisited(){
            return this.visited;
        }

        public void hasVisited(){
            this.visited = true;
        }

        public void setDistance(IntWritable distance){
            this.distance = distance;
        }

        public void notVisited(){
            this.visited = false;
        }

        public ArrayList<Node> getEdges() {
            return edges;
        }

        public void readFields(DataInput in) {

        }

        public void write(DataOutput in) {

        }
    }

    public static class Adjacency{
        private ArrayList<Node> nodes = new ArrayList<Node>();
        public ArrayList<Node> nodeVec = new ArrayList<Node>();
        public ArrayList<IntWritable> d = new ArrayList<IntWritable>();
        public Node firstNode;

        public void addNode(Node n){
            nodes.add(n);
        }

        public Adjacency(){
            firstNode = new Node(1);
            outputPath = "output/part-r-00000";
        }

        public Node contains(Node n){
            for (Node node : nodes){
                if (node.getValue() == n.getValue()){
                    return node;
                }
            }

            return null;
        }

        public void reset(){
            for (Node n : nodes){
                n.setDistance(new IntWritable(0));
                n.notVisited();
            }
        }

        public Node getNode(Node source, int t){
            ArrayList<Node> vec = new ArrayList<Node>();
            vec.add(source);
            while (!vec.isEmpty()){
                Node curr = vec.get(0);
                vec.remove(0);
                if(curr.getValue() == t){
                    return curr;
                }

                ArrayList<Node> edges = curr.getEdges();
                for (Node n : edges){
                    if(!n.isVisited()){
                        n.hasVisited();
                        n.setDistance(new IntWritable(Integer.parseInt(curr.getDistance().toString()) + 1));
                        vec.add(n);
                    }
                }

            }

            return null;
        }

    }


    public static void exit() throws IOException{
        File file = new File("DRes");
        FileWriter fw = new FileWriter("output/part-r-00000",false);

        for (Node x : ad.nodeVec){
            fw.write(x.getN() + "   " + x.getValue() + "\n");
        }

        fw.close();

        outputPath = "DRES";
        file = new File(outputPath);
        fw = new FileWriter(file,false);
        for (IntWritable x : ad.d){
            fw.write(Integer.parseInt(x.toString()) + "\n");
        }


        fw.close();

    }
    public static Node addNode(Scanner in, Adjacency ad){
        if(!in.hasNextInt()){
            return null;
        }

        Node node = new Node(in.nextInt());
        Node found = ad.contains(node);
        if(found == null){
            ad.addNode(node);
            return node;
        }
        return found;
    }

    public static void readQuery() {
        File file = new File(inputPathForQuery);
        Scanner in;

        try{
            in = new Scanner(file);
            while (in.hasNext()){
                Node source = ad.contains(new Node(in.nextInt()));
                int t = in.nextInt();
                Node node = ad.getNode(source,t);
                ad.nodeVec.add(node);
                ad.d.add(node.getDistance());
                ad.reset();

            }
        }catch (Exception e){
            System.out.println("ERROR: " + e.getMessage());
        }


    }

    public static void readInput(){
        ad = new Adjacency();
        File file = new File(inputPath);

        Scanner in;
        try{
            in = new Scanner(file);

            while (true){
                Node a = addNode(in,ad);

                if(a == null){
                    break;
                }

                Node b = addNode(in,ad);

                if(b == null){
                    break;
                }
                a.addEdges(b);

            }
        }catch (Exception e){
            System.out.println("ERROR: " + e.getMessage());
        }

        readQuery();

    }


    public static class LoadMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String val = value.toString();
            String[] arr1 = val.split(" ");
            for (String temp:arr1){
                int node = Integer.parseInt(temp);
                word.set(Integer.toString(node));
                context.write(word,new IntWritable(node));
            }
        }
    }

    public static class LoadReducer
            extends Reducer<Text,IntWritable,Text,Node> {

        public enum UPDATED_COUNTER{
            COUNT
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String val = key.toString();
            Node node = new Node(Integer.parseInt(val));
            for (IntWritable value : values){
                Node n = new Node(Integer.parseInt(value.toString()));
                node.addEdges(n);
            }
            context.write(key,node);
            long  c =  context.getCounter(UPDATED_COUNTER.COUNT).getValue();
            if(c < ad.nodeVec.size()){
                ad.nodeVec.get((int)c).setN(Integer.parseInt(key.toString()));
            }
            context.getCounter(UPDATED_COUNTER.COUNT).increment(1);
        }
    }


    public static  class SearchFontierMapper extends
    Mapper<Text,Node,Text,Node>{
        public void map(Text key,Node value,Context context) throws IOException,InterruptedException{
                IntWritable d = value.getDistance();
                context.write(key,value);

                for(Node neighbor : value.getEdges()){
                    if(!neighbor.isVisited()){
                        neighbor.setDistance(new IntWritable(Integer.parseInt(d.toString())+1));
                        context.write(new Text(Integer.toString(neighbor.getValue())), neighbor);
                    }
                }
        }
    }

    public static class SearchFontierReducer extends Reducer<Text,Node,Text,Node>{

        public enum UpdateCounter{
            UPDATE_COUNTER
        }

        public void reduce(Text key,Iterable<Node> values,Context context) throws IOException,InterruptedException{
            IntWritable dMin = new IntWritable(Integer.MAX_VALUE);
            //Counter updated = UpdateCounter.UPDATE_COUNTER
            for (Node n : values){
                if(Integer.parseInt(n.getDistance().toString()) <
                        Integer.parseInt(dMin.toString())){
                    dMin = n.getDistance();
                    context.getCounter(UpdateCounter.UPDATE_COUNTER).increment(1);
                }
            }

            Node node = new Node(Integer.parseInt(key.toString()));
            node.hasVisited();
            node.setDistance(dMin);
            context.write(key,node);

        }
    }


    public static void main(String[] args) throws Exception {
        inputPath = args[0];
        inputPathForQuery = args[1];
        readInput();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Shortest Path");
        job.setJarByClass(SPDistance.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(LoadMapper.class);
        job.setReducerClass(LoadReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        Path outputDir = new Path(args[2]);
        FileSystem.get(conf).delete(outputDir, true);
        job.waitForCompletion(true);

        long counter = job.getCounters().findCounter(SearchFontierReducer.UpdateCounter.UPDATE_COUNTER).getValue();
        long i = 0;
        while(i < counter){
            job = new Job(conf,"BFS" + counter);
            job.setJarByClass(SPDistance.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Node.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Node.class);

            job.setMapperClass(SearchFontierMapper.class);
            job.setReducerClass(SearchFontierReducer.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.setInputPaths(job,new Path(args[1] + counter));
            FileOutputFormat.setOutputPath(job,new Path(args[1] + counter));

            i = job.getCounters().findCounter(SearchFontierReducer.UpdateCounter.UPDATE_COUNTER).getValue();
            job.waitForCompletion(true);

        }
        exit();
    }
}
