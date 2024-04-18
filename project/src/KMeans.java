package it.unipi.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.io.*;
import java.util.List;
import org.apache.hadoop.filecache.DistributedCache;
import javax.naming.ConfigurationException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.time.*;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

public class KMeans{
    private static List<Point> oldCentroids;

    //Carica i centroidi calcolati dall'iterazione appena terminata dai file di output 
    private static List<Point> loadNewCentroids(Job context, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        List<Point> newCentroids = new ArrayList<>();
        Path outputPath = FileOutputFormat.getOutputPath(context);

        // Leggi il file di output del Reducer per ottenere i nuovi centroidi
        BufferedReader reader = null;
        for(int i = 0; i < conf.getInt("NUM_CENTROIDS", 0); i++ ){
            //Usiamo String.format per i file di output con indice maggiore di 9
            String num = String.format("%0" + 5 + "d", i);
            try(FSDataInputStream in = fs.open(new Path(outputPath, "part-r-" + num))){
                reader = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parti = line.split("\t"); 
                    String stringa = parti[1]; 
                    newCentroids.add(Point.parse(stringa));
                }
            }
        }
        reader.close();
        return newCentroids;
    }

    //Inizializza i centroidi usando alcuni punti casuali del dataset (Viene chiamata solo prima della prima iterazione)
    private static void initializeCentroids(Configuration conf, String inputPath) {
        KMeans.oldCentroids = new ArrayList<>();
        for(int i = 0; i < conf.getInt("NUM_CENTROIDS", 0); i++){
            List<String> lines = new ArrayList<>();
            try { 
                lines = Files.readAllLines(Paths.get(inputPath));
            } catch (IOException e) {
                e.printStackTrace();
            }
            //Seleziona casualmente una delle righe del dataset
            int row = (int) (Math.random() * conf.getInt("NUM_POINTS", 0));
            Point p = Point.parse(lines.get(row));
            KMeans.oldCentroids.add(p);
        }
    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        private Configuration conf;
        private List<Point> centroids;

        //Aggiorna la lista di centroidi per la nuova iterazione (dalla seconda iterazione in poi)
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.conf = context.getConfiguration();
            this.centroids = new ArrayList<>();
            
            for(int i = 0; i < this.conf.getInt("NUM_CENTROIDS", 0); i++){
                Point centroid = Point.parse(this.conf.get("oldCentroids" + i)); 		
		        this.centroids.add(centroid);
            }   
        }    

        //Permette di calcolare qual è il centroide più vicino ad ogni punto appartenente al dataset
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 

            //Deserializza i punti del dataset
            String[] righe = value.toString().split("\\n");
            List<Point> points = new ArrayList<>();
            for(int i = 0; i < righe.length; i++){
                Point point = Point.parse(righe[i]);
                points.add(point);
            }

            int closestCentroidIndex = -1;
            double closestDistance = Double.MAX_VALUE;

            //Per ogni punto calcola l'indice del centroide più vicino al punto
            for(Point point : points){
                closestDistance = Double.MAX_VALUE;
                for (int i = 0; i < this.centroids.size(); i++) {
                    double distance = point.euclideanDistance(this.centroids.get(i));
                    if (distance < closestDistance) {
                        closestCentroidIndex = i;
                        closestDistance = distance;
                    }
                }
                IntWritable closestCentroidInd = new IntWritable(closestCentroidIndex);
                Point output = new Point(point);

                //Emette l'indice calcolato e il rispettivo punto
                context.write(closestCentroidInd, output);
            }   
        } 
    }

    public static class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {

        //Viene effettuata una sommatoria parziale dei punti vicini al solito centroide. Garantisce un minore traffico di dati tra Mapper e Reducer
        protected void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            
            // Aggrega i punti parziali per il centroide di indice "key"
            Point sum = Point.copy(values.iterator().next());
            while (values.iterator().hasNext()) {
                sum.add(values.iterator().next());
            }
            // Emette il centroide aggregato ed il relativo indice
            context.write(key, sum);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Point> {

        //Effettua la sommatoria totale delle sommatorie parziali e successivamente divide il risultato ottenuto per il numero di punti aggregati. 
        @Override
        protected void reduce(IntWritable key, Iterable<Point> values, Context context)
                throws IOException, InterruptedException {
                  
            Point newCentroid = Point.copy(values.iterator().next());
            while (values.iterator().hasNext()) {
                newCentroid.add(values.iterator().next());
            }
            
            Point output = newCentroid.divide(newCentroid.getAggregatedPoints());
            //Emette l'indice del centroide ed il nuovo centroide
            context.write(key, output);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("NUM_CENTROIDS", 5);
        conf.setInt("NUM_POINTS", 10000);
        conf.setInt("SPACE_DIMENSION", 5);
        conf.setInt("MAX_ITERATIONS", 15);
        conf.setDouble("THRESHOLD", 0.1);
        KMeans.initializeCentroids(conf, args[0]);
        boolean exitCode = false;
        int iterationsCounter = 0;
        boolean stopIterations = false;
        
        //Controllo le condizioni di stop
        while(!stopIterations && iterationsCounter < conf.getInt("MAX_ITERATIONS", 15)){
            for(int i = 0; i < KMeans.oldCentroids.size(); i++){
		        conf.unset("oldCentroids" + i);
                //Salva nel contesto di Hadoop i centroidi dell'iterazione precedente
                conf.set("oldCentroids" + i, KMeans.oldCentroids.get(i).toString());
            }
            Job job = Job.getInstance(conf, "KMeans" + iterationsCounter);
            stopIterations = true;
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Point.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Point.class);
            job.setNumReduceTasks(conf.getInt("NUM_CENTROIDS", 0));

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/iteration"+iterationsCounter));

            exitCode = job.waitForCompletion(true);
            iterationsCounter ++;

            if(exitCode){
                //Vengono controllate le due condizioni di stop, nel caso non siano soddisfatte si procede all'iterazione successiva, altrimenti si ferma l'applicazione
                List<Point> newCentroids = loadNewCentroids(job, conf);
                for(int i=0; i <  KMeans.oldCentroids.size(); i++){
                    if(KMeans.oldCentroids.get(i).euclideanDistance(newCentroids.get(i)) >  conf.getDouble("THRESHOLD", 1)){
                        stopIterations = false; 
                        KMeans.oldCentroids.clear();
                        //Nel caso non siano state soddisfatte le condizioni di stop si aggiornano i centroidi per l'iterazione successiva
                        KMeans.oldCentroids.addAll(newCentroids);
                        break;
                    }    
                }
            }
            else {
                //Blocco l'esecuzione dell'applicazione nel caso si sia verificata la condizione di stop relativa alla threshold
                stopIterations = true;
            }
        }   
        System.exit(exitCode? 0 : 1);
    }
}

