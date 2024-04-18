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
import java.util.List;
import java.io.*;

public class Point implements Writable {
    private int aggregatedPoints; //I punti aggregati 
    private int dim;
    private double[] data;

    public Point() {
        this.dim = 0;
        this.data = new double[0];
        this.aggregatedPoints = 1;
    }

    public Point(Point point){
        this.data = point.data;
        this.dim = point.data.length;
        this.aggregatedPoints = point.aggregatedPoints;
    }

    public Point(double[] data) {
        this.data = data;
        this. dim = data.length;
        this.aggregatedPoints = 1;
    }

    //Converte una stringa in un punto
    public static Point parse(String line) {
        String[] tokens = line.split(",");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return new Point(data);
    }

    //Calcola la distanza euclidea tra due punti
    public double euclideanDistance(Point other) {
        double sum = 0.0;
        for (int i = 0; i < data.length; i++) {
            sum += Math.pow(data[i] - other.data[i], 2);
        }
        return Math.sqrt(sum);
    }

    //Somma due punti (Somma vettoriale)
    public void add(Point other){
        for (int i = 0; i < data.length; i++) {
            this.data[i] += other.data[i];
        }

        this.aggregatedPoints += other.aggregatedPoints;
    }

    //Crea una copia del punto p
    public static Point copy(final Point p) {
        Point ret = new Point(p.data);
        ret.aggregatedPoints = p.aggregatedPoints;
        return ret;
    }

    //Divide il punto per una costante (ogni elemento del vettore)
    public Point divide(int divisor) {
        double[] result = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = data[i] / divisor;
        }
        return new Point(result);
    }


    public void setAggregatedPoints(int points) {
        this.aggregatedPoints = points;
    }

    public int getAggregatedPoints() {
        return this.aggregatedPoints;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.aggregatedPoints = in.readInt();
        this.dim = in.readInt();
        this.data = new double[dim];
        for (int i = 0; i < this.dim; i++) {
            this.data[i] = in.readDouble();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(aggregatedPoints);
        out.writeInt(dim);
        for (int i = 0; i < data.length; i++) {
            out.writeDouble(data[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (double d : data) {
            sb.append(d).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}