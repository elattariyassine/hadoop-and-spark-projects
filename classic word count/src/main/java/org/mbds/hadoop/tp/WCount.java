package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Anne 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountMap.java: classe driver (contient le main du programme).
*/

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;


// Note classe Driver (contient le main du programme Hadoop).
public class WCount
{
	// Le main du programme.
	public static void main(String[] args) throws Exception
	{
		// Cr un object de configuration Hadoop.
		Configuration conf=new Configuration();
		// Permet  Hadoop de lire ses arguments gnriques, rcupre les arguments restants dans ourArgs.
		String[] ourArgs=new GenericOptionsParser(conf, args).getRemainingArgs();
		// Obtient un nouvel objet Job: une tche Hadoop. On fourni la configuration Hadoop ainsi qu'une description
		// textuelle de la tche.
		Job job=Job.getInstance(conf, "Compteur de mots v1.0");

		// Dfini les classes driver, map et reduce.
		job.setJarByClass(WCount.class);
		job.setMapperClass(WCountMap.class);
		job.setReducerClass(WCountReduce.class);

		// Dfini types clefs/valeurs de notre programme Hadoop.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Dfini les fichiers d'entre du programme et le rpertoire des rsultats.
		// On se sert du premier et du deuxime argument restants pour permettre  l'utilisateur de les spcifier
		// lors de l'excution.
		FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));

		// On lance la tche Hadoop. Si elle s'est effectue correctement, on renvoie 0. Sinon, on renvoie -1.
		if(job.waitForCompletion(true))
			System.exit(0);
		System.exit(-1);
	}
}
