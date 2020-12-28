package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Anne 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountReduce.java: classe REDUCE.
*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;


// Notre classe REDUCE - templatee avec un type generique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class WCountReduce extends Reducer<Text, IntWritable, Text, Text>
{
	// La fonction REDUCE elle-meme. Les arguments: la clef key (d'un type generique K), un Iterable de toutes les valeurs
	// qui sont associees a la clef en question, et le contexte Hadoop (un handle qui nous permet de renvoyer le resultat a Hadoop).
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		// Pour parcourir toutes les valeurs associees a la clef fournie.
		Iterator<IntWritable> i=values.iterator();
		int count=0;
		while(i.hasNext())   // Pour chaque valeur...
			count+=i.next().get();    // ...on l'ajoute au total.
		// On renvoie le couple (clef;valeur) constituee de notre clef key et du total, au format Text.
		context.write(key, new Text(count+" occurences."));
  }
}