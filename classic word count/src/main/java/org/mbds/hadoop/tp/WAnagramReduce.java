package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Anne 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountReduce.java: classe REDUCE.
*/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


// Notre classe REDUCE - templatee avec un type generique K pour la clef, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class WAnagramReduce extends Reducer<Text, Text, Text, Text>
{
	private Text result = new Text();
	// La fonction REDUCE elle-meme. Les arguments: la clef key (d'un type generique K), un Iterable de toutes les valeurs
	// qui sont associees a la clef en question, et le contexte Hadoop (un handle qui nous permet de renvoyer le resultat a Hadoop).
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		String anagramlist = "";
		int length = 0;
		for(Text val : values){
			anagramlist+= "," + val.toString();
			length++;
		}
		System.out.println(key+" "+anagramlist);
		if(length>2){
			result.set(anagramlist);
			context.write(key,result);
		}
    }
}