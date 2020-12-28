package org.mbds.hadoop.tp;/*
  M2 MBDS - Big Data/Hadoop
	Anne 2013/2014
  --
  TP1: exemple de programme Hadoop - compteur d'occurences de mots.
  --
  org.mbds.hadoop.tp.WCountMap.java: classe MAP.
*/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;


// Notre classe MAP.
public class WAnagramMap extends Mapper<Object, Text, Text, Text>
{
	private Text word = new Text();
	private Text sortedText = new Text();
	private Text orginalText = new Text();
	// La fonction MAP elle-mme.
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			char[] wordChars = word.toString().toCharArray();
			Arrays.sort(wordChars);
			String sortedWord = new String(wordChars);
			sortedText.set(sortedWord);
			orginalText.set(word);
			System.out.println(sortedText+" "+orginalText);
			context.write(sortedText, orginalText);
		}
	}
}
