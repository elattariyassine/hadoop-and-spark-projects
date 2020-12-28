package org.mbds.hadoop.tp;

import java.util.Arrays;

public class Testing {
    public static void main(String[] args) {
        String word = "yassine";
        char[] arr = word.toCharArray();
        Arrays.sort(arr);
        System.out.println(new String(arr));
    }
}
