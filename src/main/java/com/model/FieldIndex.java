package com.model;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class FieldIndex {

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        BufferedReader br = null;
        try {
            File file = new File(args[0]);
            br = new BufferedReader(new FileReader(file));
            int i=0;
            String line;
            while (i<1 && (line = br.readLine()) != null) {
                String[] fields = line.split(",");
                System.out.println(Arrays.deepToString(fields));
                ArrayList<String> values = new ArrayList<String>(Arrays.asList(fields));
                ArrayList<String> al = new ArrayList<String>();
                al.addAll(values.subList(0, 19));
                al.addAll(values.subList(26,27));
                al.addAll(values.subList(955, 1016));
                al.addAll(values.subList(1018, values.size()));
                System.out.println(al.toString());
//		        for(int j=0 ; j<fields.length;j++)
//		        	System.out.println(fields[j]+":"+j);
                i++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
