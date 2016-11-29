//faz a combinação quadratica de todos os registros de um determinado arquivo csv para criar um novo arquivo a ser lido pelo PairSpout
package com.storminho.uffs.tests;

import com.storminho.uffs.Variables;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;

public class CSVPairCreator {

    public static void main (String[] args) {
        BufferedReader reader;
        PrintStream ps;
        ArrayList<String> list;
        String line = null;
        list = new ArrayList();
        try {
            reader = new BufferedReader(new FileReader(Variables.csvPath + "cd-100.csv"));
            ps = new PrintStream(Variables.csvPath + "cd-100-pairs.csv");
            for (line = reader.readLine(); line != null; line = reader.readLine()) {
                list.add(line);
                for (String str : list) {
                    ps.println(line + "\n" + str);
                }
                ps.flush();
            }
            ps.close();
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}
