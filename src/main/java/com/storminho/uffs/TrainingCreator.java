//code inspired in the code posted in http://codereview.stackexchange.com/questions/16154/is-this-code-an-efficient-implementation-of-reservoir-sampling (accessed on 24th November 2016)

package com.storminho.uffs;

import java.util.List;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import java.io.PrintStream;

public class TrainingCreator {

    public static void main(String[] args) throws FileNotFoundException {
        PrintStream ps = new PrintStream(Variables.csvPath + Variables.trainingOutputFile);
        List<String> trainingLines = sampler(Variables.trainingLinesNumber);

        for(String element : trainingLines) {
            ps.println(element);
        }
        ps.flush();
    }

    public static List<String> sampler (int reservoirSize) throws FileNotFoundException {
        String currentLine = null;
        //reservoirList is where our selected lines stored
        List <String> reservoirList= new ArrayList<String>(reservoirSize);
        // we will use this counter to count the current line number while iterating
        int count = 0;

        Random ra = new Random();
        int randomNumber = 0;
        Scanner sc = new Scanner(new File(Variables.csvPath + Variables.trainingCsvFile)).useDelimiter("\n");
        while (sc.hasNext()) {
            currentLine = sc.next();
            count ++;
            if (count <= reservoirSize) {
                reservoirList.add(currentLine);
            }
            else if ((randomNumber = (int) ra.nextInt(count)) < reservoirSize) {
                reservoirList.set(randomNumber, currentLine);
            }
        }
        return reservoirList;
    }
}
