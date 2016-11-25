package com.storminho.uffs;

import java.io.BufferedReader;
import java.io.FileReader;

public class Comparator {
    public static void main (String[] args) {
        // int qtdFiles = Integer.parseInt(args[0]); //quantidade de arquivos que vai checar
        int qtdFiles = 1; //for now we gonna use only 1 file
        int fp = 0, fn = 0, vp = 0, vn = 0;
        for (int i = 0; i < qtdFiles; i++) {
            try {
                BufferedReader gabarito = new BufferedReader(new FileReader(GlobalVariables.projectPath + "/out/" + i + "-gabarito.out"));
                BufferedReader arvore = new BufferedReader(new FileReader(GlobalVariables.projectPath + "/out/" + i + "-arvore.out"));
                for (String gabs = gabarito.readLine(), arvs = arvore.readLine(); gabs != null; gabs = gabarito.readLine(), arvs = arvore.readLine()) {
                    boolean gab = (Integer.parseInt(gabs) == 1);
                    boolean arv = (Integer.parseInt(arvs) == 1);
                    if (gab && arv) vp++;
                    if (!gab && arv) fp++;
                    if (gab && !arv) fn++;
                    if (!gab && !arv) vn++;
                }
                gabarito.close(); arvore.close();
            } catch (Exception e) { System.out.println(e); }
        }
        System.out.println("Resultado:\nFalso-Positivos: " + fp + "\nFalso-Negativos: " + fn + "\nVerdadeiro-Positivos: " + vp + "\nVerdadeiro-Negativos: " + vn);
    }
}
