package com.shekhar.kafka.basic;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;


public class ReadResourceFile {


    public List<String> read(String fileName) {
        List lines = new ArrayList<String>();
        try {
            File file = getFileFromResources(fileName);
            lines = readFileInList(file);
        } catch (Exception e) {
            System.out.println(e);
        }
        return lines;
    }

    private static File getFileFromResources(String fileName) {

        ClassLoader classLoader = ReadResourceFile.class.getClassLoader();

        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file is not found!");
        } else {
            return new File(resource.getFile());
        }
    }

    private static List<String> readFileInList(File file) throws IOException {
        List lines = new ArrayList<String>();

        if (file == null) {
            return lines;
        }

        try {
            FileReader reader = new FileReader(file);
            BufferedReader br = new BufferedReader(reader); {
                String line;
                while ((line = br.readLine()) != null) {
                    lines.add(line);
                }
            }
        } catch (Exception e) {
            System.err.println(e);
        }

        return lines;
    }

}
