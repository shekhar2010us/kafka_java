package com.shekhar.kafka.basic;

import java.util.List;

public class Test {

    public static void main(String[] args) {
        ReadResourceFile readResourceFile = new ReadResourceFile();
        String fileName = "producerkey.txt";
        List<String> lines = readResourceFile.read(fileName);
        for (String line:lines) {
            System.out.println(line);
        }
    }
}
