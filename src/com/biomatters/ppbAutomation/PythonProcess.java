package com.biomatters.ppbAutomation;

import com.biomatters.geneious.publicapi.databaseservice.DatabaseServiceException;
import com.biomatters.geneious.publicapi.plugin.DocumentOperationException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

interface PythonOutputHandler {
    void accept(String pythonOutputLine) throws DatabaseServiceException, IOException;
}

public class PythonProcess {
    public final String name;

    public final String containingFolder;

    public final PythonOutputHandler pythonOutputHandler;

    public PythonProcess(
        String name,
        String containingFolder,
        PythonOutputHandler pythonOutputHandler
    ) {
        this.name = name;
        this.containingFolder = containingFolder;
        this.pythonOutputHandler = pythonOutputHandler;
    }

    public final Integer execute(
        boolean waitForFlag,
        String arguments
    ) throws IOException, InterruptedException, DatabaseServiceException {
        String pythonCommand = String.format(
            "python3 %s%s%s.py %s",
            this.containingFolder,
            File.separator,
            this.name,
            arguments
        );
//        System.out.println("pythonCommand: " + pythonCommand);
        Process process = Runtime.getRuntime().exec(pythonCommand);
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                this.pythonOutputHandler.accept(line);
            }
        }
        if (waitForFlag) {
            return process.waitFor();
        } else {
            return null;
        }
    }

    public final void validateExitValue(
        int exitValue
    ) throws DocumentOperationException {
        if (exitValue != 0) {
            throw new DocumentOperationException(String.format(
                "Python script \"%s.py\" had a nonzero exit value.",
                this.name
            ));
        }
    }
}
