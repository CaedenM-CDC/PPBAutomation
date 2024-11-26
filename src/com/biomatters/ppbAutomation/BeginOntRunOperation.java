package com.biomatters.ppbAutomation;

import com.biomatters.geneious.publicapi.databaseservice.DatabaseService;
import com.biomatters.geneious.publicapi.databaseservice.DatabaseServiceException;
import com.biomatters.geneious.publicapi.databaseservice.WritableDatabaseService;
import com.biomatters.geneious.publicapi.documents.AnnotatedPluginDocument;
import com.biomatters.geneious.publicapi.documents.PluginDocument;
import com.biomatters.geneious.publicapi.documents.sequence.NucleotideSequenceDocument;
import com.biomatters.geneious.publicapi.plugin.*;
import jebl.util.ProgressListener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.biomatters.ppbAutomation.CommonGeneiousKeys.*;
import static com.biomatters.ppbAutomation.HelperFunctions.*;

public class BeginOntRunOperation extends DocumentOperation {
    private static final String HELP = "Initializes an ONT run for Bioinformaticians working in PPB.";
    private static final String REPORT_ONT_FASTQ_FILE_PATHS_PYTHON_SCRIPT_NAME = "report_ont_fastq_file_paths";

    @Override
    public GeneiousActionOptions getActionOptions() {
        GeneiousActionOptions options = new GeneiousActionOptions("Begin ONT Run");
        options.setMainMenuLocation(GeneiousActionOptions.MainMenu.Import);
        options.setInMainToolbar(true);
        return options;
    }

    @Override
    public String getHelp() { return HELP; }

    @Override
    public DocumentSelectionSignature[] getSelectionSignatures() {
        // This is not working for me. I am going to try to do this manually.
        return new DocumentSelectionSignature[] {
            new DocumentSelectionSignature(
                PluginDocument.class,
                1,
                Integer.MAX_VALUE
            )
        };
    }

    @Override
    public Options getOptions(AnnotatedPluginDocument... documents) {
        Options options = new Options(getClass());
        options.addStringOption(
            FULL_RUN_NAME_KEY,
            FULL_RUN_NAME_LABEL,
            ""
        );
        options.addFileSelectionOption(
            WET_LAB_EXCEL_FILE_PATH_KEY,
            WET_LAB_EXCEL_FILE_PATH_LABEL,
            ""
        );
        options.addBooleanOption(
            CALCULATE_CONTIGS_FLAG_KEY,
            CALCULATE_CONTIGS_FLAG_LABEL,
            DEFAULT_CALCULATE_CONTIGS_FLAG
        );
        options.addStringOption(
            DEFAULT_ACCESSION_NUMBER_KEY,
            DEFAULT_ACCESSION_NUMBER_LABEL,
            DEFAULT_ACCESSION_NUMBER
        );
        options.addIntegerOption(
            NUMBER_OF_THREADS_KEY,
            NUMBER_OF_THREADS_LABEL,
            DEFAULT_NUMBER_OF_THREADS,
            MINIMUM_NUMBER_OF_THREADS,
            MAXIMUM_NUMBER_OF_THREADS
        );
        options.addFileSelectionOption(
            PYTHON_SCRIPTS_FOLDER_PATH_KEY,
            PYTHON_SCRIPTS_FOLDER_PATH_LABEL,
            ""
        );
        return options;
    }

    @Override
    public List<AnnotatedPluginDocument> performOperation(
        AnnotatedPluginDocument[] documents,
        ProgressListener progressListener,
        Options options
    ) throws DocumentOperationException {
        long startTimeMs = System.currentTimeMillis();
        List<AnnotatedPluginDocument> returnValue = new LinkedList<>();
        String fullRunName = options.getValueAsString(FULL_RUN_NAME_KEY);
        Path wetLabExcelFilePath = Paths.get(options.getValueAsString(WET_LAB_EXCEL_FILE_PATH_KEY));
        int numberOfThreads = (Integer)options.getValue(NUMBER_OF_THREADS_KEY);
        if (!Files.exists(wetLabExcelFilePath)) {
            throw new DocumentOperationException(String.format(
                "Input wet-lab excel-file path \"%s\" does not exist.",
                wetLabExcelFilePath
            ));
        }
        if (Files.isDirectory(wetLabExcelFilePath)) {
            throw new DocumentOperationException(String.format(
                "Input wet-lab excel-file path \"%s\" is a directory, but should be an Excel file.",
                wetLabExcelFilePath
            ));
        }
        Path pythonScriptsFolderPath = Paths.get(options.getValueAsString(PYTHON_SCRIPTS_FOLDER_PATH_KEY));
        if (!Files.exists(pythonScriptsFolderPath)) {
            throw new DocumentOperationException(String.format(
                "Input python-scripts folder path \"%s\" does not exist.",
                pythonScriptsFolderPath
            ));
        }
        if (!Files.isDirectory(pythonScriptsFolderPath)) {
            throw new DocumentOperationException(String.format(
                "Input python-scripts folder path \"%s\" is not a directory.",
                pythonScriptsFolderPath
            ));
        }
        String defaultAccessionNumber = options.getValueAsString(DEFAULT_ACCESSION_NUMBER_KEY);
        boolean calculateContigsFlag = (boolean)options.getValue(CALCULATE_CONTIGS_FLAG_KEY);

        HashMap<String, AnnotatedPluginDocument> referenceGenomesPerAccessionNumber = new HashMap<>();
        for (AnnotatedPluginDocument document : documents) {
            PluginDocument pluginDocument = document.getDocument();
            if (!(pluginDocument instanceof NucleotideSequenceDocument)) {
                continue;
            }
            referenceGenomesPerAccessionNumber.put(
                    pluginDocument.getName(),
                    document
            );
        }

        AnnotatedPluginDocument document0 = documents[0];
        DatabaseService originalDatabaseService = document0.getDatabase();
        if (!(originalDatabaseService instanceof WritableDatabaseService)) {
            throw new DocumentOperationException(String.format(
                "The original database service \"%s\" is not writable.",
                originalDatabaseService.getName()
            ));
        }
        WritableDatabaseServiceDatum rootWritableDatabaseServiceDatum = new WritableDatabaseServiceDatum((WritableDatabaseService)originalDatabaseService);
        LinkedHashSet<WritableDatabaseServiceDatum> writableDatabaseServiceDataForSamples = new LinkedHashSet<>();
        try {
            WritableDatabaseService writableDatabaseServicePerRun = rootWritableDatabaseServiceDatum.writableDatabaseService.createChildFolder(fullRunName);
            WritableDatabaseServiceDatum writableDatabaseServiceDatumPerRun = new WritableDatabaseServiceDatum(writableDatabaseServicePerRun);
            rootWritableDatabaseServiceDatum.children.put(
                fullRunName,
                writableDatabaseServiceDatumPerRun
            );
            Path temporaryDirectoryPath = Files.createTempDirectory("networkDumpFolderForGeneious");
            AtomicInteger numberOfSubtasks = new AtomicInteger();
            PythonProcess pythonProcess = new PythonProcess(
                REPORT_ONT_FASTQ_FILE_PATHS_PYTHON_SCRIPT_NAME,
                pythonScriptsFolderPath.toAbsolutePath().toString(),
                (String pythonOutputLine) -> {
                    String[] splitResults = pythonOutputLine.split("=>");
                    String sampleName = splitResults[0];
                    String fastqFilePathString = splitResults[1];
                    WritableDatabaseServiceDatum writableDatabaseServiceDatumPerSample;
                    if (writableDatabaseServiceDatumPerRun.children.containsKey(sampleName)) {
                        writableDatabaseServiceDatumPerSample = writableDatabaseServiceDatumPerRun.children.get(sampleName);
                    } else {
                        writableDatabaseServiceDatumPerSample = new WritableDatabaseServiceDatum(
                            writableDatabaseServicePerRun.createChildFolder(sampleName),
                            sampleName
                        );
                        writableDatabaseServiceDatumPerRun.children.put(
                            sampleName,
                            writableDatabaseServiceDatumPerSample
                        );
                        writableDatabaseServiceDataForSamples.add(writableDatabaseServiceDatumPerSample);
                    }
                    Path fastqFilePath = Paths.get(fastqFilePathString);
                    Path copiedFilePath = temporaryDirectoryPath.resolve(fastqFilePath.getFileName());
                    if (Files.size(fastqFilePath) != 0) {
                        Files.copy(
                            fastqFilePath,
                            copiedFilePath
                        );
                        writableDatabaseServiceDatumPerSample.rawFastqFiles.add(copiedFilePath.toFile());
                        numberOfSubtasks.addAndGet(2);
                    }
                }
            );
            Integer exitValue = pythonProcess.execute(
                true,
                String.format(
                    "-r %s -e %s",
                    fullRunName,
                    wetLabExcelFilePath
                )
            );
            if (exitValue == null) {
                throw new DocumentOperationException("exitValue should never be null.");
            }
            pythonProcess.validateExitValue(exitValue);

            ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
            validateAndCopyReferenceGenomes(
                writableDatabaseServiceDataForSamples,
                defaultAccessionNumber,
                referenceGenomesPerAccessionNumber,
                progressListener
            );
            importFastqFiles(
                writableDatabaseServiceDataForSamples,
                executorService,
                progressListener
            );
            performAssemblies(
                writableDatabaseServiceDataForSamples,
                executorService,
                writableDatabaseServiceDatumPerRun,
                calculateContigsFlag,
                progressListener
            );
            long endTimeMs = System.currentTimeMillis();
            long elapsedTimeMs = endTimeMs - startTimeMs;
            long elapsedTimeS = elapsedTimeMs / 1000;
            System.out.printf(
                "Elapsed time: %d min, %d sec%n",
                elapsedTimeS / 60,
                elapsedTimeS % 60
            );
        } catch (InterruptedException | IOException | DatabaseServiceException | ExecutionException | DocumentImportException exception) {
            throw new DocumentOperationException(exception);
        }

        return returnValue;
    }
}
