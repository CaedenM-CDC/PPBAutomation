package com.biomatters.ppbAutomation;

import com.biomatters.geneious.publicapi.databaseservice.DatabaseService;
import com.biomatters.geneious.publicapi.databaseservice.DatabaseServiceException;
import com.biomatters.geneious.publicapi.databaseservice.WritableDatabaseService;
import com.biomatters.geneious.publicapi.documents.AnnotatedPluginDocument;
import com.biomatters.geneious.publicapi.documents.PluginDocument;
import com.biomatters.geneious.publicapi.documents.sequence.NucleotideSequenceDocument;
import com.biomatters.geneious.publicapi.plugin.*;
import jebl.util.ProgressListener;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.biomatters.ppbAutomation.CommonGeneiousKeys.*;
import static com.biomatters.ppbAutomation.HelperFunctions.*;

public class BeginIlluminaRunOperation extends DocumentOperation {
    static final String HELP = "Initializes an Illumina run for Bioinformaticians working in PPB";
    static final String JSON = "json";
    static final String FASTQ = "fastq";
    static final String GET_GENOME_TYPES_FROM_VPIPE_PYTHON_SCRIPT_NAME = "get_genome_types_from_vpipe_json_file";
    static final List<String> FILE_TYPES = List.of(JSON, FASTQ);

    public GeneiousActionOptions getActionOptions() {
        GeneiousActionOptions options = new GeneiousActionOptions("Begin Illumina Run");
        options.setMainMenuLocation(GeneiousActionOptions.MainMenu.Import);
        options.setInMainToolbar(true);
        return options;
    }

    public String getHelp() {
        return HELP;
    }

    public Options getOptions(AnnotatedPluginDocument... documents) {
        Options options = new Options(getClass());
        options.addStringOption(
            FULL_RUN_NAME_KEY,
            FULL_RUN_NAME_LABEL,
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
        options.addFileSelectionOption(
            ILLUMINA_NETWORK_PREFIX_KEY,
            "Illumina network prefix",
            DEFAULT_ILLUMINA_NETWORK_PREFIX
        );
        options.addStringOption(
            ILLUMINA_NETWORK_SUFFIX_KEY,
            "Illumina network suffix",
            DEFAULT_ILLUMINA_NETWORK_SUFFIX
        );
        options.addStringOption(
            ILLUMINA_FASTQ_REGEX_KEY,
            "Illumina .fastq regex",
            DEFAULT_ILLUMINA_FASTQ_REGEX
        );
        options.addStringOption(
            ILLUMINA_JSON_REGEX_KEY,
            "Illumina .json regex",
            DEFAULT_ILLUMINA_JSON_REGEX
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

    interface FileProcessor {
        void processFile(
            Path path,
            WritableDatabaseServiceDatum writableDatabaseServiceDatum
        ) throws DocumentOperationException, IOException, InterruptedException, DatabaseServiceException;
    }

    public List<AnnotatedPluginDocument> performOperation(
        AnnotatedPluginDocument[] documents,
        ProgressListener progressListener,
        Options options
    ) throws DocumentOperationException {
        long startTimeMs = System.currentTimeMillis();
        List<AnnotatedPluginDocument> returnValue = new LinkedList<>();
        // Unpack option values.
        String fullRunName = options.getValueAsString(FULL_RUN_NAME_KEY);
        String defaultAccessionNumber = options.getValueAsString(DEFAULT_ACCESSION_NUMBER_KEY);
        String illuminaNetworkPrefix = options.getValueAsString(ILLUMINA_NETWORK_PREFIX_KEY);
        String illuminaNetworkSuffix = options.getValueAsString(ILLUMINA_NETWORK_SUFFIX_KEY);
        Pattern illuminaFastqRegexPattern = Pattern.compile(options.getValueAsString(ILLUMINA_FASTQ_REGEX_KEY));
        Pattern illuminaJsonRegexPattern = Pattern.compile(options.getValueAsString(ILLUMINA_JSON_REGEX_KEY));
        Path pythonScriptsFolderPath = Paths.get(options.getValueAsString(PYTHON_SCRIPTS_FOLDER_PATH_KEY));
        int numberOfThreads = (Integer)options.getValue(NUMBER_OF_THREADS_KEY);
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
        boolean calculateContigsFlag = (boolean)options.getValue(CALCULATE_CONTIGS_FLAG_KEY);

        AnnotatedPluginDocument document0 = documents[0];
        DatabaseService originalDatabaseService = document0.getDatabase();
        if (!(originalDatabaseService instanceof WritableDatabaseService)) {
            throw new DocumentOperationException(String.format(
                "The database service (aka Geneious folder) in which this plugin was run (\"%s\") is not writable.",
                originalDatabaseService.getName()
            ));
        }
        WritableDatabaseServiceDatum rootWritableDatabaseServiceDatum = new WritableDatabaseServiceDatum((WritableDatabaseService)originalDatabaseService);

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

        Path illuminaNetworkPath = Paths.get(String.join(
            File.separator,
            illuminaNetworkPrefix,
            fullRunName,
            illuminaNetworkSuffix
        ));
        Set<WritableDatabaseServiceDatum> writableDatabaseServiceDataForSamples = new LinkedHashSet<>();
        try (Stream<Path> pathStream = Files.walk(illuminaNetworkPath)) {
            WritableDatabaseService writableDatabaseServicePerRun = rootWritableDatabaseServiceDatum.writableDatabaseService.createChildFolder(fullRunName);
            WritableDatabaseServiceDatum writableDatabaseServiceDatumPerRun = new WritableDatabaseServiceDatum(writableDatabaseServicePerRun);
            rootWritableDatabaseServiceDatum.children.put(
                fullRunName,
                writableDatabaseServiceDatumPerRun
            );
            Path temporaryLocalDirectory = Files.createTempDirectory("networkDumpFolderForGeneious");
            List<Path> paths = pathStream.collect(Collectors.toList());
//            AtomicInteger numberOfSubtasks = new AtomicInteger();
            FileProcessor fastqFileProcessor = (
                Path path,
                WritableDatabaseServiceDatum writableDatabaseServiceDatum
            ) -> {
                Path copyPath = temporaryLocalDirectory.resolve(path.getFileName());
                // This shouldn't be necessary, but importing documents directly from network drives is very slow. This method is faster.
                Files.copy(
                    path,
                    copyPath
                );
//                numberOfSubtasks.addAndGet(2);
                writableDatabaseServiceDatum.rawFastqFiles.add(copyPath.toFile());
            };
            FileProcessor jsonFileProcessor = (
                Path path,
                WritableDatabaseServiceDatum writableDatabaseServiceDatum
            ) -> {
                PythonProcess pythonProcess = new PythonProcess(
                    GET_GENOME_TYPES_FROM_VPIPE_PYTHON_SCRIPT_NAME,
                    pythonScriptsFolderPath.toAbsolutePath().toString(),
                    writableDatabaseServiceDatum.referenceGenomeAccessionNumbers::add
                );
                Integer exitValue = pythonProcess.execute(
                    true,
                    String.format(
                        "-i %s",
                        path.toAbsolutePath()
                    )
                );
                if (exitValue == null) {
                    throw new DocumentOperationException("exitValue should never be null.");
                }
                pythonProcess.validateExitValue(exitValue);
            };
            Map<String, FileProcessor> fileProcessorsPerFileType = Map.of(
                FASTQ, fastqFileProcessor,
                JSON, jsonFileProcessor
            );
            Map<String, Pattern> patternsPerFileType = Map.of(
                JSON, illuminaJsonRegexPattern,
                FASTQ, illuminaFastqRegexPattern
            );
            AtomicInteger i = new AtomicInteger(0);
            AtomicInteger totalNumberOfTasks = new AtomicInteger(0);
            ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
            List<Callable<Void>> tasks = new LinkedList<>();
            for (Path path : paths) {
                if (!Files.isRegularFile(path)) {
                    continue;
                }

                String fileName = path.getFileName().toString();
                for (String fileType : FILE_TYPES) {
                    Matcher fileNameMatcher = patternsPerFileType.get(fileType).matcher(fileName);
                    if (!fileNameMatcher.matches()) {
                        continue;
                    }
                    String sampleName = fileNameMatcher.group(1);
                    sampleName = sampleName.replaceAll("-", "_");
                    WritableDatabaseServiceDatum writableDatabaseServiceDatumPerSample;
                    if (writableDatabaseServiceDatumPerRun.children.containsKey(sampleName)) {
                        writableDatabaseServiceDatumPerSample = writableDatabaseServiceDatumPerRun.children.get(sampleName);
                    } else {
                        WritableDatabaseService writableDatabaseServicePerSample = writableDatabaseServiceDatumPerRun.writableDatabaseService.createChildFolder(sampleName);
                        writableDatabaseServiceDatumPerSample = new WritableDatabaseServiceDatum(
                            writableDatabaseServicePerSample,
                            sampleName
                        );
                        writableDatabaseServiceDatumPerRun.children.put(
                            sampleName,
                            writableDatabaseServiceDatumPerSample
                        );
                    }
                    if (Files.size(path) == 0) {
                        // Ignore empty files.
                        continue;
                    }
                    tasks.add(() -> {
                        fileProcessorsPerFileType.get(fileType).processFile(
                            path,
                            writableDatabaseServiceDatumPerSample
                        );
                        String message = String.format(
                            "Pre-processed VPipe .%s file \"%s\" (%d / %d)",
                            fileType,
                            path.getFileName(),
                            i.incrementAndGet(),
                            totalNumberOfTasks.get()
                        );
                        progressListener.setMessage(message);
                        System.out.println(message);
                        return null;
                    });
                    writableDatabaseServiceDataForSamples.add(writableDatabaseServiceDatumPerSample);
                }
            }
            totalNumberOfTasks.set(tasks.size());
            List<Future<Void>> taskReturnValues = executorService.invokeAll(tasks);
            for (Future<Void> taskReturnValue : taskReturnValues) {
                taskReturnValue.get();
            }
            // This is fast enough that we don't need multiple threads.
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
        } catch (IOException | DocumentImportException | DatabaseServiceException | ExecutionException | InterruptedException exception) {
            exception.printStackTrace();
            throw new DocumentOperationException(exception);
        }
        return returnValue;
    }
}
