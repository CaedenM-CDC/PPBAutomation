package com.biomatters.ppbAutomation;

import com.biomatters.geneious.publicapi.databaseservice.DatabaseServiceException;
import com.biomatters.geneious.publicapi.databaseservice.WritableDatabaseService;
import com.biomatters.geneious.publicapi.documents.AnnotatedPluginDocument;
import com.biomatters.geneious.publicapi.documents.DocumentField;
import com.biomatters.geneious.publicapi.documents.DocumentUtilities;
import com.biomatters.geneious.publicapi.documents.PluginDocument;
import com.biomatters.geneious.publicapi.documents.sequence.NucleotideSequenceDocument;
import com.biomatters.geneious.publicapi.documents.sequence.SequenceAlignmentDocument;
import com.biomatters.geneious.publicapi.plugin.*;
import com.biomatters.geneious.publicapi.utilities.ImportUtilities;
import jebl.util.ProgressListener;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.biomatters.ppbAutomation.CommonGeneiousKeys.*;

public class HelperFunctions {
    private HelperFunctions() {
        // Do nothing.
    }

    private static HashMap<String, GeneiousPlugin> geneiousPluginsPerNameMap = null;

    public static GeneiousPlugin getGeneiousPlugin(
        String geneiousPluginName,
        boolean throwExceptionIfNotFound
    ) throws DocumentOperationException {
        if (geneiousPluginsPerNameMap == null) {
            geneiousPluginsPerNameMap = new HashMap<>();

            List<GeneiousPlugin> activeGeneiousPlugins = PluginUtilities.getActiveGeneiousPlugins();
            for (GeneiousPlugin activeGeneiousPlugin : activeGeneiousPlugins) {
                geneiousPluginsPerNameMap.put(
                    activeGeneiousPlugin.getName(),
                    activeGeneiousPlugin
                );
            }
        }
        if (!geneiousPluginsPerNameMap.containsKey(geneiousPluginName) && throwExceptionIfNotFound) {
            throw new DocumentOperationException(String.format(
                "Geneious plugin \"%s\" must be installed and active, but is not.",
                geneiousPluginName
            ));
        }
        return geneiousPluginsPerNameMap.get(geneiousPluginName);
    }

    public static void validateAndCopyReferenceGenomes(
        Collection<WritableDatabaseServiceDatum> writableDatabaseServiceData,
        String defaultReferenceAccessionNumber,
        HashMap<String, AnnotatedPluginDocument> referenceGenomesPerAccessionNumber,
        ProgressListener progressListener
    ) throws DocumentOperationException, DatabaseServiceException {
        AtomicInteger i = new AtomicInteger(0);
        AtomicInteger totalNumberOfReferenceGenomes = new AtomicInteger(0);
        writableDatabaseServiceData.forEach(
            writableDatabaseServiceDatum -> totalNumberOfReferenceGenomes.addAndGet(Math.max(
                1,
                writableDatabaseServiceDatum.referenceGenomeAccessionNumbers.size()
            ))
        );
//        CompositeProgressListener compositeProgressListener = new CompositeProgressListener(
//            progressListener,
//            numberOfSubtasks.get()
//        );
        for (WritableDatabaseServiceDatum writableDatabaseServiceDatum : writableDatabaseServiceData) {
            if (writableDatabaseServiceDatum.referenceGenomeAccessionNumbers.size() == 0) {
                writableDatabaseServiceDatum.referenceGenomeAccessionNumbers.add(defaultReferenceAccessionNumber);
            }
            for (String referenceGenomeAccessionNumber : writableDatabaseServiceDatum.referenceGenomeAccessionNumbers) {
                if (!referenceGenomesPerAccessionNumber.containsKey(referenceGenomeAccessionNumber)) {
                    throw new DocumentOperationException(String.format(
                        "Reference genome \"%s\" was not included as an input document.",
                        referenceGenomeAccessionNumber
                    ));
                }
                AnnotatedPluginDocument referenceGenome = referenceGenomesPerAccessionNumber.get(referenceGenomeAccessionNumber);
                writableDatabaseServiceDatum.writableDatabaseService.addDocumentCopy(
                    referenceGenome,
                    ProgressListener.EMPTY
                );
                writableDatabaseServiceDatum.referenceGenomes.add(referenceGenome);
                String message = String.format(
                    "Copied reference genome \"%s\" into folder \"%s\" (%d / %d)",
                    referenceGenome.getName(),
                    writableDatabaseServiceDatum.writableDatabaseService.getName(),
                    i.incrementAndGet(),
                    totalNumberOfReferenceGenomes.get()
                );
                progressListener.setMessage(message);
                System.out.println(message);
            }
        }
    }

    public static void importFastqFiles(
        Collection<WritableDatabaseServiceDatum> writableDatabaseServiceDataWithFastqFiles,
        ExecutorService executorService,
        ProgressListener progressListener
    ) throws InterruptedException, ExecutionException {
        // Download and copy FASTQ files.
        List<Callable<Void>> tasks = new LinkedList<>();
        AtomicInteger i = new AtomicInteger(0);
        AtomicInteger fastqFileCount = new AtomicInteger(0);
        writableDatabaseServiceDataWithFastqFiles.forEach(
            writableDatabaseServiceDatum -> fastqFileCount.addAndGet(writableDatabaseServiceDatum.rawFastqFiles.size())
        );
        for (WritableDatabaseServiceDatum writableDatabaseServiceDatum : writableDatabaseServiceDataWithFastqFiles) {
            tasks.add(() -> {
                for (File rawFastqFile : writableDatabaseServiceDatum.rawFastqFiles) {
                    WritableDatabaseService writableDatabaseService = writableDatabaseServiceDatum.writableDatabaseService;
//                    compositeProgressListener.beginSubtask(String.format(
//                        "Import %s",
//                        rawFastqFile.getName()
//                    ));
                    List<AnnotatedPluginDocument> importedAnnotatedPluginDocuments = new LinkedList<>();
                    ImportUtilities.importDocuments(
                        rawFastqFile,
                        new DocumentFileImporter.ImportCallback() {
                            @Override
                            public AnnotatedPluginDocument addDocument(PluginDocument pluginDocument) {
                                return null;
                            }

                            @Override
                            public AnnotatedPluginDocument addDocument(AnnotatedPluginDocument annotatedPluginDocument) {
                                importedAnnotatedPluginDocuments.add(annotatedPluginDocument);
                                return annotatedPluginDocument;
                            }
                        },
                        ImportUtilities.ActionWhenInvalid.ReplaceInvalidBases,
                        ProgressListener.EMPTY
                    );
//                    compositeProgressListener.beginNextSubtask(String.format(
//                        "Copy FASTQ file into \"%s\" folder.",
//                        writableDatabaseService.getName()
//                    ));
                    for (AnnotatedPluginDocument importedAnnotatedPluginDocument : importedAnnotatedPluginDocuments) {
                        writableDatabaseService.addDocumentCopy(
                            importedAnnotatedPluginDocument,
                            ProgressListener.EMPTY
                        );
                    }
                    writableDatabaseServiceDatum.importedFastqFiles.addAll(importedAnnotatedPluginDocuments);
                    String message = String.format(
                        "Imported \"%s\" into folder \"%s\" (%d / %d)",
                        rawFastqFile.getName(),
                        writableDatabaseService.getName(),
                        i.incrementAndGet(),
                        fastqFileCount.get()
                    );
                    progressListener.setMessage(message);
                    System.out.println(message);
                }
                return null;
            });
        }
        List<Future<Void>> taskReturnValues = executorService.invokeAll(tasks);
        for (Future<Void> taskReturnValue : taskReturnValues) {
            taskReturnValue.get();
        }
    }

    public static void performAssemblies(
        Collection<WritableDatabaseServiceDatum> writableDatabaseServiceDataWithFastqFiles,
        ExecutorService executorService,
        WritableDatabaseServiceDatum writableDatabaseServiceDatumPerRun,
        boolean calculateContigsFlag,
        ProgressListener progressListener
    ) throws IOException, DocumentImportException, DatabaseServiceException, DocumentOperationException, InterruptedException, ExecutionException {
        GeneiousPlugin geneiousAssemblerPlugin = getGeneiousPlugin(
            GENEIOUS_ASSEMBLER_PLUGIN_NAME,
            true
        );
        Assembler[] geneiousAssemblers = geneiousAssemblerPlugin.getAssemblers();
        Assembler geneiousAssembler = geneiousAssemblers[1];
//        CompositeProgressListener compositeProgressListener = new CompositeProgressListener(
//            progressListener,
//            numberOfSubtasks
//        );
        Set<AnnotatedPluginDocument> allReferenceGenomes = new HashSet<>();
        AtomicInteger i = new AtomicInteger(0);
        AtomicInteger totalNumberOfReferenceGenomes = new AtomicInteger(0);
        writableDatabaseServiceDataWithFastqFiles.forEach(
            writableDatabaseServiceDatum -> totalNumberOfReferenceGenomes.addAndGet(writableDatabaseServiceDatum.referenceGenomes.size())
        );
        // Assemble FASTQ files to reference genomes.
        List<Callable<Void>> tasks = new LinkedList<>();
        for (WritableDatabaseServiceDatum writableDatabaseServiceDatum : writableDatabaseServiceDataWithFastqFiles) {
            for (AnnotatedPluginDocument referenceGenome : writableDatabaseServiceDatum.referenceGenomes) {
                allReferenceGenomes.add(referenceGenome);
                if (!calculateContigsFlag) {
                    continue;
                }
                if (writableDatabaseServiceDatum.importedFastqFiles.size() == 0) {
                    continue;
                }
                long numberOfSequencesExcludingReferences = 0;
                for (AnnotatedPluginDocument importedFastqFile : writableDatabaseServiceDatum.importedFastqFiles) {
                    numberOfSequencesExcludingReferences += (int)importedFastqFile.getFieldValue(DocumentField.NUCLEOTIDE_SEQUENCE_COUNT);
                }

                AssemblerInput.Properties assemblerInputProperties = new AssemblerInput.Properties(numberOfSequencesExcludingReferences);
                Options geneiousAssemblerOptions = geneiousAssembler.getOptions(
                    null,
                    assemblerInputProperties
                );
                List<AssemblerInput.ReferenceSequence> referenceSequences = List.of(new AssemblerInput.ReferenceSequence(
                    referenceGenome,
                    -1
                ));
                List<AnnotatedPluginDocument> documentsToBeAssembled = new LinkedList<>();
                documentsToBeAssembled.add(referenceGenome);
                documentsToBeAssembled.addAll(writableDatabaseServiceDatum.importedFastqFiles);
                AssemblerInput geneiousAssemblerInput = new AssemblerInput(
                    documentsToBeAssembled,
                    referenceSequences,
                    false
                );
                String sampleName = writableDatabaseServiceDatum.sampleName;
                String contigDocumentName = String.format(
                    "%s - [TYPE] - Final Contig",
                    sampleName == null ? "[SAMPLE_NAME]" : sampleName
                );
                String folderName = String.format(
                    "%s - Final Contig",
                    referenceGenome.getName()
                );
                List<AnnotatedPluginDocument> outputContigDocuments = new LinkedList<>();
                tasks.add(() -> {
                    geneiousAssembler.assemble(
                        geneiousAssemblerOptions,
                        geneiousAssemblerInput,
                        null,
                        new Assembler.Callback() {
                            @Override
                            public void addContigDocument(
                                SequenceAlignmentDocument contig,
                                NucleotideSequenceDocument contigConsensus,
                                boolean isThisTheOnlyContigGeneratedByDeNovoAssembly,
                                ProgressListener progressListener
                            ) {
                                outputContigDocuments.add(DocumentUtilities.createAnnotatedPluginDocument(contig));
                            }

                            @Override
                            public void addUnusedRead(
                                AssemblerInput.Read read,
                                ProgressListener progressListener
                            ) {
                                // Do nothing.
                            }
                        }
                    );
                    WritableDatabaseService writableDatabaseServiceForContig = writableDatabaseServiceDatum.writableDatabaseService.createChildFolder(folderName);
                    for (AnnotatedPluginDocument outputContigDocument : outputContigDocuments) {
                        // I'm trying to do this in multiple ways, because it isn't working reliably.
                        // It might be necessary to set a delay in order for this to work reliably.
                        outputContigDocument.setName(contigDocumentName);
                        AnnotatedPluginDocument documentCopy = writableDatabaseServiceForContig.addDocumentCopy(
                            outputContigDocument,
                            null
                        );
                        documentCopy.setName(contigDocumentName);
                    }
                    String message = String.format(
                        "Calculated \"%s\" in folder \"%s\" (%d / %d)",
                        contigDocumentName,
                        folderName,
                        i.incrementAndGet(),
                        totalNumberOfReferenceGenomes.get()
                    );
                    progressListener.setMessage(message);
                    System.out.println(message);
                    return null;
                });
            }
        }
        List<Future<Void>> taskReturnValues = executorService.invokeAll(tasks);
        for (Future<Void> taskReturnValue : taskReturnValues) {
            taskReturnValue.get();
        }

        WritableDatabaseService writableDatabaseServicePerFinalAssemblies = writableDatabaseServiceDatumPerRun.writableDatabaseService.createChildFolder(FINAL_ASSEMBLIES_FOLDER_NAME);
        WritableDatabaseServiceDatum writableDatabaseServiceDatumPerFinalAssemblies = new WritableDatabaseServiceDatum(writableDatabaseServicePerFinalAssemblies);
        writableDatabaseServiceDatumPerRun.children.put(
            FINAL_ASSEMBLIES_FOLDER_NAME,
            writableDatabaseServiceDatumPerFinalAssemblies
        );
        WritableDatabaseService writableDatabaseServicePerAlignments = writableDatabaseServicePerFinalAssemblies.createChildFolder(ALIGNMENTS_FOLDER_NAME);
        WritableDatabaseServiceDatum writableDatabaseServiceDatumPerAlignments = new WritableDatabaseServiceDatum(writableDatabaseServicePerAlignments);
        writableDatabaseServiceDatumPerFinalAssemblies.children.put(
            ALIGNMENTS_FOLDER_NAME,
            writableDatabaseServiceDatumPerAlignments
        );
        WritableDatabaseService writableDatabaseServicePerReferences = writableDatabaseServicePerFinalAssemblies.createChildFolder(REFERENCES_FOLDER_NAME);
        WritableDatabaseServiceDatum writableDatabaseServiceDatumPerReferences = new WritableDatabaseServiceDatum(writableDatabaseServicePerReferences);
        writableDatabaseServiceDatumPerFinalAssemblies.children.put(
            REFERENCES_FOLDER_NAME,
            writableDatabaseServiceDatumPerReferences
        );

        for (WritableDatabaseService writableDatabaseService : List.of(
            writableDatabaseServicePerFinalAssemblies,
            writableDatabaseServicePerReferences
        )) {
            for (AnnotatedPluginDocument referenceGenome : allReferenceGenomes) {
                writableDatabaseService.addDocumentCopy(
                    referenceGenome,
                    null
                );
            }
        }
    }
}
