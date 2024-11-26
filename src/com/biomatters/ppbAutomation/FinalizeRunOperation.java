package com.biomatters.ppbAutomation;

import com.biomatters.geneious.publicapi.components.Dialogs;
import com.biomatters.geneious.publicapi.databaseservice.DatabaseService;
import com.biomatters.geneious.publicapi.databaseservice.DatabaseServiceException;
import com.biomatters.geneious.publicapi.documents.AnnotatedPluginDocument;
import com.biomatters.geneious.publicapi.documents.DocumentField;
import com.biomatters.geneious.publicapi.documents.DocumentUtilities;
import com.biomatters.geneious.publicapi.documents.PluginDocument;
import com.biomatters.geneious.publicapi.documents.sequence.*;
import com.biomatters.geneious.publicapi.implementations.DefaultAlignmentDocument;
import com.biomatters.geneious.publicapi.implementations.Percentage;
import com.biomatters.geneious.publicapi.implementations.sequence.DefaultNucleotideSequence;
import com.biomatters.geneious.publicapi.plugin.*;
import com.biomatters.geneious.publicapi.utilities.SequenceUtilities;
import jebl.util.CompositeProgressListener;
import jebl.util.ProgressListener;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.biomatters.ppbAutomation.CommonGeneiousKeys.*;
import static com.biomatters.ppbAutomation.HelperFunctions.getGeneiousPlugin;

public class FinalizeRunOperation extends DocumentOperation {
    private static final String GREEN = "GREEN";
    private static final String YELLOW_REDO = "YELLOW-REDO";
    private static final String YELLOW_KEEP = "YELLOW_KEEP";
    private static final String YELLOW_REMOVE = "YELLOW_REMOVE";
    private static final String RED_REDO = "RED-REDO";
    private static final String DECIMAL_PLACES_FORMAT = "%.2f";
    private static final String FINAL_ANALYSIS = "finalAnalysis";
    private static final String DATABASE_DATA = "databaseData";
    private static final String VP1_SNPS = "vp1Snps";
    private static final String INPUT_DOCUMENT = "INPUT_DOCUMENT";
    private static final String MATURE_PEPTIDE_SEQUENCE_ANNOTATION_TYPE = "mat_peptide";
    private static final String MISSING_MATURE_PEPTIDE_ANNOTATION = "MISSING_MATURE_PEPTIDE_ANNOTATION";
    private static final String FINAL_ASSEMBLIES_FOLDER_NAME = "Final Assemblies";
    // 95% confidence, df ~ 1000, source: https://www.mathsisfun.com/data/students-t-test-table.html
    private static final double T_STATISTIC_ONE_TAIL_THRESHOLD = 1.646;
    // Empirically derived heuristic
    private static final double FIVE_PRIME_MINIMUM_SNP_FREQUENCY = 0.07;
    // Empirically derived heuristic
    private static final double THREE_PRIME_MINIMUM_SNP_FREQUENCY = 0.13;
    enum PolioReferenceType {
        PV1("PV1"),
        PV2("PV2"),
        PV3("PV3"),
        NOPV2("nOPV2");

        final String name;
        PolioReferenceType(String name) {
            this.name = name;
        }
    }
    interface FinalAnalysisExcelDatumToString {
        String print(
            String sampleName,
            String type,
            ExcelData excelData
        );
    }
    interface DatabaseDataExcelDatumToString {
        String print(
            DatabaseData databaseDataPerSamplePerType,
            ExcelData excelDataPerSamplePerType
        );
    }
    interface SingularNucleotidePolymorphismExcelDatumToString {
        String print(
            String sampleName,
            String type,
            SingleNucleotidePolymorphism snp
        );
    }
    enum FinalAnalysisExcelColumn {
        COLOR_RESULT_KEY(
            "Color Result",
            "Key",
            (sampleName, type, excelData) -> excelData.calculateColorResultKey(),
            sampleName -> RED_REDO
        ),
        ANALYSIS_KEY(
            "Analysis",
            "Key",
            (sampleName, type, excelData) -> "",
            sampleName -> ""
        ),
        BARCODE_NUMBER(
            "Barcode",
            "Number",
            (sampleName, type, excelData) -> "",
            sampleName -> ""
        ),
        SAMPLE_NAME(
            "Sample",
            "Name",
            (sampleName, type, excelData) -> sampleName,
            sampleName -> sampleName
        ),
        TYPE(
            "Type",
            "",
            (sampleName, type, excelData) -> type,
            sampleName -> "Insufficient amount of viral data"
        ),
        FULL_COVERAGE_LENGTH(
            "F.C.",
            "Length",
            (sampleName, type, excelData) -> Integer.toString(excelData.fullCoverageLength),
            sampleName -> ""
        ),
        AVERAGE_COVERAGE(
            "Avg.",
            "Coverage",
            (sampleName, type, excelData) -> String.format(
                DECIMAL_PLACES_FORMAT,
                excelData.averageCoverage
            ),
            sampleName -> ""
        ),
        PERCENT_FULL_COVERAGE_COVERED_BY_READS(
            "% F.C. Covered",
            "by Reads (i.e. gaps)",
            (sampleName, type, excelData) -> String.format(
                DECIMAL_PLACES_FORMAT,
                excelData.percentFullCoverageCoveredByReads
            ),
            sampleName -> ""
        ),
        PERCENT_PAIRWISE_IDENTITY(
            "% Pairwise",
            "Identity",
            (sampleName, type, excelData) -> String.format(
                DECIMAL_PLACES_FORMAT,
                excelData.percentPairwiseIdentity
            ),
            sampleName -> ""
        ),
        PERCENT_QUERY_COVERAGE(
            "% Query",
            "Coverage",
            (sampleName, type, excelData) -> String.format(
                DECIMAL_PLACES_FORMAT,
                excelData.percentQueryCoverage
            ),
            sampleName -> ""
        ),
        TOP_BLAST_HIT(
            "Top BLAST Hit",
            "",
            (sampleName, type, excelData) -> excelData.topBlastHit,
            sampleName -> ""
        ),
        ACCESSION_NUMBER(
            "Accession",
            "Number",
            (sampleName, type, excelData) -> excelData.accessionNumber,
            sampleName -> ""
        ),
        NOTES(
            "Notes",
            "",
            (sampleName, type, excelData) -> excelData.calculateNotes(),
            sampleName -> ""
        ),
        VP1_SNP(
            "VP1 SNP",
            "",
            (sampleName, type, excelData) -> excelData.calculateVp1SnpsString(),
            sampleName -> "-"
        );

        final String titleLine0;
        final String titleLine1;
        final FinalAnalysisExcelDatumToString excelDatumToString;
        final Function<String, String> nullExcelDatumToString;

        FinalAnalysisExcelColumn(
            String titleLine0,
            String titleLine1,
            FinalAnalysisExcelDatumToString excelDatumToString,
            Function<String, String> nullExcelDatumToString
        ) {
            this.titleLine0 = titleLine0;
            this.titleLine1 = titleLine1;
            this.excelDatumToString = excelDatumToString;
            this.nullExcelDatumToString = nullExcelDatumToString;
        }
    }
    enum DatabaseDataExcelColumn {
        DATABASE_NAME(
            "DB_Name",
            (databaseDataPerSamplePerType, excelDataPerSamplePerType) -> databaseDataPerSamplePerType.dbName,
            databaseDataPerSamplePerType -> databaseDataPerSamplePerType.dbName
        ),
        GENOTYPE_SEQUENCE_NAME(
            "Genotype_Seq",
            (databaseDataPerSamplePerType, excelDataPerSamplePerType) -> databaseDataPerSamplePerType.genotypeSeq,
            databaseDataPerSamplePerType -> databaseDataPerSamplePerType.genotypeSeq
        ),
        PERCENT_IDENTITY(
            "PerID",
            (databaseDataPerSamplePerType, excelDataPerSamplePerType) -> String.format(
                DECIMAL_PLACES_FORMAT + "%%",
                databaseDataPerSamplePerType.percentIdentity * 100
            ),
            databaseDataPerSamplePerType -> String.format(
                DECIMAL_PLACES_FORMAT + "%%",
                databaseDataPerSamplePerType.percentIdentity * 100
            )
        ),
        NUMBER_OF_DIFFERENCES_FROM_THE_GENOTYPE_SEQUENCE(
            "Ntdiff_ofGENO",
            (databaseDataPerSamplePerType, excelDataPerSamplePerType) -> Integer.toString(
                databaseDataPerSamplePerType.nucleotideDifferencesOfGenotypeCount
            ),
            databaseDataPerSamplePerType -> Integer.toString(
                databaseDataPerSamplePerType.nucleotideDifferencesOfGenotypeCount
            )
        ),
        VP1_GAP_COUNT(
            "VP1_Gap_Count",
            (databaseDataPerSamplePerType, excelDataPerSamplePerType) -> excelDataPerSamplePerType.vp1GapCount == null ? "-" : Integer.toString(excelDataPerSamplePerType.vp1GapCount),
            databaseDataPerSamplePerType -> "-"
        ),
        VP1_SNP_COUNT(
            "VP1_SNP_Count",
            (databaseDataPerSamplePerType, excelDataPerSamplePerType) -> excelDataPerSamplePerType.vp1GapCount == null ? "" : Integer.toString(databaseDataPerSamplePerType.nucleotideDifferencesOfGenotypeCount - excelDataPerSamplePerType.vp1GapCount),
            databaseDataPerSamplePerType -> ""
        );

        final String title;
        final DatabaseDataExcelDatumToString excelDatumToString;
        final Function<DatabaseData, String> nullExcelDatumToString;

        DatabaseDataExcelColumn(
            String title,
            DatabaseDataExcelDatumToString excelDatumToString,
            Function<DatabaseData, String> nullExcelDatumToString
        ) {
            this.title = title;
            this.excelDatumToString = excelDatumToString;
            this.nullExcelDatumToString = nullExcelDatumToString;
        }
    }
    enum VP1SnpsExcelColumn {
        SAMPLE_NAME(
            "SAMPLE_NAME",
            (sampleName, type, vp1) -> sampleName
        ),
        TYPE(
            "TYPE",
            (sampleName, type, vp1) -> type
        ),
        REFERENCE_NAME(
            "REFERENCE_NAME",
            (sampleName, type, vp1) -> vp1.reference
        ),
        POSITION(
            "POSITION",
            (sampleName, type, vp1) -> Integer.toString(vp1.index)
        ),
        REF(
            "REF",
            (sampleName, type, vp1) -> Character.toString(vp1.ref)
        ),
        ALT(
            "ALT",
            (sampleName, type, vp1) -> Character.toString(vp1.alt)
        ),
        CONCATENATION(
            "CONCATENATION",
            (sampleName, type, vp1) -> String.format(
                "%s%d%s",
                vp1.ref,
                vp1.index,
                vp1.alt
            )
        );

        final String title;
        final SingularNucleotidePolymorphismExcelDatumToString snpToString;

        VP1SnpsExcelColumn(
            String title,
            SingularNucleotidePolymorphismExcelDatumToString snpToString
        ) {
            this.title = title;
            this.snpToString = snpToString;
        }
    }
    enum NonFatalErrorCategory {
        GENOME("genomes"),
        ALIGNMENT("alignments"),
        CONTIG("contigs");

        final String name;

        NonFatalErrorCategory(
            String name
        ) {
            this.name = name;
        }
    }
    enum NonFatalError {
        GENOMES_WITHOUT_ANNOTATIONS(
            "no annotations",
            NonFatalErrorCategory.GENOME
        ),
        CONTIGS_WITHOUT_A_CDS_ANNOTATION(
            "no CDS annotation",
            NonFatalErrorCategory.CONTIG
        ),
        GENOMES_WITHOUT_A_CDS_ANNOTATION(
            "no CDS annotation",
            NonFatalErrorCategory.GENOME
        ),
        CONTIGS_WITH_AN_UNEXPECTED_NUMBER_OF_CDS_ANNOTATION_INTERVALS(
            "an unexpected number of \"CDS\" annotation intervals (# != 1)",
            NonFatalErrorCategory.GENOME
        ),
        GENOMES_WITHOUT_A_VP1_ANNOTATION(
            "no VP1 annotation",
            NonFatalErrorCategory.GENOME
        ),
        CONTIGS_WITHOUT_A_VP1_ANNOTATION(
            "no VP1 annotation",
            NonFatalErrorCategory.GENOME
        ),
        GENOMES_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS(
            "an unexpected number of \"VP1\" annotation intervals (# != 1)",
            NonFatalErrorCategory.GENOME
        ),
        CONTIGS_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS(
            "an unexpected number of \"VP1\" annotation intervals (# != 1)",
            NonFatalErrorCategory.CONTIG
        ),
        ALIGNMENTS_WITHOUT_REFERENCE_SEQUENCES(
            "no reference sequence",
            NonFatalErrorCategory.ALIGNMENT
        ),
        ALIGNMENTS_WITHOUT_A_VP1_ANNOTATION(
            "no \"VP1\" annotation",
            NonFatalErrorCategory.ALIGNMENT
        ),
        ALIGNMENTS_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS(
            "an unexpected number of \"VP1\" annotation intervals (# != 1)",
            NonFatalErrorCategory.ALIGNMENT
        ),
        ALIGNMENTS_WITHOUT_A_CDS_ANNOTATION(
            "no \"CDS\" annotation",
            NonFatalErrorCategory.ALIGNMENT
        ),
        ALIGNMENTS_WITH_AN_UNEXPECTED_NUMBER_OF_CDS_ANNOTATION_INTERVALS(
            "an unexpected number of \"CDS\" annotation intervals (# != 1)",
            NonFatalErrorCategory.ALIGNMENT
        ),
        GENOMES_WITH_AN_UNEXPECTED_NUMBER_OF_CDS_ANNOTATION_INTERVALS(
            "an unexpected number of \"CDS\" annotation intervals (# != 1)",
            NonFatalErrorCategory.GENOME
        ),
        GENOMES_WITH_AN_UNEXPECTED_NUMBER_OF_MATURE_PEPTIDE_ANNOTATION_INTERVALS(
            "an unexpected number of \"mat_peptide\" annotation intervals (# != 1)",
            NonFatalErrorCategory.GENOME
        ),
        GENOMES_WITH_NO_CORRESPONDING_FILES_WITHIN_THE_FINAL_ASSEMBLIES_FOLDER(
            String.format(
                "no corresponding files within the \"%s\" folder, but were found within an alignment",
                FINAL_ASSEMBLIES_FOLDER_NAME
            ),
            NonFatalErrorCategory.GENOME
        );

        final String errorDescription;
        final NonFatalErrorCategory category;
        final List<String> names;

        NonFatalError(
            String errorDescription,
            NonFatalErrorCategory category
        ) {
            this.errorDescription = errorDescription;
            this.category = category;
            this.names = new LinkedList<>();
        }
    }
    public static final String HELP = "Finalizes NGS runs for Bioinformaticians working in PPB.";
    private static final String SHORTENED_RUN_NAME_KEY = "shortenedRunName";
    private static final String EXPORT_FINAL_ANALYSIS_FOLDER_AND_FILES_FLAG_KEY = "exportFinalAnalysisFolderAndFilesFlag";
    private static final String UPLOAD_EXCEL_TO_SHAREPOINT_FLAG_KEY = "uploadExcelToSharepointFlag";
    private static final String GENERATE_EMAIL_FLAG_KEY = "generateEmailFlag";
    private static final String BIOINFORMATICIAN_CDC_ID_KEY = "bioinformaticianCdcIdKey";
    private static final String BIOINFORMATICIAN_NAME_KEY = "bioinformaticianName";
    private static final Map<String, Set<String>> referenceGenomeNameToTypesMap = Map.of(
        "AY184219", Set.of(PolioReferenceType.PV1.name),
        "AY184220", Set.of(PolioReferenceType.PV2.name, PolioReferenceType.NOPV2.name),
        "AY184221", Set.of(PolioReferenceType.PV3.name),
        "MZ245455", Set.of(PolioReferenceType.NOPV2.name),
        PolioReferenceType.NOPV2.name, Set.of(PolioReferenceType.NOPV2.name)
    );
    private static final Set<String> referenceGenomeNames = referenceGenomeNameToTypesMap.keySet();
    private static final Set<String> polioTypes = Arrays.stream(PolioReferenceType.values()).map(polioReferenceType -> polioReferenceType.name).collect(Collectors.toSet());
    private static final String FOURTY_EIGHT_POLIO_GENOTYPES_DOCUMENT_NAME = "48 Polio Genotypes";
    private static final String SAMPLE_NAME_PATTERN_STRING = "(\\d{10})\\s*[-_]\\s*([A-Za-z\\d]+)";
    private static final String SAMPLE_NAME_PATTERN_STRING_FOR_CONTROLS = "(.+)\\s*[-_]\\s*([Cc]ontrol(?:\\s*[-_]\\s*[Rr]ep\\s*)?(?:\\s*[-_]\\s*\\d+)?)";
    //    private static final String X = "(\\d{10})\\s*[-_]\\s*([A-Za-z\\d]+)\\s*[-_]\\s*([A-Za-z\\d\\s()-_]+?)(?=\\s*[-_])\\s*[-_]\\s*[Ff]inal [Cc]on(?:tig|sensus)\\s*(?:\\(reversed\\))?$\n";
    private static final String DOCUMENT_NAME_END_STRING = "\\s*[-_]\\s*([A-Za-z\\d\\s()-_]+?)\\s*[-_]\\s*[Ff]inal [Cc]on(?:tig|sensus)\\s*(?:\\(reversed\\))?$";
    private static final Pattern DOCUMENT_NAME_PATTERN = Pattern.compile("^\\s*" + SAMPLE_NAME_PATTERN_STRING + DOCUMENT_NAME_END_STRING);
    private static final Pattern DOCUMENT_NAME_PATTERN_FOR_CONTROLS = Pattern.compile("^\\s*" + SAMPLE_NAME_PATTERN_STRING_FOR_CONTROLS + DOCUMENT_NAME_END_STRING);
    private static final Pattern DUPLICATE_TYPE_PER_SAMPLE_PATTERN = Pattern.compile("^\\s*(.*)\\s*\\(\\s*\\d+\\s*\\)\\s*$");
    private static final Pattern ALIGNED_SEQUENCE_PATTERN = Pattern.compile(String.format(
        "^%s: %s$",
        INPUT_DOCUMENT,
        formatGenomeName(
            SAMPLE_NAME_PATTERN_STRING,
            "([A-Za-z\\d\\s()-_]+)"
        )
    ));
    private static final Pattern SAMPLE_FOLDER_PATTERN = Pattern.compile("^(?:Folder:)?\\s*" + SAMPLE_NAME_PATTERN_STRING + "\\s*$");
    private static final Pattern SAMPLE_FOLDER_PATTERN_FOR_CONTROLS = Pattern.compile("^(?:Folder:)?\\s*" + SAMPLE_NAME_PATTERN_STRING_FOR_CONTROLS + "\\s*$");
    private static final Pattern TYPE_FOLDER_PATTERN = Pattern.compile("^(?:Folder:)?\\s*([A-Za-z\\d\\s()-_]+)\\s*[-_]\\s*[Ff]inal [Cc]ontig.*$");
    private static final Pattern BLAST_HITS_FOLDER_PATTERN = Pattern.compile("^(?:Folder:)?\\s*[\\d\\w\\s()-_]+Nucleotide collection \\(nr_nt\\) (?:blastn|Megablast).*$");
    private static final List<AnnotatedPluginDocument> vp1SequenceDocuments = new LinkedList<>();
    private static final Comparator<String> compareStringsIgnoreCase = Comparator.comparing(String::toLowerCase);
    private static final int AVERAGE_READ_COVERAGE_THRESHOLD_FOR_GREEN_GENOMES = 50;
    private static final int AVERAGE_READ_COVERAGE_THRESHOLD_FOR_YELLOW_GENOMES = 25;
    private static final int CDS_MINIMUM_COVERAGE_FOR_GREEN_GENOMES = 3;
    private static final int VP1_MINIMUM_COVERAGE_FOR_YELLOW_GENOMES = 6;

    static class WrappedSequenceDocument {
        private final int leadingGapLength;
        private final SequenceDocument sequenceDocument;

        static final Pattern NON_GAP_PATTERN = Pattern.compile("[^-]");

        WrappedSequenceDocument(
            int leadingGapLength,
            SequenceDocument sequenceDocument
        ) {
            this.leadingGapLength = leadingGapLength;
            this.sequenceDocument = sequenceDocument;
        }

        static List<SequenceDocument> sortSequenceDocuments(
            DefaultAlignmentDocument defaultAlignmentDocument
        ) {
            return defaultAlignmentDocument.getSequences().stream().map(unsortedSequenceDocument -> {
                String sequenceString = unsortedSequenceDocument.getSequenceString();
                Matcher matcher = NON_GAP_PATTERN.matcher(sequenceString);
                matcher.find();
                return new WrappedSequenceDocument(
                    matcher.start(),
                    unsortedSequenceDocument
                );
            }).sorted(
                Comparator.comparingInt(wrappedSequenceDocument -> wrappedSequenceDocument.leadingGapLength)
            ).map(
                wrappedSequenceDocument -> wrappedSequenceDocument.sequenceDocument
            ).collect(Collectors.toList());
        }
    }
    static class SingleNucleotidePolymorphism {
        private final String reference;
        private final char ref;
        private final char alt;
        private final int index;

        private SingleNucleotidePolymorphism(
            String reference,
            char ref,
            char alt,
            int index
        ) {
            this.reference = reference;
            this.ref = ref;
            this.alt = alt;
            this.index = index;
        }
    }

    static class ExcelData {
        AnnotatedPluginDocument nucleotideSequenceDocumentAnnotatedPluginDocument;
        AnnotatedPluginDocument defaultAlignmentDocumentAnnotatedPluginDocument;
        int fullCoverageLength;
        int totalGapCount;
        double averageCoverage;
        double percentFullCoverageCoveredByReads;
        double percentPairwiseIdentity;
        double percentQueryCoverage;
        String topBlastHit;
        String accessionNumber;
        List<SingleNucleotidePolymorphism> vp1Snps = null;
        Integer cdsGapCount = null;
        Integer vp1GapCount = null;
        boolean partialVp1CoverageFlag;
        Integer cdsMinimumCoverage;
        Integer vp1MinimumCoverage;
        String partialCdsNote = null;
        String recombinationNote = "recombination status indeterminate";

        String calculateColorResultKey() {
            String colorResultKey;
            if (this.cdsGapCount == null) {
                // If cdsGapCount was never assigned, we are forced to assume every gap is within the CDS.
                this.cdsGapCount = this.totalGapCount;
            }
            if (this.averageCoverage > AVERAGE_READ_COVERAGE_THRESHOLD_FOR_GREEN_GENOMES) {
                colorResultKey = GREEN;
                if (
                    this.cdsGapCount > 0 ||
                    this.partialCdsNote != null
                ) {
                    colorResultKey = YELLOW_REDO;
                } else if (this.totalGapCount > 0) {
                    colorResultKey = YELLOW_KEEP;
                }
            } else if (this.averageCoverage > AVERAGE_READ_COVERAGE_THRESHOLD_FOR_YELLOW_GENOMES) {
                colorResultKey = YELLOW_KEEP;
                if (
                    this.cdsGapCount > 0 ||
                    this.partialCdsNote != null
                ) {
                    colorResultKey = YELLOW_REDO;
                }
            } else {
                colorResultKey = RED_REDO;
            }
            if (colorResultKey.equals(GREEN)) {
                if (
                    this.cdsMinimumCoverage == null ||
                    this.cdsMinimumCoverage < CDS_MINIMUM_COVERAGE_FOR_GREEN_GENOMES
                ) {
                    colorResultKey = YELLOW_REDO;
                }
            }
            if (
                this.vp1MinimumCoverage == null ||
                this.vp1MinimumCoverage < VP1_MINIMUM_COVERAGE_FOR_YELLOW_GENOMES
            ) {
                colorResultKey = RED_REDO;
            }
            return colorResultKey;
        }
        String calculateVp1SnpsString() {
            if (this.vp1Snps == null || this.vp1GapCount == null) {
                return "-";
            } else if (this.partialVp1CoverageFlag) {
                return "-";
            } else if (this.vp1GapCount == 0) {
                return Integer.toString(this.vp1Snps.size());
            } else {
                int vp1SnpCount = this.vp1Snps.size();
                return String.format(
                    "%d = %d (Gaps) + %d (SNPs)",
                    this.vp1GapCount + vp1SnpCount,
                    this.vp1GapCount,
                    vp1SnpCount
                );
            }
        }
        String calculateNotes() {
            List<String> notes = new LinkedList<>();
            if (this.cdsMinimumCoverage == null) {
                notes.add("CDS coverage data unavailable");
            } else if (
                this.totalGapCount == 0 &&
                this.cdsMinimumCoverage < CDS_MINIMUM_COVERAGE_FOR_GREEN_GENOMES
            ) {
                notes.add(String.format(
                    "CDS min. coverage too low: %d <= %d",
                    this.cdsMinimumCoverage,
                    CDS_MINIMUM_COVERAGE_FOR_GREEN_GENOMES - 1
                ));
            }
            if (this.vp1MinimumCoverage == null) {
                notes.add("VP1 coverage data unavailable");
            } else if (this.vp1MinimumCoverage < VP1_MINIMUM_COVERAGE_FOR_YELLOW_GENOMES) {
                notes.add(String.format(
                    "VP1 min. coverage too low: %d <= %d",
                    this.vp1MinimumCoverage,
                    VP1_MINIMUM_COVERAGE_FOR_YELLOW_GENOMES - 1
                ));
            }
            if (this.totalGapCount > 0) {
                notes.add(String.format(
                    "Gaps (n = %d)",
                    this.totalGapCount
                ));
            }
            if (this.partialCdsNote != null) {
                notes.add(this.partialCdsNote);
            }
            if (this.recombinationNote != null) {
                notes.add(this.recombinationNote);
            }
            return String.join(
                "; ",
                notes
            );
        }

        void parseNucleotideSequenceDocument(
            String sampleName,
            String type
        ) throws DocumentOperationException {
            NucleotideSequenceDocument nucleotideSequenceDocument = (NucleotideSequenceDocument)this.nucleotideSequenceDocumentAnnotatedPluginDocument.getDocument();
            int gapCount = 0;
            String sequenceString = nucleotideSequenceDocument.getSequenceString();
            int sequenceLength = sequenceString.length();
            for (int i = 0; i < sequenceLength; i++) {
                char c = sequenceString.charAt(i);
                if (
                    c == 'n' ||
                    c == 'N'
                ) {
                    gapCount++;
                }
            }
            this.fullCoverageLength = sequenceLength;
            this.totalGapCount = gapCount;

            String genomeName = formatGenomeName(
                sampleName,
                type
            );
            List<SequenceAnnotation> sequenceAnnotations = nucleotideSequenceDocument.getSequenceAnnotations();
            SequenceAnnotation vp1SequenceAnnotation = null;
            for (SequenceAnnotation sequenceAnnotation : sequenceAnnotations) {
                if (sequenceAnnotation.getName().matches("[Vv][Pp]1")) {
                    vp1SequenceAnnotation = sequenceAnnotation;
                }
            }
            if (sequenceAnnotations.isEmpty()) {
                NonFatalError.GENOMES_WITHOUT_ANNOTATIONS.names.add(genomeName);
            } else if (vp1SequenceAnnotation == null) {
                NonFatalError.GENOMES_WITHOUT_A_VP1_ANNOTATION.names.add(genomeName);
            } else {
                int numberOfVp1Intervals = vp1SequenceAnnotation.getNumberOfIntervals();
                if (numberOfVp1Intervals != 1) {
                    NonFatalError.GENOMES_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS.names.add(genomeName);
                } else {
                    Matcher duplicateTypePerSampleMatcher = DUPLICATE_TYPE_PER_SAMPLE_PATTERN.matcher(type);
                    if (duplicateTypePerSampleMatcher.matches()) {
                        type = duplicateTypePerSampleMatcher.group(1);
                    }

                    if (polioTypes.contains(type)) {
                        SequenceAnnotationInterval vp1SequenceInterval = vp1SequenceAnnotation.getInterval();
                        int vp1IntervalMinimumIndex = vp1SequenceInterval.getMinimumIndex();
                        int vp1IntervalMaximumIndex = vp1SequenceInterval.getMaximumIndex();
                        DefaultNucleotideSequence vp1SequenceDocument = new DefaultNucleotideSequence(
                            String.format(
                                "%s: %s",
                                INPUT_DOCUMENT,
                                genomeName
                            ),
                            sequenceString.substring(
                                Math.max(
                                    0,
                                    vp1IntervalMinimumIndex - 1
                                ),
                                Math.min(
                                    sequenceLength,
                                    vp1IntervalMaximumIndex
                                )
                            )
                        );
                        vp1SequenceDocuments.add(DocumentUtilities.createAnnotatedPluginDocument(vp1SequenceDocument));
                    }
                }
            }
        }

        void parseAlignmentDocument(
                String sampleName,
                String type
        ) throws DocumentOperationException, IOException {
            DefaultAlignmentDocument defaultAlignmentDocument = (DefaultAlignmentDocument)this.defaultAlignmentDocumentAnnotatedPluginDocument.getDocument();
            this.averageCoverage = (double)this.defaultAlignmentDocumentAnnotatedPluginDocument.getFieldValue(DocumentField.CONTIG_MEAN_COVERAGE);
            this.percentFullCoverageCoveredByReads = ((Percentage)this.defaultAlignmentDocumentAnnotatedPluginDocument.getFieldValue(DocumentField.CONTIG_PERCENTAGE_OF_REFERENCE_SEQUENCE_COVERED)).doubleValue();

            SequenceDocument consensusSequenceDocument = SequenceUtilities.generateConsensus(
                defaultAlignmentDocument,
                new CompositeProgressListener(
                    null,
                    1
                )
            );
            SequenceListSummary sequenceListSummary = new SequenceListSummary(
                WrappedSequenceDocument.sortSequenceDocuments(
                    defaultAlignmentDocument
                ),
                true,
                null
            );
            int alignmentNumberOfColumns = sequenceListSummary.getAlignmentNumberOfColumns();

            SequenceDocument referenceSequenceDocument = defaultAlignmentDocument.getSequence(defaultAlignmentDocument.getContigReferenceSequenceIndex());
            List<SequenceAnnotation> sequenceAnnotations = referenceSequenceDocument.getSequenceAnnotations();
            List<SequenceAnnotation> vp1MatchingSequenceAnnotations = sequenceAnnotations.stream().filter(
                sequenceAnnotation -> sequenceAnnotation.getName().matches("^[Vv][Pp]1$")
            ).collect(
                Collectors.toList()
            );
            List<SequenceAnnotation> cdsMatchingSequenceAnnotations = sequenceAnnotations.stream().filter(
                sequenceAnnotation -> sequenceAnnotation.getName().matches("^[Cc][Dd][Ss]$")
            ).collect(
                Collectors.toList()
            );

            String genomeName = formatGenomeName(
                sampleName,
                type
            );
            String consensusSequence = consensusSequenceDocument.getSequenceString();

            int cdsMinimumCoverage = Integer.MAX_VALUE;
            int cdsLowerBoundInclusive = 0;
            int cdsUpperBoundExclusive = alignmentNumberOfColumns;

            if (sequenceAnnotations.isEmpty()) {
                NonFatalError.GENOMES_WITHOUT_ANNOTATIONS.names.add(genomeName);
            } else {
                if (vp1MatchingSequenceAnnotations.size() < 1) {
                    NonFatalError.CONTIGS_WITHOUT_A_VP1_ANNOTATION.names.add(genomeName);
                } else if (vp1MatchingSequenceAnnotations.size() > 1) {
                    NonFatalError.CONTIGS_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS.names.add(genomeName);
                } else {
                    SequenceAnnotation vp1SequenceAnnotation = vp1MatchingSequenceAnnotations.get(0);
                    if (vp1SequenceAnnotation.getNumberOfIntervals() != 1) {
                        NonFatalError.CONTIGS_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS.names.add(genomeName);
                    } else {
                        SequenceAnnotationInterval vp1AnnotationInterval = vp1SequenceAnnotation.getInterval();
                        int vp1LowerBoundInclusive = Math.max(
                            0,
                            vp1AnnotationInterval.getMinimumIndex()
                        );
                        int vp1UpperBoundExclusive = Math.min(
                            alignmentNumberOfColumns,
                            vp1AnnotationInterval.getMaximumIndex() + 1
                        );
                        int vp1MinimumCoverage = Integer.MAX_VALUE;
                        for (int i = vp1LowerBoundInclusive; i < vp1UpperBoundExclusive; i++) {
                            if (consensusSequence.charAt(i) != '-') {
                                int vp1Coverage = sequenceListSummary.getCoverage(i) - 1;
                                if (vp1Coverage < vp1MinimumCoverage) {
                                    vp1MinimumCoverage = vp1Coverage;
                                }
                            }
                        }
                        this.vp1MinimumCoverage = vp1MinimumCoverage;
                    }
                }

                if (cdsMatchingSequenceAnnotations.size() < 1) {
                    NonFatalError.CONTIGS_WITHOUT_A_CDS_ANNOTATION.names.add(genomeName);
                } else if (cdsMatchingSequenceAnnotations.size() > 1) {
                    NonFatalError.CONTIGS_WITH_AN_UNEXPECTED_NUMBER_OF_CDS_ANNOTATION_INTERVALS.names.add(genomeName);
                } else {
                    SequenceAnnotation cdsSequenceAnnotation = cdsMatchingSequenceAnnotations.get(0);
                    if (cdsSequenceAnnotation.getNumberOfIntervals() != 1) {
                        NonFatalError.CONTIGS_WITH_AN_UNEXPECTED_NUMBER_OF_CDS_ANNOTATION_INTERVALS.names.add(genomeName);
                    } else {
                        SequenceAnnotationInterval cdsAnnotationInterval = cdsSequenceAnnotation.getInterval();
                        cdsLowerBoundInclusive = Math.max(
                            0,
                            cdsAnnotationInterval.getMinimumIndex()
                        );
                        cdsUpperBoundExclusive = Math.min(
                            alignmentNumberOfColumns,
                            cdsAnnotationInterval.getMaximumIndex() + 1
                        );
                    }
                }
            }
            for (int i = cdsLowerBoundInclusive; i < cdsUpperBoundExclusive; i++) {
                if (consensusSequence.charAt(i) != '-') {
                    int cdsCoverage = sequenceListSummary.getCoverage(i) - 1;
                    if (cdsCoverage < cdsMinimumCoverage) {
                        cdsMinimumCoverage = cdsCoverage;
                    }
                }
            }
            this.cdsMinimumCoverage = cdsMinimumCoverage;
        }
    }

    static class DatabaseData {
        private String dbName;
        private String genotypeSeq;
        private double percentIdentity;
        private int nucleotideDifferencesOfGenotypeCount;

        DatabaseData() {
            // Do nothing.
        }
    }

    static class SnpsCalculationData {
        final int start;
        final int stop;
        List<Double> distances;
        int numberOfSnpsEncountered;
        int numberOfGapsEncountered;

        SnpsCalculationData(
                int start,
                int stop
        ) {
            this.start = start;
            this.stop = stop;
        }
    }

    @Override
    public GeneiousActionOptions getActionOptions() {
        GeneiousActionOptions options = new GeneiousActionOptions("Finalize Run");
        options.setMainMenuLocation(GeneiousActionOptions.MainMenu.Export);
        options.setInMainToolbar(true);
        return options;
    }

    @Override
    public String getHelp() {
        return HELP;
    }

    @Override
    public DocumentSelectionSignature[] getSelectionSignatures() {
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
        options.addStringOption(
            SHORTENED_RUN_NAME_KEY,
            "Shortened run name",
            "Polio###"
        );
        options.addBooleanOption(
            EXPORT_FINAL_ANALYSIS_FOLDER_AND_FILES_FLAG_KEY,
            String.format(
                "Export \"%s\" folder and files?",
                FINAL_ASSEMBLIES_FOLDER_NAME
            ),
            true
        );
        options.addBooleanOption(
            UPLOAD_EXCEL_TO_SHAREPOINT_FLAG_KEY,
            "Upload excel to SharePoint?",
            true
        );
        options.addBooleanOption(
            GENERATE_EMAIL_FLAG_KEY,
            "Generate email?",
            true
        );
        options.addStringOption(
            BIOINFORMATICIAN_CDC_ID_KEY,
            "Bioinformatician CDC ID",
            ""
        );
        options.addStringOption(
            BIOINFORMATICIAN_NAME_KEY,
            "Bioinformatician name",
            ""
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

    public static String formatSampleName(
        String csid,
        String cuid
    ) {
        return String.format(
            "%s_%s",
            csid,
            cuid
        );
    }

    public static String formatGenomeName(
        String sampleName,
        String type
    ) {
        return String.format(
            "%s - %s",
            sampleName,
            type
        );
    }

    public double average(List<Double> values) {
        double sum = 0;
        for (double value : values) {
            sum += value;
        }
        return sum / values.size();
    }
    public double standardDeviation(
        List<Double> values,
        boolean populationFlag
    ) {
        double average = average(values);
        double sumOfSquaredDistances = 0;
        for (double value : values) {
            double difference = value - average;
            sumOfSquaredDistances += difference * difference;
        }
        int denominator = values.size();
        if (!populationFlag) {
            denominator--;
        }
        return Math.sqrt(sumOfSquaredDistances / denominator);
    }

    public List<Integer> getIndicesOfSnps(
        String referenceSequence,
        String alternateSequence,
        int start,
        int stop,
        SnpsCalculationData snpsCalculationData
    ) {
        List<Integer> indicesOfSnps = new LinkedList<>();
        int numberOfGapsEncountered = 0;
        for (int i = start; i <= stop; i++) {
            char ref = referenceSequence.charAt(i);
            char alt = alternateSequence.charAt(i);
            if (
                ref == 'U' ||
                ref == 'u'
            ) {
                ref--;
            }
            if (
                alt == 'U' ||
                alt == 'u'
            ) {
                alt--;
            }
            if (
                alt == 'N' ||
                alt == 'n'
            ) {
                numberOfGapsEncountered++;
            } else if (
                alt != ref
            ) {
                indicesOfSnps.add(i);
            }
        }
        if (snpsCalculationData != null) {
            snpsCalculationData.numberOfGapsEncountered = numberOfGapsEncountered;
        }
        return indicesOfSnps;
    }

    @Override
    public List<AnnotatedPluginDocument> performOperation(
        AnnotatedPluginDocument[] documents,
        ProgressListener progressListener,
        Options options
    ) throws DocumentOperationException {
        long systemStartTime = System.currentTimeMillis();
        List<AnnotatedPluginDocument> returnValue = new LinkedList<>();

        String fullRunName = options.getValueAsString(FULL_RUN_NAME_KEY);
        String shortenedRunName = options.getValueAsString(SHORTENED_RUN_NAME_KEY);
        String wetLabExcelFilePathAsString = options.getValueAsString(WET_LAB_EXCEL_FILE_PATH_KEY);
        Path wetLabExcelFilePath = Paths.get(wetLabExcelFilePathAsString);
        if (!Files.exists(wetLabExcelFilePath)) {
            throw new DocumentOperationException(String.format(
                "Wet-lab Excel-file path \"%s\" does not exist.",
                wetLabExcelFilePathAsString
            ));
        }
        boolean exportFinalAnalysisFolderAndFilesFlag = (boolean)options.getValue(EXPORT_FINAL_ANALYSIS_FOLDER_AND_FILES_FLAG_KEY);
        boolean uploadExcelDocumentToSharepointFlag = (boolean)options.getValue(UPLOAD_EXCEL_TO_SHAREPOINT_FLAG_KEY);
        boolean generateEmailFlag = (boolean)options.getValue(GENERATE_EMAIL_FLAG_KEY);
        String bioinformaticianCdcId = options.getValueAsString(BIOINFORMATICIAN_CDC_ID_KEY);
        String bioinformaticianName = options.getValueAsString(BIOINFORMATICIAN_NAME_KEY);
        String pythonScriptsFolderPathAsString = options.getValueAsString(PYTHON_SCRIPTS_FOLDER_PATH_KEY);
        int numberOfThreads = (Integer)options.getValue(NUMBER_OF_THREADS_KEY);
        Path pythonScriptsFolderPath = Paths.get(pythonScriptsFolderPathAsString);
        if (!Files.exists(pythonScriptsFolderPath)) {
            throw new DocumentOperationException(String.format(
                "Python-scripts folder path \"%s\" does not exist.",
                pythonScriptsFolderPathAsString
            ));
        }
        if (!Files.isDirectory(pythonScriptsFolderPath)) {
            throw new DocumentOperationException(String.format(
                "Python-scripts folder path \"%s\" is not a folder.",
                pythonScriptsFolderPathAsString
            ));
        }

        if (documents.length == 0) {
            throw new DocumentOperationException("Zero input documents were provided.");
        }
        DatabaseService finalAssembliesFolderDatabaseService = documents[0].getDatabase();
        if (finalAssembliesFolderDatabaseService == null) {
            throw new DocumentOperationException("documents[0] was not located within a database.");
        }
        String finalAssembliesDatabaseServiceName = finalAssembliesFolderDatabaseService.getName();
        if (!finalAssembliesDatabaseServiceName.matches("^(?:Folder:)?\\s*[Ff]inal[\\s_]*[Aa]ssemblies\\s*$")) {
            throw new DocumentOperationException(String.format(
                "This operation should be run within the \"%s\" folder. Instead, it was run within \"%s\"",
                FINAL_ASSEMBLIES_FOLDER_NAME,
                finalAssembliesDatabaseServiceName
            ));
        }

        GeneiousService parentService = finalAssembliesFolderDatabaseService.getParentService();
        if (!(parentService instanceof DatabaseService)) {
            throw new DocumentOperationException(String.format(
                "The parent Geneious service of the input folder \"%s\" should be another DatabaseService",
                finalAssembliesDatabaseServiceName
            ));
        }
        DatabaseService parentDatabaseService = (DatabaseService)parentService;
        List<DatabaseService> matchingChildFolders = finalAssembliesFolderDatabaseService.getChildServices().stream().filter(
            childService -> childService instanceof DatabaseService
        ).map(
            childService -> (DatabaseService)childService
        ).filter(
            childFolder -> childFolder.getName().matches("^(?:Folder:)?\\s*[Aa]lignments\\s*$")
        ).collect(Collectors.toList());
        if (matchingChildFolders.size() != 1) {
            throw new DocumentOperationException(String.format(
                "Precisely 1 child folder of \"%s\" should be named \"Alignments.\" Found %d instead.",
                FINAL_ASSEMBLIES_FOLDER_NAME,
                matchingChildFolders.size()
            ));
        }
        DatabaseService alignmentsFolder = matchingChildFolders.get(0);

        HashMap<String, HashMap<String, ExcelData>> excelData = new HashMap<>();
        HashMap<String, HashMap<String, DatabaseData>> databaseData = new HashMap<>();
        AnnotatedPluginDocument vp1ReferencesDocument = null;
        List<AnnotatedPluginDocument> defaultNucleotideSequenceAnnotatedPluginDocuments = new LinkedList<>();
        for (AnnotatedPluginDocument annotatedPluginDocument : documents) {
            PluginDocument pluginDocument = annotatedPluginDocument.getDocument();

            String pluginDocumentName = pluginDocument.getName();
            if (referenceGenomeNames.contains(pluginDocumentName)) {
                continue;
            }

            boolean documentIsDefaultNucleotideSequence = pluginDocument instanceof DefaultNucleotideSequence;
            boolean documentIsDefaultAlignmentDocument = pluginDocument instanceof DefaultAlignmentDocument;
            if (
                !documentIsDefaultNucleotideSequence &&
                !documentIsDefaultAlignmentDocument
            ) {
                if (
                    pluginDocument instanceof SequenceListDocument &&
                    pluginDocumentName.equals(FOURTY_EIGHT_POLIO_GENOTYPES_DOCUMENT_NAME)
                ) {
                    vp1ReferencesDocument = annotatedPluginDocument;
                }
                continue;
            }
            Matcher pluginDocumentNameMatcher;
            if (
                !(pluginDocumentNameMatcher = DOCUMENT_NAME_PATTERN.matcher(pluginDocumentName)).matches() &&
                !(pluginDocumentNameMatcher = DOCUMENT_NAME_PATTERN_FOR_CONTROLS.matcher(pluginDocumentName)).matches()
            ) {
                throw new DocumentOperationException(String.format(
                    "Input document name \"%s\" had an unrecognized format (example: CSID-CUID - type - Final Consensus).",
                    pluginDocumentName
                ));
            }
            String sampleName = formatSampleName(
                pluginDocumentNameMatcher.group(1),
                pluginDocumentNameMatcher.group(2)
            );
            String type = pluginDocumentNameMatcher.group(3);
            type = type.replaceAll("\\s+", "");
            if (!excelData.containsKey(sampleName)) {
                excelData.put(
                    sampleName,
                    new HashMap<>()
                );
            }
            HashMap<String, ExcelData> excelDataPerSample = excelData.get(sampleName);
            if (!excelDataPerSample.containsKey(type)) {
                excelDataPerSample.put(
                    type,
                    new ExcelData()
                );
            }
            ExcelData excelDataPerSamplePerType = excelDataPerSample.get(type);
            if (documentIsDefaultAlignmentDocument) {
                if (excelDataPerSamplePerType.defaultAlignmentDocumentAnnotatedPluginDocument != null) {
                    throw new DocumentOperationException(String.format(
                        "Multiple contig documents from sample \"%s\" and type \"%s\" were provided. This is not allowed.",
                        sampleName,
                        type
                    ));
                }
                excelDataPerSamplePerType.defaultAlignmentDocumentAnnotatedPluginDocument = annotatedPluginDocument;
            } else {
                if (excelDataPerSamplePerType.nucleotideSequenceDocumentAnnotatedPluginDocument != null) {
                    throw new DocumentOperationException(String.format(
                        "Multiple consensus documents from sample \"%s\" and type \"%s\" were provided. This is not allowed.",
                        sampleName,
                        type
                    ));
                }
                excelDataPerSamplePerType.nucleotideSequenceDocumentAnnotatedPluginDocument = annotatedPluginDocument;
                defaultNucleotideSequenceAnnotatedPluginDocuments.add(annotatedPluginDocument);
            }
        }
        if (vp1ReferencesDocument == null) {
            throw new DocumentOperationException(String.format(
                "Required input document \"%s\" was not provided.",
                FOURTY_EIGHT_POLIO_GENOTYPES_DOCUMENT_NAME
            ));
        }
        vp1SequenceDocuments.add(vp1ReferencesDocument);

        for (GeneiousService servicePerSample : parentDatabaseService.getChildServices()) {
            if (
                !(servicePerSample instanceof DatabaseService) ||
                finalAssembliesFolderDatabaseService == servicePerSample
            ) {
                continue;
            }
            String servicePerSampleName = servicePerSample.getName();
            Matcher servicePerSampleNameMatcher;
            if (
                !(servicePerSampleNameMatcher = SAMPLE_FOLDER_PATTERN.matcher(servicePerSampleName)).matches() &&
                !(servicePerSampleNameMatcher = SAMPLE_FOLDER_PATTERN_FOR_CONTROLS.matcher(servicePerSampleName)).matches()
            ) {
                continue;
            }
            String sampleName = formatSampleName(
                servicePerSampleNameMatcher.group(1),
                servicePerSampleNameMatcher.group(2)
            );
            if (!excelData.containsKey(sampleName)) {
                excelData.put(
                    sampleName,
                    null
                );
                continue;
            }
            HashMap<String, ExcelData> excelDataPerSample = excelData.get(sampleName);
            for (GeneiousService servicePerSamplePerType : servicePerSample.getChildServices()) {
                String servicePerSamplePerTypeName = servicePerSamplePerType.getName();
                Matcher servicePerSamplePerTypeNameMatcher = TYPE_FOLDER_PATTERN.matcher(servicePerSamplePerTypeName);
                if (!servicePerSamplePerTypeNameMatcher.matches()) {
                    continue;
                }
                if (!(servicePerSamplePerType instanceof DatabaseService)) {
                    continue;
                }
                DatabaseService databaseServicePerSamplePerType = (DatabaseService)servicePerSamplePerType;
                String type = servicePerSamplePerTypeNameMatcher.group(1);
                type = type.replaceAll("\\s+", "");
                // The type within this folder name is not a direct match to input files' types.
                boolean foundMatchFlag = excelDataPerSample.containsKey(type);
                if (!foundMatchFlag) {
                    // Attempt to find the correct type via reference-file-name replacement.
                    Set<String> typesPerSample = excelDataPerSample.keySet();
                    if (referenceGenomeNameToTypesMap.containsKey(type)) {
                        // The folder was not renamed from the reference genome. This is the most likely case of all.
                        // This is not necessarily 1-to-1, especially in the case of PV2/nOPV2, mostly due to 5'-recombinant genomes.
                        Set<String> candidateTypes = referenceGenomeNameToTypesMap.get(type);
                        Set<String> typeMatches = typesPerSample.stream().filter(
                            typePerSample -> candidateTypes.stream().anyMatch(
                                typePerSample::equals
                            )
                        ).collect(Collectors.toSet());
                        if (typeMatches.size() == 1) {
                            type = typeMatches.iterator().next();
                            foundMatchFlag = true;
                        }
                    }
                }
                if (!foundMatchFlag) {
                    // Attempt to find the correct type via the files inside the folder.
                    Set<String> typesPerRelevantFiles = databaseServicePerSamplePerType.retrieve("").stream().map(
                        file -> {
                            Matcher documentNameMatcher = DOCUMENT_NAME_PATTERN.matcher(file.getName());
                            if (!documentNameMatcher.matches()) {
                                // Cannot derive a type from this file.
                                return null;
                            }
                            return documentNameMatcher.group(3).replaceAll("\\s+", "");
                        }
                    ).filter(
                        Objects::nonNull
                    ).collect(Collectors.toSet());
                    if (typesPerRelevantFiles.size() == 1) {
                        type = typesPerRelevantFiles.iterator().next();
                        foundMatchFlag = excelDataPerSample.containsKey(type);
                    }
                }
                if (!foundMatchFlag) {
                    continue;
                }
//                if (!excelDataPerSample.containsKey(type)) {
//                    if (!referenceGenomeNameToTypesMap.containsKey(type)) {
//                        continue;
//                    }
//                    List<String> typesPerReferenceGenomeName = referenceGenomeNameToTypesMap.get(type);
//                    boolean foundMatchingTypeFlag = false;
//                    for (String _type : typesPerReferenceGenomeName) {
//                        if (excelDataPerSample.containsKey(_type)) {
//                            ExcelData excelDataPerSamplePerType = excelDataPerSample.get(_type);
//                            // TODO: Rectify this ambiguity issue.
//                            if (excelDataPerSamplePerType.topBlastHit == null) {
//                                type = _type;
//                                foundMatchingTypeFlag = true;
//                                break;
//                            }
//                        }
//                    }
//                    if (!foundMatchingTypeFlag) {
//                        continue;
//                    }
//                }
                ExcelData excelDataPerSamplePerType = excelDataPerSample.get(type);
                for (GeneiousService serviceForBlastHits : servicePerSamplePerType.getChildServices()) {
                    if (!(serviceForBlastHits instanceof DatabaseService)) {
                        continue;
                    }
                    String serviceForBlastHitsName = serviceForBlastHits.getName();
                    Matcher serviceForBlastHitsNameMatcher = BLAST_HITS_FOLDER_PATTERN.matcher(serviceForBlastHitsName);
                    if (!serviceForBlastHitsNameMatcher.matches()) {
                        // Ignore folders which aren't BLAST hits.
                        continue;
                    }
                    DatabaseService databaseServiceForBlastHits = (DatabaseService)serviceForBlastHits;
                    List<AnnotatedPluginDocument> annotatedBlastHitDocuments = databaseServiceForBlastHits.retrieve("");
                    double bestBlastHitGrade = -1d;
                    String bestBlastHitAccession = "";
                    String bestBlastHitDescription = "";
                    double bestBlastHitPairwiseIdentity = -1d;
                    double bestBlastHitQueryCoverage = -1d;
                    for (AnnotatedPluginDocument annotatedBlastHitDocument : annotatedBlastHitDocuments) {
                        if (!annotatedBlastHitDocument.getDocumentClass().toString().equals("class com.biomatters.plugins.ncbi.blast.NucleotideBlastSummaryDocument")) {
                            // Ignore files which are not BLAST hits.
                            continue;
                        }
                        PluginDocument blastHitDocument = annotatedBlastHitDocument.getDocument();
                        double grade = ((Percentage)annotatedBlastHitDocument.getFieldValue(CODE_FOR_GRADE_FIELD)).doubleValue();
                        if (grade > bestBlastHitGrade) {
                            bestBlastHitGrade = grade;
                            bestBlastHitAccession = (String)annotatedBlastHitDocument.getFieldValue(DocumentField.ACCESSION_FIELD);
                            // Commas will interefere with the behavior of CSV files.
                            bestBlastHitDescription = blastHitDocument.getDescription().replace(",", ";");
                            bestBlastHitPairwiseIdentity = ((Percentage)annotatedBlastHitDocument.getFieldValue(DocumentField.ALIGNMENT_SIMILARITY)).doubleValue();
                            bestBlastHitQueryCoverage = ((Percentage)annotatedBlastHitDocument.getFieldValue(CODE_FOR_QUERY_COVERAGE_FIELD)).doubleValue();
                        }
                    }
                    if (bestBlastHitGrade != -1) {
                        excelDataPerSamplePerType.percentPairwiseIdentity = bestBlastHitPairwiseIdentity;
                        excelDataPerSamplePerType.percentQueryCoverage = bestBlastHitQueryCoverage;
                        excelDataPerSamplePerType.topBlastHit = bestBlastHitDescription;
                        excelDataPerSamplePerType.accessionNumber = bestBlastHitAccession;
                    }
                }
            }
        }

        List<Callable<Void>> parseExcelDataFromDocumentsTasks = new LinkedList<>();
        List<AnnotatedPluginDocument> alignmentsFolderDocuments = alignmentsFolder.retrieve("");
        for (AnnotatedPluginDocument alignmentsFolderDocument : alignmentsFolderDocuments) {
            PluginDocument alignmentsFolderPluginDocument = alignmentsFolderDocument.getDocument();
            if (!(alignmentsFolderPluginDocument instanceof DefaultAlignmentDocument)) {
                continue;
            }
            DefaultAlignmentDocument defaultAlignmentDocument = (DefaultAlignmentDocument)alignmentsFolderPluginDocument;
            int numberOfSequences = defaultAlignmentDocument.getNumberOfSequences();
            List<SequenceDocument> sequenceDocuments = defaultAlignmentDocument.getSequences();
            String defaultAlignmentDocumentName = defaultAlignmentDocument.getName();
            int referenceSequenceIndex = defaultAlignmentDocument.getContigReferenceSequenceIndex();
            if (referenceSequenceIndex == -1) {
                for (int i = 0; i < numberOfSequences; i++) {
                    SequenceDocument sequenceDocument = sequenceDocuments.get(i);
                    String sequenceDocumentName = sequenceDocument.getName();
                    if (referenceGenomeNames.contains(sequenceDocumentName)) {
                        referenceSequenceIndex = i;
                        break;
                    }
                }
            }
            if (referenceSequenceIndex == -1) {
                NonFatalError.ALIGNMENTS_WITHOUT_REFERENCE_SEQUENCES.names.add(defaultAlignmentDocumentName);
                continue;
            }
            SequenceDocument referenceSequenceDocument = sequenceDocuments.get(referenceSequenceIndex);
            String referenceSequenceName = referenceSequenceDocument.getName();
            String referenceSequence = referenceSequenceDocument.getSequenceString().toLowerCase();

            String nopv2ReferenceSequence = null;
            for (SequenceDocument sequenceDocument : sequenceDocuments) {
                String sequenceDocumentName = sequenceDocument.getName();
                if (sequenceDocumentName.equals("nOPV2") || sequenceDocumentName.equals("MZ245455")) {
                    nopv2ReferenceSequence = sequenceDocument.getSequenceString().toLowerCase();
                    break;
                }
            }

            List<SequenceAnnotation> referenceSequenceAnnotations = referenceSequenceDocument.getSequenceAnnotations();
            if (defaultAlignmentDocumentName.matches(".*[Vv][Pp]1\\s*$")) {
                List<SequenceAnnotation> vp1Annotations = referenceSequenceAnnotations.stream().filter(
                    referenceSequenceAnnotation -> referenceSequenceAnnotation.getName().matches("^[Vv][Pp]1$")
                ).collect(
                    Collectors.toList()
                );
                if (vp1Annotations.size() < 1) {
                    NonFatalError.ALIGNMENTS_WITHOUT_REFERENCE_SEQUENCES.names.add(defaultAlignmentDocumentName);
                    continue;
                }
                if (vp1Annotations.size() > 1) {
                    NonFatalError.ALIGNMENTS_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS.names.add(defaultAlignmentDocumentName);
                    continue;
                }
                SequenceAnnotation vp1Annotation = vp1Annotations.get(0);
                int numberOfIntervals = vp1Annotation.getNumberOfIntervals();
                if (numberOfIntervals != 1) {
                    NonFatalError.ALIGNMENTS_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS.names.add(defaultAlignmentDocumentName);
                    continue;
                }
                SequenceAnnotationInterval vp1AnnotationInterval = vp1Annotation.getInterval();
                int vp1AnnotationMinimumIndex = vp1AnnotationInterval.getMinimumIndex();
                int vp1AnnotationMaximumIndex = vp1AnnotationInterval.getMaximumIndex();
                for (SequenceDocument sequenceDocument : sequenceDocuments) {
                    String sequenceDocumentName = sequenceDocument.getName();
                    Matcher sequenceDocumentNameMatcher;
                    if (
                        !(sequenceDocumentNameMatcher = DOCUMENT_NAME_PATTERN.matcher(sequenceDocumentName)).matches() &&
                        !(sequenceDocumentNameMatcher = DOCUMENT_NAME_PATTERN_FOR_CONTROLS.matcher(sequenceDocumentName)).matches()
                    ) {
                        continue;
                    }
                    String sampleName = formatSampleName(
                        sequenceDocumentNameMatcher.group(1),
                        sequenceDocumentNameMatcher.group(2)
                    );
                    String type = sequenceDocumentNameMatcher.group(3);
                    type = type.replaceAll("\\s+", "");
                    HashMap<String, ExcelData> excelDataPerSample = excelData.get(sampleName);
                    String genomeName = formatGenomeName(
                        sampleName,
                        type
                    );
                    if (excelDataPerSample == null) {
                        NonFatalError.GENOMES_WITH_NO_CORRESPONDING_FILES_WITHIN_THE_FINAL_ASSEMBLIES_FOLDER.names.add(
                            genomeName
                        );
                        continue;
                    }
                    ExcelData excelDataPerSamplePerType = excelDataPerSample.get(type);
                    if (excelDataPerSamplePerType == null) {
                        NonFatalError.GENOMES_WITH_NO_CORRESPONDING_FILES_WITHIN_THE_FINAL_ASSEMBLIES_FOLDER.names.add(
                            genomeName
                        );
                        continue;
                    }
                    String sequence = sequenceDocument.getSequenceString().toLowerCase();
                    List<SingleNucleotidePolymorphism> vp1Snps = new LinkedList<>();
                    int vp1GapCount = 0;
                    for (int i = vp1AnnotationMinimumIndex; i <= vp1AnnotationMaximumIndex; i++) {
                        char reference = referenceSequence.charAt(i - 1);
                        char alternate = sequence.charAt(i - 1);
                        if (
                            reference == 'U' ||
                            reference == 'u'
                        ) {
                            reference--;
                        }
                        if (
                            alternate == 'U' ||
                            alternate == 'u'
                        ) {
                            alternate--;
                        }
                        if (alternate == '-') {
                            excelDataPerSamplePerType.partialVp1CoverageFlag = true;
                        } else if (
                            alternate == 'N' ||
                            alternate == 'n'
                        ) {
                            vp1GapCount++;
                        } else if (reference != alternate) {
                            vp1Snps.add(new SingleNucleotidePolymorphism(
                                referenceSequenceName,
                                reference,
                                alternate,
                                i - vp1AnnotationMinimumIndex + 1
                            ));
                        }
                    }
                    excelDataPerSamplePerType.vp1Snps = vp1Snps;
                    excelDataPerSamplePerType.vp1GapCount = vp1GapCount;
                }
            } else {
                String genomeName = String.format(
                    "Reference sequence \"%s\" within alignment \"%s\"",
                    referenceSequenceName,
                    defaultAlignmentDocumentName
                );
                List<SequenceAnnotation> cdsMatchingSequenceAnnotations = referenceSequenceAnnotations.stream().filter(
                    sequenceAnnotation -> sequenceAnnotation.getName().matches("[Cc][Dd][Ss]")
                ).collect(Collectors.toList());
                if (cdsMatchingSequenceAnnotations.size() < 1) {
                    NonFatalError.GENOMES_WITHOUT_A_CDS_ANNOTATION.names.add(genomeName);
                    continue;
                }
                if (cdsMatchingSequenceAnnotations.size() > 1) {
                    NonFatalError.GENOMES_WITH_AN_UNEXPECTED_NUMBER_OF_CDS_ANNOTATION_INTERVALS.names.add(genomeName);
                    continue;
                }
                SequenceAnnotation cdsSequenceAnnotation = cdsMatchingSequenceAnnotations.get(0);
                int numberOfIntervals = cdsSequenceAnnotation.getNumberOfIntervals();
                if (numberOfIntervals != 1) {
                    NonFatalError.GENOMES_WITH_AN_UNEXPECTED_NUMBER_OF_CDS_ANNOTATION_INTERVALS.names.add(genomeName);
                    continue;
                }
                SequenceAnnotationInterval cdsAnnotationInterval = cdsSequenceAnnotation.getInterval();
                int cdsAnnotationMinimumIndex = cdsAnnotationInterval.getMinimumIndex();
//                int cdsAnnotationMinimumIndex = Math.max(
//                    0,
//                    cdsAnnotationInterval.getMinimumIndex()
//                );
                int cdsAnnotationMaximumIndex = cdsAnnotationInterval.getMaximumIndex();
//                int cdsAnnotationMaximumIndex = Math.min(
//                    referenceSequence.length(),
//                    cdsAnnotationInterval.getMaximumIndex()
//                );
                List<SequenceAnnotation> maturePeptideSequenceAnnotations = referenceSequenceAnnotations.stream().filter(
                    sequenceAnnotation -> sequenceAnnotation.getType().equals(MATURE_PEPTIDE_SEQUENCE_ANNOTATION_TYPE)
                ).collect(Collectors.toList());

                if (maturePeptideSequenceAnnotations.stream().anyMatch(
                    sequenceAnnotation -> sequenceAnnotation.getNumberOfIntervals() != 1
                )) {
                    NonFatalError.GENOMES_WITH_AN_UNEXPECTED_NUMBER_OF_MATURE_PEPTIDE_ANNOTATION_INTERVALS.names.add(genomeName);
                    continue;
                }
                maturePeptideSequenceAnnotations = referenceSequenceAnnotations.stream().sorted(
                    Comparator.comparing(
                        maturePeptideSequenceAnnotation -> maturePeptideSequenceAnnotation.getInterval().getMinimumIndex()
                    )
                ).collect(Collectors.toList());
                for (SequenceDocument sequenceDocument : sequenceDocuments) {
                    String sequenceDocumentName = sequenceDocument.getName();

                    Matcher sequenceDocumentNameMatcher;
                    if (
                        !(sequenceDocumentNameMatcher = DOCUMENT_NAME_PATTERN.matcher(sequenceDocumentName)).matches() &&
                        !(sequenceDocumentNameMatcher = DOCUMENT_NAME_PATTERN_FOR_CONTROLS.matcher(sequenceDocumentName)).matches()
                    ) {
                        continue;
                    }
                    String sampleName = formatSampleName(
                        sequenceDocumentNameMatcher.group(1),
                        sequenceDocumentNameMatcher.group(2)
                    );
                    String type = sequenceDocumentNameMatcher.group(3);
                    type = type.replaceAll("\\s+", "");
                    String simplifiedType = type;
                    Matcher duplicateTypeMatcher = DUPLICATE_TYPE_PER_SAMPLE_PATTERN.matcher(type);
                    if (duplicateTypeMatcher.matches()) {
                        simplifiedType = duplicateTypeMatcher.group(1);
//                        simplifiedType = simplifiedType.strip();
                    }
                    HashMap<String, ExcelData> excelDataPerSample = excelData.get(sampleName);
                    if (excelDataPerSample == null) {
                        NonFatalError.GENOMES_WITH_NO_CORRESPONDING_FILES_WITHIN_THE_FINAL_ASSEMBLIES_FOLDER.names.add(genomeName);
                        continue;
                    }
                    ExcelData excelDataPerSamplePerType = excelDataPerSample.get(type);
                    if (excelDataPerSamplePerType == null) {
                        NonFatalError.GENOMES_WITH_NO_CORRESPONDING_FILES_WITHIN_THE_FINAL_ASSEMBLIES_FOLDER.names.add(genomeName);
                        continue;
                    }
                    String sequence = sequenceDocument.getSequenceString().toLowerCase();
                    int cdsGapCount = 0;
                    for (int i = cdsAnnotationMinimumIndex; i <= cdsAnnotationMaximumIndex; i++) {
                        char sequenceI = sequence.charAt(i - 1);
                        if (
                            sequenceI == 'N' ||
                            sequenceI == 'n'
                        ) {
                            cdsGapCount++;
                        }
                    }
                    excelDataPerSamplePerType.cdsGapCount = cdsGapCount;
                    int sequenceLength = sequence.length();
                    Integer lowestIndexOfNonGap = null;
                    for (int i = 0; i < sequenceLength; i++) {
                        if (sequence.charAt(i) != '-') {
                            lowestIndexOfNonGap = i;
                            break;
                        }
                    }
                    Integer highestIndexOfNonGap = null;
                    for (int i = sequenceLength - 1; i >= 0; i--) {
                        if (sequence.charAt(i) != '-') {
                            highestIndexOfNonGap = i;
                            break;
                        }
                    }
                    if (
                        lowestIndexOfNonGap == null ||
                        highestIndexOfNonGap == null
                    ) {
                        continue;
                    }

                    int fivePrimeStart = lowestIndexOfNonGap;
                    int fivePrimeStop = highestIndexOfNonGap;
                    SequenceAnnotation lowerBoundingMaturePeptideAnnotation = null;
                    for (SequenceAnnotation maturePeptideSequenceAnnotation : maturePeptideSequenceAnnotations) {
                        SequenceAnnotationInterval maturePeptideSequenceAnnotationInterval = maturePeptideSequenceAnnotation.getInterval();
                        int maturePeptideSequenceAnnotationMinimumIndex = maturePeptideSequenceAnnotationInterval.getMinimumIndex();
                        int maturePeptideSequenceAnnotationMaximumIndex = maturePeptideSequenceAnnotationInterval.getMaximumIndex();
                        if (
                            lowestIndexOfNonGap >= maturePeptideSequenceAnnotationMinimumIndex &&
                            lowestIndexOfNonGap <= maturePeptideSequenceAnnotationMaximumIndex
                        ) {
                            lowerBoundingMaturePeptideAnnotation = maturePeptideSequenceAnnotation;
                        }
                        if (fivePrimeStop > maturePeptideSequenceAnnotationMaximumIndex) {
                            fivePrimeStop = maturePeptideSequenceAnnotationMaximumIndex;
                        }
                    }
                    if (lowerBoundingMaturePeptideAnnotation == null) {
                        // The sequence is not partial w.r.t. the 5' region. Use the 5' UTR.
                        int fivePrimeUtrMaximumIndex = maturePeptideSequenceAnnotations.get(0).getInterval().getMinimumIndex() - 1;
                        if (fivePrimeStop > fivePrimeUtrMaximumIndex) {
                            fivePrimeStop = fivePrimeUtrMaximumIndex;
                        }
                    }
                    int threePrimeStart = lowestIndexOfNonGap;
                    int threePrimeStop = highestIndexOfNonGap;
                    SequenceAnnotation upperBoundingMaturePeptideAnnotation = null;
                    for (SequenceAnnotation maturePeptideSequenceAnnotation : maturePeptideSequenceAnnotations) {
                        SequenceAnnotationInterval maturePeptideSequenceAnnotationInterval = maturePeptideSequenceAnnotation.getInterval();
                        int maturePeptideSequenceAnnotationMinimumIndex = maturePeptideSequenceAnnotationInterval.getMinimumIndex();
                        int maturePeptideSequenceAnnotationMaximumIndex = maturePeptideSequenceAnnotationInterval.getMaximumIndex();
                        if (
                            highestIndexOfNonGap >= maturePeptideSequenceAnnotationMinimumIndex &&
                            highestIndexOfNonGap <= maturePeptideSequenceAnnotationMaximumIndex
                        ) {
                            upperBoundingMaturePeptideAnnotation = maturePeptideSequenceAnnotation;
                        }
                        if (threePrimeStart < maturePeptideSequenceAnnotationMinimumIndex) {
                            threePrimeStart = maturePeptideSequenceAnnotationMinimumIndex;
                        }
                    }
                    if (upperBoundingMaturePeptideAnnotation == null) {
                        // The sequence is not partial w.r.t. the 3' region Howesver, the 3' UTR is too small to use to determine recombination status.
                        int threePrimeUtrMinimumIndex = maturePeptideSequenceAnnotations.get(maturePeptideSequenceAnnotations.size() - 1).getInterval().getMinimumIndex();
//                        int threePrimeUtrMinimumIndex = maturePeptideSequenceAnnotations.get(maturePeptideSequenceAnnotations.size() - 1).getInterval().getMaximumIndex() + 1;
                        if (threePrimeStart < threePrimeUtrMinimumIndex) {
                            threePrimeStart = threePrimeUtrMinimumIndex;
                        }
                    }

                    int vp1Start = lowestIndexOfNonGap;
                    int vp1Stop = highestIndexOfNonGap;
                    List<SequenceAnnotation> vp1MatchingSequenceAnnotations = maturePeptideSequenceAnnotations.stream().filter(
                        sequenceAnnotation -> sequenceAnnotation.getName().matches("[Vv][Pp]1")
                    ).collect(Collectors.toList());
                    if (vp1MatchingSequenceAnnotations.size() != 1) {
                        NonFatalError.ALIGNMENTS_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS.names.add(defaultAlignmentDocumentName);
                    } else {
                        SequenceAnnotation vp1SequenceAnnotation = vp1MatchingSequenceAnnotations.get(0);
                        if (vp1SequenceAnnotation.getNumberOfIntervals() != 1) {
                            NonFatalError.ALIGNMENTS_WITH_AN_UNEXPECTED_NUMBER_OF_VP1_ANNOTATION_INTERVALS.names.add(defaultAlignmentDocumentName);
                        } else {
                            SequenceAnnotationInterval vp1SequenceAnnotationInterval = vp1SequenceAnnotation.getInterval();
                            int vp1SequenceAnnotationMinimumIndex = vp1SequenceAnnotationInterval.getMinimumIndex();
                            int vp1SequenceAnnotationMaximumIndex = vp1SequenceAnnotationInterval.getMaximumIndex();
                            if (vp1Start < vp1SequenceAnnotationMinimumIndex) {
                                vp1Start = vp1SequenceAnnotationMinimumIndex;
                            }
                            if (vp1Stop > vp1SequenceAnnotationMaximumIndex) {
                                vp1Stop = vp1SequenceAnnotationMaximumIndex;
                            }
                        }
                    }

                    SnpsCalculationData fivePrimeSnpsCalculationData = new SnpsCalculationData(
                            fivePrimeStart,
                            fivePrimeStop
                    );
                    SnpsCalculationData threePrimeSnpsCalculationData = new SnpsCalculationData(
                            threePrimeStart,
                            threePrimeStop
                    );
                    SnpsCalculationData vp1SnpsCalculationData = new SnpsCalculationData(
                            vp1Start,
                            vp1Stop
                    );
                    List<SnpsCalculationData> snpsCalculationDataList = List.of(
                        fivePrimeSnpsCalculationData,
                        threePrimeSnpsCalculationData,
                        vp1SnpsCalculationData
                    );
                    for (SnpsCalculationData snpsCalculationData : snpsCalculationDataList) {
                        String localReferenceSequence = referenceSequence;
                        if (simplifiedType.equalsIgnoreCase("nOPV2")) {
                            if (nopv2ReferenceSequence == null) {
                                throw new DocumentOperationException(String.format(
                                    "Alignment document \"%s\" is missing nOPV2/MZ245455, but it contains an nOPV2 genome. Please add MZ245455 to this alignment and re-run the operation.",
                                    defaultAlignmentDocumentName
                                ));
                            }
                            localReferenceSequence = nopv2ReferenceSequence;
                        }
                        int start = snpsCalculationData.start;
                        int stop = snpsCalculationData.stop;
                        List<Integer> indicesOfSnps = getIndicesOfSnps(
                            localReferenceSequence,
                            sequence,
                            start,
                            stop,
                            snpsCalculationData
                        );
                        if (
                            indicesOfSnps.size() == 0 &&
                            snpsCalculationData == vp1SnpsCalculationData
                        ) {
                            indicesOfSnps = getIndicesOfSnps(
                                localReferenceSequence,
                                sequence,
                                lowestIndexOfNonGap,
                                highestIndexOfNonGap,
                                // Avoid overwriting to snpsCalculationData.numberOfGapsEncountered
                                null
                            );
                        }
                        snpsCalculationData.numberOfSnpsEncountered = indicesOfSnps.size();
                        List<Double> distances = new LinkedList<>();
                        for (int i = start; i <= stop; i++) {
                            double distance = Double.MAX_VALUE;
                            for (int indexOfSnp : indicesOfSnps) {
                                int dif = i - indexOfSnp;
                                double newDistance = Math.sqrt(dif * dif);
                                if (distance > newDistance) {
                                    distance = newDistance;
                                }
                            }
                            distances.add(distance);
                        }
                        snpsCalculationData.distances = distances;
                    }
                    if (
                        vp1SnpsCalculationData.numberOfGapsEncountered > 0 ||
                        threePrimeSnpsCalculationData.numberOfGapsEncountered > 0 ||
                        fivePrimeSnpsCalculationData.numberOfGapsEncountered > 0
                    ) {
                        excelDataPerSamplePerType.recombinationNote = "recombination status indeterminate due to gaps";
                    } else if (
                        vp1SnpsCalculationData.numberOfSnpsEncountered > 0
                    ) {
                        double vp1AverageDistance = average(vp1SnpsCalculationData.distances);
                        double vp1StandardDeviation = standardDeviation(
                            vp1SnpsCalculationData.distances,
                            false
                        );
                        double vp1VarianceOverVp1Length = vp1StandardDeviation * vp1StandardDeviation / (vp1Stop - vp1Start + 1);


                        boolean fivePrimeRecombinationFlag = false;
                        boolean threePrimeRecombinationFlag = false;
                        int fivePrimeLength = fivePrimeStop - fivePrimeStart + 1;
                        if (fivePrimeSnpsCalculationData.numberOfSnpsEncountered / ((double)fivePrimeLength) >= FIVE_PRIME_MINIMUM_SNP_FREQUENCY) {
                            double fivePrimeAverageDistance = average(fivePrimeSnpsCalculationData.distances);
                            double fivePrimeStandardDeviation = standardDeviation(
                                fivePrimeSnpsCalculationData.distances,
                                false
                            );
                            double fivePrimeTStatistic = (vp1AverageDistance - fivePrimeAverageDistance) / Math.sqrt(
                                fivePrimeStandardDeviation * fivePrimeStandardDeviation / fivePrimeLength +
                                vp1VarianceOverVp1Length
                            );
                            fivePrimeRecombinationFlag = fivePrimeTStatistic > T_STATISTIC_ONE_TAIL_THRESHOLD;
                        }

                        int threePrimeLength = threePrimeStop - threePrimeStart + 1;
                        if (threePrimeSnpsCalculationData.numberOfSnpsEncountered / ((double)threePrimeLength) >= THREE_PRIME_MINIMUM_SNP_FREQUENCY) {
                            double threePrimeAverageDistance = average(threePrimeSnpsCalculationData.distances);
                            double threePrimeStandardDeviation = standardDeviation(
                                threePrimeSnpsCalculationData.distances,
                                false
                            );
                            double threePrimeTStatistic = (vp1AverageDistance - threePrimeAverageDistance) / Math.sqrt(
                                threePrimeStandardDeviation * threePrimeStandardDeviation / threePrimeLength +
                                vp1VarianceOverVp1Length
                            );
                            threePrimeRecombinationFlag = threePrimeTStatistic > T_STATISTIC_ONE_TAIL_THRESHOLD;
                        }
                        
                        if (fivePrimeRecombinationFlag && threePrimeRecombinationFlag) {
                            excelDataPerSamplePerType.recombinationNote = "double recombination";
                        } else if (fivePrimeRecombinationFlag) {
                            excelDataPerSamplePerType.recombinationNote = "5' recombination";
                        } else if (threePrimeRecombinationFlag) {
                            excelDataPerSamplePerType.recombinationNote = "3' recombination";
                        } else {
                            excelDataPerSamplePerType.recombinationNote = null;
                        }
                    } else {
                        excelDataPerSamplePerType.recombinationNote = null;
                    }

                    boolean lowerBoundPartialCdsFlag = lowestIndexOfNonGap > cdsAnnotationMinimumIndex;
                    boolean upperBoundPartialCdsFlag = highestIndexOfNonGap < cdsAnnotationMaximumIndex;
                    if (
                        !lowerBoundPartialCdsFlag &&
                        !upperBoundPartialCdsFlag
                    ) {
                        continue;
                    }
                    List<String> partialCdsNotePortions = new LinkedList<>();
                    if (lowerBoundPartialCdsFlag) {
                        String maturePeptideAnnotationName = lowerBoundingMaturePeptideAnnotation == null ? MISSING_MATURE_PEPTIDE_ANNOTATION : lowerBoundingMaturePeptideAnnotation.getName();
                        partialCdsNotePortions.add(String.format(
                            "%s-",
                            maturePeptideAnnotationName
                        ));
                    }
                    if (upperBoundPartialCdsFlag) {
                        String maturePeptideAnnotationName = upperBoundingMaturePeptideAnnotation == null ? MISSING_MATURE_PEPTIDE_ANNOTATION : upperBoundingMaturePeptideAnnotation.getName();
                        partialCdsNotePortions.add(String.format(
                            "-%s",
                            maturePeptideAnnotationName
                        ));
                    }
                    excelDataPerSamplePerType.partialCdsNote = String.format(
                        "Partial CDS (%s)",
                        String.join(
                            ":",
                            partialCdsNotePortions
                        )
                    );
                }
            }
        }
        for (String sampleName : excelData.keySet()) {
            HashMap<String, ExcelData> excelDataPerSample = excelData.get(sampleName);
            if (excelDataPerSample == null) {
                continue;
            }
            for (String type : excelDataPerSample.keySet()) {
                ExcelData excelDataPerSamplePerType = excelDataPerSample.get(type);

                if (excelDataPerSamplePerType.defaultAlignmentDocumentAnnotatedPluginDocument != null) {
                    parseExcelDataFromDocumentsTasks.add(() -> {
                        excelDataPerSamplePerType.parseAlignmentDocument(
                            sampleName,
                            type
                        );
                        return null;
                    });
                }
                if (excelDataPerSamplePerType.nucleotideSequenceDocumentAnnotatedPluginDocument != null) {
                    parseExcelDataFromDocumentsTasks.add(() -> {
                        excelDataPerSamplePerType.parseNucleotideSequenceDocument(
                            sampleName,
                            type
                        );
                        return null;
                    });
                }
            }
        }

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
        try {
            List<Future<Void>> taskReturnValues = executorService.invokeAll(parseExcelDataFromDocumentsTasks);
            for (Future<Void> taskReturnValue : taskReturnValues) {
                taskReturnValue.get();
            }
        } catch (ExecutionException | InterruptedException e) {
            throw new DocumentOperationException(e.getCause());
        }

        CompositeProgressListener compositeProgressListener = new CompositeProgressListener(
            progressListener,
            defaultNucleotideSequenceAnnotatedPluginDocuments.size() + 2
        );
        DocumentOperation alignmentOperation = PluginUtilities.getAlignmentOperation(
            "MAFFT",
            SequenceDocument.Alphabet.NUCLEOTIDE
        );
        Options alignmentOptions = alignmentOperation.getOptions(vp1SequenceDocuments);
        List<AnnotatedPluginDocument> alignedVp1Segments = alignmentOperation.performOperation(
            vp1SequenceDocuments,
            compositeProgressListener,
            alignmentOptions
        );
        vp1SequenceDocuments.clear();
        DefaultAlignmentDocument vp1AlignmentDocument = (DefaultAlignmentDocument)alignedVp1Segments.get(0).getDocument();
        List<SequenceDocument> sequenceDocuments = vp1AlignmentDocument.getSequences();
        for (SequenceDocument sequenceDocumentI : sequenceDocuments) {
            String sequenceDocumentIName = sequenceDocumentI.getName();
            Matcher sequenceDocumentINameMatcher = ALIGNED_SEQUENCE_PATTERN.matcher(sequenceDocumentIName);
            if (!sequenceDocumentINameMatcher.matches()) {
                // Only perform these calculations for input sequences (not references genomes).
                continue;
            }
            String csid = sequenceDocumentINameMatcher.group(1);
            String cuid = sequenceDocumentINameMatcher.group(2);
            String type = sequenceDocumentINameMatcher.group(3);
            type = type.replaceAll("\\s+", "");
            String sampleName = formatSampleName(
                csid,
                cuid
            );
            if (!databaseData.containsKey(sampleName)) {
                databaseData.put(
                    sampleName,
                    new HashMap<>()
                );
            }
            HashMap<String, DatabaseData> databaseDataPerSample = databaseData.get(sampleName);
            if (!databaseDataPerSample.containsKey(type)) {
                databaseDataPerSample.put(
                    type,
                    new DatabaseData()
                );
            }
            DatabaseData databaseDataPerSamplePerType = databaseDataPerSample.get(type);
            String dbName = String.join(
                "_",
                shortenedRunName,
                csid,
                cuid,
                type
            );
            String sequenceI = sequenceDocumentI.getSequenceString();

            int lengthI = sequenceI.length();
            int minimumDifferencesCount = Integer.MAX_VALUE;
            String nameOfClosestVp1ReferenceSequence = null;
            double maximumPercentIdentity = -1;
            for (SequenceDocument sequenceDocumentJ : sequenceDocuments) {
                String sequenceDocumentJName = sequenceDocumentJ.getName();
                Matcher sequenceDocumentJNameMatcher = ALIGNED_SEQUENCE_PATTERN.matcher(sequenceDocumentJName);
                if (sequenceDocumentJNameMatcher.matches()) {
                    // Do not attempt to classify input (non-reference) sequences according to other input sequences
                    continue;
                }
                String sequenceJ = sequenceDocumentJ.getSequenceString();
                int lengthJ = sequenceJ.length();
                int minimumLength = Math.min(
                    lengthI,
                    lengthJ
                );
                int maximumLength = Math.max(
                    lengthI,
                    lengthJ
                );
                int differencesCount = 0;
                for (int sequenceIndex = 0; sequenceIndex < minimumLength; sequenceIndex++) {
                    char characterI = sequenceI.charAt(sequenceIndex);
                    char characterJ = sequenceJ.charAt(sequenceIndex);
                    if (
                        characterI == 'U' ||
                        characterI == 'u'
                    ) {
                        characterI--;
                    }
                    if (
                        characterJ == 'U' ||
                        characterJ == 'u'
                    ) {
                        characterJ--;
                    }
                    if (characterI != characterJ) {
                        differencesCount++;
                    }
                }
                if (differencesCount < minimumDifferencesCount) {
                    minimumDifferencesCount = differencesCount;
                    nameOfClosestVp1ReferenceSequence = sequenceDocumentJName;
                    maximumPercentIdentity = 1d - differencesCount / (double)maximumLength;
                }
            }
            databaseDataPerSamplePerType.dbName = dbName;
            databaseDataPerSamplePerType.genotypeSeq = nameOfClosestVp1ReferenceSequence;
            databaseDataPerSamplePerType.percentIdentity = maximumPercentIdentity;
            databaseDataPerSamplePerType.nucleotideDifferencesOfGenotypeCount = minimumDifferencesCount;
        }

        List<String> orderedListOfSampleNames = new LinkedList<>(excelData.keySet());
        orderedListOfSampleNames.sort(compareStringsIgnoreCase);
        List<String> documentLines = new LinkedList<>();
        documentLines.add(
            Stream.of(FinalAnalysisExcelColumn.values()).map(
                excelColumn -> excelColumn.titleLine0
            ).collect(
                Collectors.joining(",")
            )
        );
        documentLines.add(
            Stream.of(FinalAnalysisExcelColumn.values()).map(
                excelColumn -> excelColumn.titleLine1
            ).collect(
                Collectors.joining(",")
            )
        );
        for (String sampleName : orderedListOfSampleNames) {
            HashMap<String, ExcelData> excelDataPerSample = excelData.get(sampleName);
            if (excelDataPerSample == null) {
                documentLines.add(
                    Stream.of(FinalAnalysisExcelColumn.values()).map(
                        excelColumn -> excelColumn.nullExcelDatumToString.apply(
                            sampleName
                        )
                    ).collect(
                        Collectors.joining(",")
                    )
                );
                continue;
            }
            List<String> types = new LinkedList<>(excelDataPerSample.keySet());
            types.sort(compareStringsIgnoreCase);
            for (String type : types) {
                ExcelData excelDataPerSamplePerType = excelDataPerSample.get(type);
                documentLines.add(
                    Stream.of(FinalAnalysisExcelColumn.values()).map(
                        excelColumn -> excelColumn.excelDatumToString.print(
                            sampleName,
                            type,
                            excelDataPerSamplePerType
                        )
                    ).collect(
                        Collectors.joining(",")
                    )
                );
            }
        }
        String finalAnalysisExcelDocumentText = String.join(
            "\n",
            documentLines
        );
        documentLines.clear();

        documentLines.add(
            Stream.of(DatabaseDataExcelColumn.values()).map(
                databaseDataExcelColumn -> databaseDataExcelColumn.title
            ).collect(
                Collectors.joining(",")
            )
        );
        for (String sampleName : databaseData.keySet()) {
            HashMap<String, DatabaseData> databaseDataPerSample = databaseData.get(sampleName);
            HashMap<String, ExcelData> excelDataPerSample = excelData.get(sampleName);
            for (String type : databaseDataPerSample.keySet()) {
                DatabaseData databaseDataPerSamplePerType = databaseDataPerSample.get(type);
                ExcelData excelDataPerSamplePerType = excelDataPerSample == null ? null : excelDataPerSample.get(type);
                if (excelDataPerSamplePerType == null) {
                    documentLines.add(
                        Stream.of(DatabaseDataExcelColumn.values()).map(
                            databaseDataExcelColumn -> databaseDataExcelColumn.nullExcelDatumToString.apply(
                                databaseDataPerSamplePerType
                            )
                        ).collect(
                            Collectors.joining(",")
                        )
                    );
                } else {
                    documentLines.add(
                        Stream.of(DatabaseDataExcelColumn.values()).map(
                            databaseDataExcelColumn -> databaseDataExcelColumn.excelDatumToString.print(
                                databaseDataPerSamplePerType,
                                excelDataPerSamplePerType
                            )
                        ).collect(
                            Collectors.joining(",")
                        )
                    );
                }
            }
        }
        String databaseDataExcelDocumentText = String.join(
            "\n",
            documentLines
        );
        documentLines.clear();

        documentLines.add(
            Stream.of(VP1SnpsExcelColumn.values()).map(
                vp1SnpsExcelColumn -> vp1SnpsExcelColumn.title
            ).collect(
                Collectors.joining(",")
            )
        );
        for (String sampleName : orderedListOfSampleNames) {
            HashMap<String, ExcelData> excelDataPerSample = excelData.get(sampleName);
            if (excelDataPerSample == null) {
                continue;
            }
            List<String> orderedListOfTypes = new LinkedList<>(excelDataPerSample.keySet());
            orderedListOfTypes.sort(compareStringsIgnoreCase);
            for (String type : orderedListOfTypes) {
                ExcelData excelDataPerSamplePerType = excelDataPerSample.get(type);
                if (excelDataPerSamplePerType.vp1Snps == null) {
                    continue;
                }
                for (SingleNucleotidePolymorphism vp1Snp : excelDataPerSamplePerType.vp1Snps) {
                    documentLines.add(Stream.of(VP1SnpsExcelColumn.values()).map(vp1SnpsExcelColumn -> vp1SnpsExcelColumn.snpToString.print(
                        sampleName,
                        type,
                        vp1Snp
                    )).collect(Collectors.joining(",")));
                }
            }
        }
        String vp1SnpsExcelDocumentText = String.join(
            "\n",
            documentLines
        );
        documentLines.clear();

        Map<String, String> prefixToFileTextMap = Map.of(
            FINAL_ANALYSIS, finalAnalysisExcelDocumentText,
            DATABASE_DATA, databaseDataExcelDocumentText,
            VP1_SNPS, vp1SnpsExcelDocumentText
        );
        HashMap<String, File> prefixToCsvFileMap = new HashMap<>();
        try {
            for (String prefix : prefixToFileTextMap.keySet()) {
                File csvFile = File.createTempFile(
                    prefix,
                    ".csv"
                );
                csvFile.deleteOnExit();
                BufferedWriter bufferedWriter = new BufferedWriter(
                    new FileWriter(
                        csvFile,
                        false
                    )
                );
                bufferedWriter.write(prefixToFileTextMap.get(prefix));
                bufferedWriter.close();
                prefixToCsvFileMap.put(
                    prefix,
                    csvFile
                );
            }
        } catch (IOException ioException) {
            throw new DocumentOperationException(ioException);
        }

        Path exportDirectoryPathPerRun = Paths.get(String.format(
            "\\\\cdc.gov\\project\\OID_DVD_AmdNGS\\AnalysisOutput\\%s",
            fullRunName
        ));
        Path exportDirectoryPathPerRunPerBioinformatician = exportDirectoryPathPerRun.resolve(bioinformaticianName);
        PythonProcess pythonProcess = new PythonProcess(
            INTERACT_WITH_FINAL_ANALYSIS_DOCUMENT_PYTHON_SCRIPT_NAME,
            pythonScriptsFolderPath.toAbsolutePath().toString(),
            System.out::println
        );
        Integer exitValue;
        try {
            exitValue = pythonProcess.execute(
                true,
                String.format(
                    "-w %s -f %s -d %s -v %s -s %s -r %s -c %s -u %s -g %s -a %s",
                    wetLabExcelFilePathAsString,
                    prefixToCsvFileMap.get(FINAL_ANALYSIS).getAbsolutePath(),
                    prefixToCsvFileMap.get(DATABASE_DATA).getAbsolutePath(),
                    prefixToCsvFileMap.get(VP1_SNPS).getAbsolutePath(),
                    shortenedRunName,
                    fullRunName,
                    bioinformaticianCdcId,
                    uploadExcelDocumentToSharepointFlag ? "True" : "False",
                    generateEmailFlag ? "True" : "False",
                    exportDirectoryPathPerRunPerBioinformatician.toAbsolutePath()
                )
            );
        } catch (IOException | InterruptedException | DatabaseServiceException e) {
            throw new DocumentOperationException(e);
        }
        if (exitValue == null) {
            throw new DocumentOperationException("exitValue should never be null.");
        }
        pythonProcess.validateExitValue(exitValue);
        if (exportFinalAnalysisFolderAndFilesFlag) {
            try {
                if (!Files.exists(exportDirectoryPathPerRun)) {
                    Files.createDirectory(exportDirectoryPathPerRun);
                }
                Files.copy(
                    wetLabExcelFilePath,
                    exportDirectoryPathPerRun.resolve(wetLabExcelFilePath.getFileName()),
                    StandardCopyOption.REPLACE_EXISTING
                );

                if (!Files.exists(exportDirectoryPathPerRunPerBioinformatician)) {
                    Files.createDirectory(exportDirectoryPathPerRunPerBioinformatician);
                }
                String finalAnalysisFileName = wetLabExcelFilePath.getFileName().toString().replaceAll(
                    ".xlsx$",
                    "_FinalAnalysis.xlsx"
                );
                Path finalAnalysisFilePath = wetLabExcelFilePath.getParent().resolve(finalAnalysisFileName);
                Files.copy(
                    finalAnalysisFilePath,
                    exportDirectoryPathPerRunPerBioinformatician.resolve(finalAnalysisFileName),
                    StandardCopyOption.REPLACE_EXISTING
                );
                AnnotatedPluginDocument[] documentsToExport = defaultNucleotideSequenceAnnotatedPluginDocuments.toArray(AnnotatedPluginDocument[]::new);

                Path outputPath = exportDirectoryPathPerRunPerBioinformatician.resolve(String.format(
                    "%d documents from %s.fasta",
                    defaultNucleotideSequenceAnnotatedPluginDocuments.size(),
                    FINAL_ASSEMBLIES_FOLDER_NAME
                ));
                Files.deleteIfExists(outputPath);
                compositeProgressListener.beginNextSubtask();
                GeneiousPlugin fastaExporterGeneiousPlugin = getGeneiousPlugin(
                    FASTA_EXPORTER_PLUGIN_NAME,
                    true
                );
                DocumentFileExporter fastaFileExporter = fastaExporterGeneiousPlugin.getDocumentFileExporters()[1];
                fastaFileExporter.export(
                    outputPath.toFile(),
                    documentsToExport,
                    compositeProgressListener,
                    fastaFileExporter.getOptions(documentsToExport)
                );

                outputPath = exportDirectoryPathPerRunPerBioinformatician.resolve(String.format(
                    "%d documents from %s.geneious",
                    defaultNucleotideSequenceAnnotatedPluginDocuments.size(),
                    FINAL_ASSEMBLIES_FOLDER_NAME
                ));
                Files.deleteIfExists(outputPath);
                PluginUtilities.exportDocumentsInGeneiousFormat(
                    outputPath.toFile(),
                    true,
                    documentsToExport
                );

                for (AnnotatedPluginDocument defaultNucleotideSequenceAnnotatedPluginDocument : defaultNucleotideSequenceAnnotatedPluginDocuments) {
                    outputPath = exportDirectoryPathPerRunPerBioinformatician.resolve(String.format(
                        "%s.geneious",
                        defaultNucleotideSequenceAnnotatedPluginDocument.getName()
                    ));
                    Files.deleteIfExists(outputPath);
                    PluginUtilities.exportDocumentsInGeneiousFormat(
                        outputPath.toFile(),
                        true,
                        defaultNucleotideSequenceAnnotatedPluginDocument
                    );
                    documentsToExport = new AnnotatedPluginDocument[] { defaultNucleotideSequenceAnnotatedPluginDocument };

                    outputPath = exportDirectoryPathPerRunPerBioinformatician.resolve(String.format(
                        "%s.fasta",
                        defaultNucleotideSequenceAnnotatedPluginDocument.getName()
                    ));
                    Files.deleteIfExists(outputPath);
                    compositeProgressListener.beginNextSubtask();
                    fastaFileExporter.export(
                        outputPath.toFile(),
                        documentsToExport,
                        compositeProgressListener,
                        fastaFileExporter.getOptions(documentsToExport)
                    );
                }

                outputPath = exportDirectoryPathPerRunPerBioinformatician.resolve(String.format(
                    "%s.geneious",
                    FINAL_ASSEMBLIES_FOLDER_NAME
                ));
                Files.deleteIfExists(outputPath);
                PluginUtilities.exportDocumentsInGeneiousFormat(
                    outputPath.toFile(),
                    true,
                    finalAssembliesFolderDatabaseService.retrieve("").toArray(AnnotatedPluginDocument[]::new)
                );
            } catch (IOException ioException) {
                throw new DocumentOperationException(ioException);
            }
        }
        String errorMessages = Stream.of(NonFatalError.values()).filter(
            nonFatalError -> !nonFatalError.names.isEmpty()
        ).map(
            nonFatalError -> String.format(
                "The following genomes had %s:\n%s",
                nonFatalError.errorDescription,
                String.join(
                    "\n",
                    nonFatalError.names
                )
            )
        ).collect(
            Collectors.joining("\n\n")
        );
        long systemStopTime = System.currentTimeMillis();
        long elapsedTime = systemStopTime - systemStartTime;
        long elapsedTimeInSeconds = elapsedTime / 1000;
        System.out.printf(
            "Runtime: %d minutes, %d seconds%n",
            elapsedTimeInSeconds / 60,
            elapsedTimeInSeconds % 60
        );
        if (errorMessages.length() > 0) {
            Dialogs.showMessageDialog(
                errorMessages,
                "Non-fatal errors caused by this run's input files"
            );
        }
        Stream.of(NonFatalError.values()).forEachOrdered(
            nonFatalError -> nonFatalError.names.clear()
        );
        return returnValue;
    }
}
