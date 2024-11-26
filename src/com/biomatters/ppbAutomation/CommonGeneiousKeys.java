package com.biomatters.ppbAutomation;

import java.io.File;

public class CommonGeneiousKeys {
    private CommonGeneiousKeys() {
        // Do nothing.
    }
//    public static final String ;
    public static final String ILLUMINA_NETWORK_PREFIX_KEY = "illuminaNetworkPrefix";
    public static final String ILLUMINA_NETWORK_SUFFIX_KEY = "illuminaNetworkSuffix";
    public static final String DEFAULT_ILLUMINA_NETWORK_PREFIX = String.join(
        File.separator,
        String.format(
            "%s%sgroups.biotech.cdc.gov",
            File.separator,
            File.separator
        ),
        "groups",
        "OID",
        "NCIRD-OD",
        "OI",
        "ncbs",
        "share",
        "out",
        "PPLB",
        "PPLB-Demo"
    );
    public static final String DEFAULT_ILLUMINA_NETWORK_SUFFIX = "Alignment_Results";
    public static final String FULL_RUN_NAME_KEY = "fullRunName";
    public static final String FULL_RUN_NAME_LABEL = "Full run name";
    public static final String ILLUMINA_FASTQ_REGEX_KEY = "illuminaFastqRegex";
    public static final String ILLUMINA_JSON_REGEX_KEY = "illuminaJsonRegex";
    public static final String PYTHON_SCRIPTS_FOLDER_PATH_KEY = "pythonScriptsFolderPath";
    public static final String PYTHON_SCRIPTS_FOLDER_PATH_LABEL = "Python-scripts folder path";
    public static final String DEFAULT_ACCESSION_NUMBER_KEY = "defaultAccessionNumber";
    public static final String DEFAULT_ACCESSION_NUMBER_LABEL = "Default accession number";
    public static final String CALCULATE_CONTIGS_FLAG_KEY = "calculateContigsFlag";
    public static final String CALCULATE_CONTIGS_FLAG_LABEL = "Calculate contigs?";
    public static final String WET_LAB_EXCEL_FILE_PATH_KEY = "wetLabExcelFilePath";
    public static final String WET_LAB_EXCEL_FILE_PATH_LABEL = "Wet-lab Excel-file path";
    public static final int MINIMUM_NUMBER_OF_THREADS = 1;
    public static final int MAXIMUM_NUMBER_OF_THREADS = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_NUMBER_OF_THREADS = MAXIMUM_NUMBER_OF_THREADS;
    public static final String NUMBER_OF_THREADS_KEY = "numberOfThreads";
    public static final String NUMBER_OF_THREADS_LABEL = "Number of threads";
    public static final String FILE_SEPARATOR_FOR_REGEX = File.separator.equals("\\") ? "\\\\" : File.separator;
    public static final String DEFAULT_ILLUMINA_FASTQ_REGEX = String.format(
        "(?:^|%s)([^%s]+?)(?:_S\\d+)?(?:_L\\d+)?-trim\\.dedup\\.R[12]\\.fq$",
        FILE_SEPARATOR_FOR_REGEX,
        FILE_SEPARATOR_FOR_REGEX
    );
    public static final String DEFAULT_ILLUMINA_JSON_REGEX = String.format(
        "(?:^|%s)([^%s]+?)(?:_S\\d+)?(?:_L\\d+)?_Blast-NT\\.json$",
        FILE_SEPARATOR_FOR_REGEX,
        FILE_SEPARATOR_FOR_REGEX
    );
    public static final String FINAL_ASSEMBLIES_FOLDER_NAME = "Final Assemblies";
    public static final String ALIGNMENTS_FOLDER_NAME = "Alignments";
    public static final String REFERENCES_FOLDER_NAME = "References";
    public static final String DEFAULT_ACCESSION_NUMBER = "AY184220";
    public static final String GENEIOUS_ASSEMBLER_PLUGIN_NAME = "Geneious Assembler";
    public static final String FASTA_EXPORTER_PLUGIN_NAME = "FASTA Importer/exporter";
    public static final boolean DEFAULT_CALCULATE_CONTIGS_FLAG = true;
    public static final String INTERACT_WITH_FINAL_ANALYSIS_DOCUMENT_PYTHON_SCRIPT_NAME = "interact_with_final_analysis_document";
    public static final String CODE_FOR_GRADE_FIELD = "grade";
    public static final String CODE_FOR_QUERY_COVERAGE_FIELD = "queryCoverage";
}
