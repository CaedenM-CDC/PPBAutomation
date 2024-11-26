package com.biomatters.ppbAutomation;

import com.biomatters.geneious.publicapi.databaseservice.WritableDatabaseService;
import com.biomatters.geneious.publicapi.documents.AnnotatedPluginDocument;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

class WritableDatabaseServiceDatum {
    final WritableDatabaseService writableDatabaseService;
    final HashMap<String, WritableDatabaseServiceDatum> children;
    final List<File> rawFastqFiles;
    final List<AnnotatedPluginDocument> importedFastqFiles;
    final LinkedHashSet<String> referenceGenomeAccessionNumbers;
    final List<AnnotatedPluginDocument> referenceGenomes;
    final String sampleName;

    public WritableDatabaseServiceDatum(
        WritableDatabaseService writableDatabaseService,
        String sampleName
    ) {
        this.writableDatabaseService = writableDatabaseService;
        this.children = new HashMap<>();
        this.rawFastqFiles = new LinkedList<>();
        this.importedFastqFiles = new LinkedList<>();
        this.referenceGenomeAccessionNumbers = new LinkedHashSet<>();
        this.referenceGenomes = new LinkedList<>();
        this.sampleName = sampleName;
    }

    public WritableDatabaseServiceDatum(
        WritableDatabaseService writableDatabaseService
    ) {
        this(
            writableDatabaseService,
            null
        );
    }

    @Override
    public int hashCode() {
        return this.writableDatabaseService.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof WritableDatabaseServiceDatum)) {
            return false;
        }
        WritableDatabaseServiceDatum other = (WritableDatabaseServiceDatum)object;
        return this.writableDatabaseService.equals(other.writableDatabaseService);
    }
}
