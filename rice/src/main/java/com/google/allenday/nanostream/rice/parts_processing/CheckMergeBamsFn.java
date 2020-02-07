package com.google.allenday.nanostream.rice.parts_processing;

import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.io.GCSService;
import com.google.allenday.genomics.core.model.*;
import com.google.allenday.genomics.core.parts_processing.PrepareMergeNotProcessedFn;
import com.google.cloud.storage.BlobId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class CheckMergeBamsFn extends DoFn<KV<SraSampleId, Iterable<SampleMetaData>>, KV<SraSampleIdReferencePair, String>> {

    private Logger LOG = LoggerFactory.getLogger(PrepareMergeNotProcessedFn.class);

    private GCSService gcsService;

    private FileUtils fileUtils;
    private List<String> references;
    private String stagedBucket;

    private String mergedFilePattern;
    private String sortedFilePattern;

    public CheckMergeBamsFn(FileUtils fileUtils, List<String> references,
                            String stagedBucket, String mergedFilePattern, String sortedFilePattern) {
        this.fileUtils = fileUtils;
        this.references = references;
        this.stagedBucket = stagedBucket;
        this.sortedFilePattern = sortedFilePattern;
        this.mergedFilePattern = mergedFilePattern;
    }

    @DoFn.Setup
    public void setUp() {
        gcsService = GCSService.initialize(fileUtils);
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        KV<SraSampleId, Iterable<SampleMetaData>> input = c.element();

        @Nonnull
        SraSampleId sraSampleId = input.getKey();

        Iterable<SampleMetaData> geneSampleMetaDataIterable = input.getValue();

        for (String ref : references) {
            BlobId blobIdMerge = BlobId.of(stagedBucket, String.format(mergedFilePattern, sraSampleId.getValue(), ref));
            boolean mergeExists = gcsService.isExists(blobIdMerge);
            if (mergeExists) {
                boolean redyToMerge = true;
                List<FileWrapper> fileWrappers = new ArrayList<>();
                long sortSize = 0;
                for (SampleMetaData geneSampleMetaData : geneSampleMetaDataIterable) {
                    BlobId blobIdSort = BlobId.of(stagedBucket, String.format(sortedFilePattern, geneSampleMetaData.getRunId(), ref));
                    boolean sortExists = gcsService.isExists(blobIdSort);
                    String uriFromBlob = gcsService.getUriFromBlob(blobIdSort);
                    fileWrappers.add(FileWrapper.fromBlobUri(uriFromBlob,
                            new FileUtils().getFilenameFromPath(uriFromBlob)));
                    if (!sortExists) {
                        redyToMerge = false;
                        LOG.info(String.format("Not ready to merge: %s", sraSampleId.getValue()));
                    } else {
                        sortSize = sortSize + gcsService.getBlobSize(blobIdSort);
                    }
                }
                if (redyToMerge) {
                    long mergeSize = gcsService.getBlobSize(blobIdMerge);

                    long delta = mergeSize - sortSize;

                    if (delta > 1024 * 1024 * 50) {
                        LOG.info(String.format("Attantion: %s %s", sraSampleId, String.valueOf(delta)));
                    }
                    c.output(KV.of(new SraSampleIdReferencePair(sraSampleId, ReferenceDatabase.onlyName(ref)), String.valueOf(delta / (1024 * 1024))));
                }
            }
        }
    }
}
