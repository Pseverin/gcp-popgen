package com.google.allenday.popgen.anomaly;

import com.google.allenday.genomics.core.gcp.GcsService;
import com.google.allenday.genomics.core.model.SampleRunMetaData;
import com.google.allenday.genomics.core.preparing.runfile.GcsFastqInputResource;
import com.google.allenday.genomics.core.processing.align.Instrument;
import com.google.allenday.genomics.core.utils.FileUtils;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RecognizeGcsInputSourceWithAnomalyFn extends DoFn<KV<SampleRunMetaData, List<GcsFastqInputResource>>,
        KV<SampleRunMetaData, List<GcsFastqInputResource>>> {

    private Logger LOG = LoggerFactory.getLogger(RecognizeGcsInputSourceWithAnomalyFn.class);

    private String srcBucket;
    private GcsService gcsService;
    private FileUtils fileUtils;
    private final boolean tryToFindWithSuffixMistake;

    public RecognizeGcsInputSourceWithAnomalyFn(String stagedBucket, FileUtils fileUtils) {
        this(stagedBucket, fileUtils, false);
    }

    public RecognizeGcsInputSourceWithAnomalyFn(String stagedBucket, FileUtils fileUtils, boolean tryToFindWithSuffixMistake) {
        this.srcBucket = stagedBucket;
        this.fileUtils = fileUtils;
        this.tryToFindWithSuffixMistake = tryToFindWithSuffixMistake;
    }

    @Setup
    public void setUp() {
        gcsService = GcsService.initialize(fileUtils);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        KV<SampleRunMetaData, List<GcsFastqInputResource>> input = c.element();
        LOG.info(String.format("RecognizeGcsInputSourceWithAnomalyFn %s", input.toString()));

        SampleRunMetaData sampleRunMetaData = input.getKey();
        List<GcsFastqInputResource> originalInputSources = input.getValue();

        if (sampleRunMetaData == null || originalInputSources == null) {
            LOG.info("Data error {}, {}", sampleRunMetaData, originalInputSources);
            return;
        }
        if (Arrays.stream(Instrument.values()).map(Enum::name).noneMatch(instrumentName -> instrumentName.equals(sampleRunMetaData.getPlatform()))) {
            sampleRunMetaData.setComment(String.format("Unknown INSTRUMENT: %s", sampleRunMetaData.getPlatform()));
            c.output(KV.of(sampleRunMetaData, Collections.emptyList()));
            return;
        }

        try {
            List<GcsFastqInputResource> checkedInputSources = new ArrayList<>();
            if (originalInputSources.size() > 0) {
                originalInputSources.stream()
                        .findFirst()
                        .map(GcsFastqInputResource::getBlobId)
                        .map(BlobId::getName)
                        .map(blobName -> fileUtils.getDirFromPath(blobName))
                        .ifPresent(dirPrefix -> {
                            String filesNameBaseToSearch = dirPrefix + sampleRunMetaData.getRunId();

                            List<Blob> blobs = StreamSupport
                                    .stream(gcsService.getBlobsWithPrefix(srcBucket, filesNameBaseToSearch)
                                            .spliterator(), false).collect(Collectors.toList());

                            if (blobs.size() != originalInputSources.size()) {
                                if (tryToFindWithSuffixMistake && blobs.size() == 1 && originalInputSources.size() == 2) {
                                    List<Blob> blobsToSearch = StreamSupport.stream(gcsService.getBlobsWithPrefix(srcBucket, dirPrefix)
                                            .spliterator(), false).collect(Collectors.toList());

                                    originalInputSources
                                            .forEach(inputResource -> {
                                                if (inputResource.exists(gcsService)) {
                                                    checkedInputSources.add(inputResource);
                                                } else {
                                                    searchWithSuffixMistake(sampleRunMetaData, inputResource, blobsToSearch, checkedInputSources);
                                                }
                                            });
                                } else {
                                    sampleRunMetaData.setComment("Wrong files number for runId");
                                    logAnomaly(blobs, sampleRunMetaData);
                                }
                            } else {
                                List<GcsFastqInputResource> gcsFastqInputResources = blobs.stream().map(blob -> {
                                    String uriFromBlob = gcsService.getUriFromBlob(blob.getBlobId());
                                    String name = fileUtils.getFilenameFromPath(uriFromBlob);

                                    GcsFastqInputResource gcsFastqInputResource = new GcsFastqInputResource(blob.getBlobId());

                                    String[] parts = name.split("_");
                                    int suffix = Integer.parseInt(parts[parts.length - 1].split("\\.")[0]);
                                    return Pair.with(gcsFastqInputResource, suffix);
                                }).sorted(Comparator.comparing(Pair::getValue1)).map(Pair::getValue0).collect(Collectors.toList());
                                checkedInputSources.addAll(gcsFastqInputResources);
                            }
                            if (originalInputSources.size() == checkedInputSources.size()) {
                                c.output(KV.of(sampleRunMetaData, checkedInputSources));
                            } else {
                                sampleRunMetaData.setComment(sampleRunMetaData.getComment() + String.format(" (%d/%d)", checkedInputSources.size(), originalInputSources.size()));
                                c.output(KV.of(sampleRunMetaData, Collections.emptyList()));
                            }
                        });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void searchWithSuffixMistake(SampleRunMetaData sampleRunMetaData, GcsFastqInputResource inputResource,
                                         List<Blob> blobs, List<GcsFastqInputResource> checkedInputResources) {
        String indexToSearch = inputResource.getName().split("\\.")[0].split("_")[1];
        if (Integer.parseInt(indexToSearch) != 2) {
            sampleRunMetaData.setComment("Already tried with _1 paired index");
            return;
        }

        LOG.info(String.format("Blob %s doesn't exist. Trying to find blob with other SRA for %s", inputResource.getBlobId().toString(),
                sampleRunMetaData.toString()));
        Optional<Blob> blobOpt = blobs.stream().filter(blob -> {
            boolean containsIndex = blob.getName().contains(String.format("_%s", indexToSearch));
            if (blobs.size() == 2) {
                return containsIndex;
            } else {
                boolean containsIndexFirst = blob.getName().contains(String.format("_%s", 1));
                int runInt = Integer.parseInt(blob.getName()
                        .substring(blob.getName().lastIndexOf('/') + 1).split("_")[0]
                        .substring(3));
                int serchedRunInt = Integer.parseInt(sampleRunMetaData.getRunId().substring(3));
                return Math.abs(serchedRunInt - runInt) == 1 && !containsIndexFirst && containsIndex;
            }
        }).findFirst();
        if (blobOpt.isPresent()) {
            LOG.info(String.format("Found: %s", blobOpt.get().getName()));
            checkedInputResources.add(new GcsFastqInputResource(blobOpt.get().getBlobId()));
        } else {
            logAnomaly(blobs, sampleRunMetaData);
            sampleRunMetaData.setComment("File not found");
        }
    }

    private void logAnomaly(List<Blob> blobs, SampleRunMetaData geneSampleMetaData) {
        LOG.info(String.format("Anomaly: %s, %s, %s, blobs %s",
                Optional.ofNullable(geneSampleMetaData.getAdditionalMetaData()).map(SampleRunMetaData.AdditionalMetaData::getCenterName).orElse(""),
                geneSampleMetaData.getSraSample(),
                geneSampleMetaData.getRunId(),
                blobs.stream().map(BlobInfo::getName).collect(Collectors.joining(", "))));
    }
}
