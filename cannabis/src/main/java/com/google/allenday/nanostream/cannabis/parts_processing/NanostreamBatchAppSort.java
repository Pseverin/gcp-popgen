package com.google.allenday.nanostream.cannabis.parts_processing;

import com.google.allenday.genomics.core.batch.BatchProcessingPipelineOptions;
import com.google.allenday.genomics.core.csv.ParseSourceCsvTransform;
import com.google.allenday.genomics.core.io.FileUtils;
import com.google.allenday.genomics.core.parts_processing.PrepareSortNotProcessedFn;
import com.google.allenday.genomics.core.parts_processing.StagingPathsBulder;
import com.google.allenday.genomics.core.pipeline.GenomicsOptions;
import com.google.allenday.genomics.core.pipeline.PipelineSetupUtils;
import com.google.allenday.genomics.core.processing.sam.SortFn;
import com.google.allenday.genomics.core.utils.NameProvider;
import com.google.allenday.nanostream.cannabis.NanostreamCannabisModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.List;

public class NanostreamBatchAppSort {

    private final static String JOB_NAME_PREFIX = "nanostream-cannabis-sort-";

    public static void main(String[] args) {

        BatchProcessingPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(BatchProcessingPipelineOptions.class);
        PipelineSetupUtils.prepareForInlineAlignment(pipelineOptions);

        Injector injector = Guice.createInjector(new NanostreamCannabisModule.Builder()
                .setFromOptions(pipelineOptions)
                .build());

        NameProvider nameProvider = injector.getInstance(NameProvider.class);
        pipelineOptions.setJobName(nameProvider.buildJobName(JOB_NAME_PREFIX, pipelineOptions.getSraSamplesToFilter()));

        Pipeline pipeline = Pipeline.create(pipelineOptions);
        final String stagedBucket = injector.getInstance(Key.get(String.class, Names.named("resultBucket")));
        final String outputDir = injector.getInstance(Key.get(String.class, Names.named("outputDir")));
        StagingPathsBulder stagingPathsBuilder = StagingPathsBulder.init(outputDir + "staged");

        GenomicsOptions genomicsOptions = injector.getInstance(GenomicsOptions.class);
        List<String> geneReferences = genomicsOptions.getGeneReferences();

        FileUtils fileUtils = injector.getInstance(FileUtils.class);

        pipeline
                .apply("Parse data", injector.getInstance(ParseSourceCsvTransform.class))
                .apply(ParDo.of(new PrepareSortNotProcessedFn(fileUtils, geneReferences, stagedBucket, stagingPathsBuilder)))
                .apply("Sort", ParDo.of(injector.getInstance(SortFn.class)))
        ;

        PipelineResult run = pipeline.run();
        if (pipelineOptions.getRunner().getName().equals(DirectRunner.class.getName())) {
            run.waitUntilFinish();
        }
    }
}
