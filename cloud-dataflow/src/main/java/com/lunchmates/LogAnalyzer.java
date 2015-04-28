/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.lunchmates;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableComparator;
import com.google.cloud.dataflow.sdk.transforms.Top;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * An example that counts words in Shakespeare. For a detailed walkthrough of this
 * example see:
 * https://cloud.google.com/dataflow/java-sdk/wordcount-example
 * <p/>
 * <p> Concepts: Reading/writing text files; counting a PCollection; user-defined PTransforms
 * <p/>
 * <p> To execute this pipeline locally, specify general pipeline configuration:
 * --project=<PROJECT ID>
 * and a local output file or output prefix on GCS:
 * --output=[<LOCAL FILE> | gs://<OUTPUT PREFIX>]
 * <p/>
 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * --project=<PROJECT ID>
 * --stagingLocation=gs://<STAGING DIRECTORY>
 * --runner=BlockingDataflowPipelineRunner
 * and an output prefix on GCS:
 * --output=gs://<OUTPUT PREFIX>
 * <p/>
 * <p> The input file defaults to gs://dataflow-samples/shakespeare/kinglear.txt and can be
 * overridden with --input.
 */
public class LogAnalyzer {

    private static final String RESPONSE_CODE_PATTERN = "HTTP.[0-9]\\.?[0-9]\" ([2345][0-9][0-9])";

    /**
     * A DoFn that tokenizes lines of text into individual words.
     */
    static class GetResponseCodeFn extends DoFn<String, String> {

        private static final long serialVersionUID = 0;
        private static final Pattern pattern = Pattern.compile(RESPONSE_CODE_PATTERN);

        public GetResponseCodeFn() {

        }

        @Override
        public void processElement(ProcessContext context) {

            // Find matches to regex
            Matcher matcher = pattern.matcher(context.element());

            // Output each word encountered into the output PCollection.
            if (matcher.find()) {
                context.output(matcher.group(1));
            }
        }
    }

    /**
     * Computes the longest session ending in each month.
     */
    private static class TopCodes
            extends PTransform<PCollection<KV<String, Long>>, PCollection<List<KV<String, Long>>>> {

        private static final long serialVersionUID = 0;

        @Override
        public PCollection<List<KV<String, Long>>> apply(PCollection<KV<String, Long>> responseCodes) {

            return responseCodes.apply(Top.of(5, new SerializableComparator<KV<String, Long>>() {
                private static final long serialVersionUID = 0;

                @Override
                public int compare(KV<String, Long> o1, KV<String, Long> o2) {
                    return Long.compare(o1.getValue(), o2.getValue());
                }
            }));
        }
    }

    /**
     * A DoFn that converts a response code result into a string that can be processed by another routine.
     */
    static class FormatResultsFn extends DoFn<List<KV<String, Long>>, String> {
        private static final long serialVersionUID = 0;

        @Override
        public void processElement(ProcessContext context) {
            for (KV<String, Long> item : context.element()) {
                context.output(item.getKey() + "|" + item.getValue());
            }
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     * <p/>
     * Although this pipeline fragment could be inlined, bundling it as a PTransform allows for easy
     * reuse, modular testing, and an improved monitoring experience.
     */
    public static class ExtractLogExperience extends PTransform<PCollection<String>, PCollection<String>> {
        private static final long serialVersionUID = 0;

        @Override
        public PCollection<String> apply(PCollection<String> lines) {

            // Filter line content to leave method alone
            PCollection<String> responseCodes = lines.apply(ParDo.of(new GetResponseCodeFn()));

            // Counts occurrences for each response code found
            PCollection<KV<String, Long>> responseCodeResults = responseCodes.apply(Count.<String>perElement());

            // Get the top three response codes
            PCollection<List<KV<String, Long>>> topThreeResponseCodes = responseCodeResults.apply(new TopCodes());

            // Format each word and count into a printable string.
            PCollection<String> results = topThreeResponseCodes.apply(ParDo.of(new FormatResultsFn()));

            return results;
        }
    }

    /**
     * Allowed set of options for this script
     */
    public static interface AllowedOptions extends PipelineOptions {

        @Description("Default path for logs file")
        @Default.String("gs://lunchmates_logs/access.log")
        String getInput();

        void setInput(String value);

        @Description("Path of the file to write to")
        @Default.InstanceFactory(OutputFactory.class)
        String getOutput();

        void setOutput(String value);

        /**
         * Stores the result under gs://${STAGING_LOCATION}/"results.txt" by default.
         */
        public static class OutputFactory implements DefaultValueFactory<String> {

            @Override
            public String create(PipelineOptions options) {

                DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
                if (dataflowOptions.getStagingLocation() != null) {
                    return GcsPath.fromUri(dataflowOptions.getStagingLocation()).resolve("results.txt").toString();
                } else {
                    throw new IllegalArgumentException("Must specify --output or --stagingLocation");
                }
            }
        }

        /**
         * By default (numShards == 0), the system will choose the shard count.
         * Most programs will not need this option.
         */
        @Description("Number of output shards (0 if the system should choose automatically)")
        int getNumShards();

        void setNumShards(int value);
    }

    public static void main(String[] args) {

        AllowedOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(AllowedOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
         .apply(new ExtractLogExperience())
         .apply(TextIO.Write.named("WriteCounts")
                            .to(options.getOutput()).withNumShards(options.getNumShards()));

        p.run();
    }
}

