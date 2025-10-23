from typing import Iterable, Tuple

import apache_beam as beam
from apache_beam.pvalue import PCollection
from apache_beam.runners.runner import PipelineResult
from domain.wordcount_domain import (
    WordCount,
    WordExtractorDoFn,
    format_word_count_result,
)


def build_word_count_pipeline(
    pipeline: beam.Pipeline, input_path: str, output_path: str
) -> None:
    """
    Word Count Beam Pipeline
    Infrastructure as Code
    """

    # 1. Read input text file
    lines = pipeline | "ReadLines" >> beam.io.ReadFromText(input_path)

    # 2. Count words usig domain service
    counts: PCollection[Tuple[str, int]] = (
        lines
        # parse words using domain service
        | "SplitWords" >> (beam.ParDo(WordExtractorDoFn()).with_output_types(str))
        # pair each word with 1
        | "PairWithOne" >> beam.Map(lambda word: (word, 1))
        # Group By Key and Sum counts
        | "GroupAndSum" >> beam.CombinePerKey(sum)
    )

    # 2.1 prepare sort key
    # countにマイナスをつけて、昇順ソートで降順を実現する。
    sort_ready: PCollection[Tuple[int, str]] = counts | "InvertCount" >> beam.Map(
        lambda item: (-item[1], item[0])
    )

    all_in_one_group: PCollection[Tuple[None, Iterable[Tuple[int, str]]]] = (
        sort_ready
        | "AddDummyKey" >> beam.Map(lambda item: (None, item))
        | "GroupAllData" >> beam.GroupByKey()
    )

    sorted_count: PCollection[Tuple[int, str]] = (
        all_in_one_group
        | "SortAndFlatten" >> beam.FlatMap(lambda item: sorted(item[1]))
    )

    final_sorted_counts: PCollection[Tuple[str, int]] = (
        sorted_count | "RestoreFormat" >> beam.Map(lambda item: (item[1], -item[0]))
    )

    # 2.5 Convert to WordCount dataclass
    typed_counts: PCollection[WordCount] = (
        final_sorted_counts | "ToWordCount" >> beam.MapTuple(WordCount)
    )

    # 3. Format the result using domain service
    output: PCollection[str] = typed_counts | "FormatResult" >> beam.Map(
        format_word_count_result
    )

    # 4. Write output text file
    output | "WriteResults" >> beam.io.WriteToText(output_path)


def run_pipeline(
    pipeline_options: beam.options.pipeline_options.PipelineOptions,
    input_path: str,
    ouptut_path: str,
) -> PipelineResult:
    """
    Run the Word Count Beam Pipeline
    """
    pipeline = beam.Pipeline(options=pipeline_options)
    build_word_count_pipeline(pipeline, input_path, ouptut_path)

    result: PipelineResult = pipeline.run()
    result.wait_until_finish()
    return result
