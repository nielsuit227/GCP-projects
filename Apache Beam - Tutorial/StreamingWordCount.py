from __future__ import absolute_import

import argparse
import logging
import random
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms import window
from past.builtins import unicode


# Subclass of beam.PTransform
class CountWords(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "ExctractWords" >> beam.FlatMap(lambda x: re.findall(r"[A-Za-z\']+", x))
            | beam.combiners.Count.PerElement()
        )


class AddTimeStampFn(beam.DoFn):
    def __init__(self, min_timestamp, max_timestamp):
        self.min_timestamp = min_timestamp
        self.max_timestamp = max_timestamp

    def process(self, element):
        return window.TimestampedValue(
            element, random.randint(self.min_timestamp, self.max_timestamp)
        )


def run(argv=None, save_main_session=True):
    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", dest="input", default="shakespear.txt")
    parser.add_argument("--output", dest="output", default="output")
    known_args, pipeline_args = parser.parse_known_args(argv)
    minTime = 0
    maxTime = 10000
    windowSize = 10
    # Settings
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    # Pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read file
        lines = p | "Read" >> ReadFromText(known_args.input)
        # Add timestamps
        # input = lines | beam.Map(AddTimeStampFn(minTime, maxTime))
        # Window them
        windowedWords = lines | "WindowInto" >> beam.WindowInto(
            window.FixedWindows(windowSize)
        )
        # Counting!
        wordCounts = windowedWords | "Count" >> CountWords()
        print(wordCounts)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
