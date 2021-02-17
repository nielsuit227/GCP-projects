from __future__ import absolute_import
import argparse
import logging
import re
from past.builtins import unicode
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class WordExtractingDoFn(beam.DoFn):
  def process(self, element):
    return re.findall(r'[\w\']+', element, re.UNICODE)


# Subclass of beam.PTransform
class CountWords(beam.PTransform):
  def expand(self, pcoll):
    return (
      pcoll
      | 'ExctractWords' >>
      beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
      | beam.combiners.Count.PerElement())


# Subclass of Beam.DoFn
class FormatAsTextFn(beam.DoFn):
  def process(self, element):
    word, count = element
    yield '%s: %s' % (word, count)


# Subclass of PipelineOptions
class WordCountOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input', default='shakespear.txt')


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input', dest='input', default='shakespear.txt')
  parser.add_argument('--output', dest='output', default='output')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(known_args.input)

    counts = lines | CountWords()

    output = counts | 'Format' >> beam.ParDo(FormatAsTextFn())

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()