import re

# define dataclass for word count result
from dataclasses import dataclass

import apache_beam as beam


@dataclass(frozen=True)
class WordCount:
    word: str
    count: int


# domain service / functions
def extract_words(line: str) -> list[str]:
    """Parse each line of input text into words."""
    return re.findall(r"[\w\']+", line, re.UNICODE)


def format_word_count_result(word_count: WordCount) -> str:
    """Format WordCount dataclass into string for output."""
    return f"{word_count.word}: {word_count.count}"


# beam Pardo wrappers
class WordExtractorDoFn(beam.DoFn):
    """apache beam DoFn wrapper for extract_words function."""

    def process(self, element: str) -> list[str]:
        return extract_words(element)
