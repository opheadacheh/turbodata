"""Error types raised by the turbodata package.

Mirrors go/errors.go. Errors carry no extra fields; callers compare on
identity / `isinstance`.
"""
from __future__ import annotations


class TurbodataError(Exception):
    """Base class for all turbodata errors."""


class TopicAlreadyOpenError(TurbodataError):
    pass


class NamesMetadatasMismatchError(TurbodataError):
    pass


class NoTopicsToOpenError(TurbodataError):
    pass


class TopicNotOpenedError(TurbodataError):
    pass


class TopicNotRegisteredError(TurbodataError):
    pass


class TimestampDecreasesError(TurbodataError):
    pass


class TopicAlreadyClosedError(TurbodataError):
    pass


class TopicNotClosedError(TurbodataError):
    pass


class InvalidMagicError(TurbodataError):
    pass


class FileTooSmallError(TurbodataError):
    pass


class SampleValidationError(TurbodataError):
    """Raised by Reader.sample on precondition violations. Contains a list of
    individual violation messages (one per offending query/timestamp).
    """

    def __init__(self, violations):
        self.violations = list(violations)
        super().__init__("; ".join(self.violations))


class TopicRemapCollisionError(TurbodataError):
    """Raised when a configured topic remap collapses two in-file topics onto
    the same exposed name (validated lazily against the summary on first use).
    """

    def __init__(self, exposed: str, first: str, second: str):
        self.exposed = exposed
        self.first = first
        self.second = second
        super().__init__(
            f"turbodata: topic remap produces duplicate exposed name {exposed!r} "
            f"(from in-file topics {first!r} and {second!r})"
        )


class VideoSourcesOverlapError(TurbodataError):
    """Raised by MultiReader.sample/read_messages under video_decodable when a
    queried video topic is provided by more than one reader whose time ranges
    overlap. Decodable multi-file video sampling requires each video topic's
    source files to be time-disjoint; merging overlapping decodable sources
    would interleave frames from different GOP chains into an undecodable
    stream. Mirrors go ErrVideoSourcesOverlap.
    """

    def __init__(
        self,
        topic: str,
        first: tuple = None,
        second: tuple = None,
    ):
        self.topic = topic
        self.first = first
        self.second = second
        super().__init__(
            f"turbodata: video topic {topic!r} sources {first} and {second} "
            f"overlap in time"
        )


# ---- Video-topic constraints. See Writer.open_topics(video=True) and
# Writer.write_video_message. -------------------------------------------------
class VideoGroupMustBeSingleTopicError(TurbodataError):
    pass


class VideoTopicCannotBeCompressedError(TurbodataError):
    pass


class FirstVideoMessageMustBeKeyFrameError(TurbodataError):
    pass


class WriteMessageOnVideoTopicError(TurbodataError):
    pass


class WriteVideoMessageOnNonVideoTopicError(TurbodataError):
    pass
