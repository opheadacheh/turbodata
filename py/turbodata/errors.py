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
