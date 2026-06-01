"""turbodata Python SDK.

Read/write port of the Go reference implementation in ../go. The on-disk
format is identical: files written by one SDK can be read by another.

Quick start:

    from turbodata import Reader, Writer, FileReadSource

    # Read
    with FileReadSource("file.td") as src:
        reader = Reader(src)
        for msg in reader.read_messages():
            print(msg.timestamp, msg.topic_name, msg.data)

    # Write
    with open("out.td", "wb") as f, Writer(f) as w:
        w.open_topics(["/imu"], [{"hz": 100}])
        w.write_message("/imu", b"hello", timestamp=1)
        w.close_topic()
"""
from ._codec import (
    Footer,
    IndexChunk,
    IndexChunkInfo,
    MessageIndex,
    Summary,
    TopicIndex,
    TopicMetadata,
    TopicsInfo,
)
from ._iter import Order
from .chunk import ChunkConfig, ChunkThresholdMode
from .errors import (
    FileTooSmallError,
    FirstVideoMessageMustBeKeyFrameError,
    InvalidMagicError,
    NamesMetadatasMismatchError,
    NoTopicsToOpenError,
    SampleValidationError,
    TimestampDecreasesError,
    TopicAlreadyClosedError,
    TopicAlreadyOpenError,
    TopicNotClosedError,
    TopicNotOpenedError,
    TopicNotRegisteredError,
    TurbodataError,
    VideoGroupMustBeSingleTopicError,
    VideoTopicCannotBeCompressedError,
    WriteMessageOnVideoTopicError,
    WriteVideoMessageOnNonVideoTopicError,
)
from .reader import (
    DEFAULT_SAMPLE_STRATEGY,
    Frame,
    Message,
    Reader,
    SampleQuery,
    SampleResult,
    lin_space_timestamps,
)
from .source import BytesReadSource, FileReadSource, ReadSource
from .strategy import (
    ReadStrategy,
    strategy_for_blended,
    strategy_for_latency,
    strategy_for_money,
)
from .writer import Writer

__all__ = [
    # Reader / writer
    "Reader",
    "Writer",
    "Message",
    # Read sources
    "ReadSource",
    "FileReadSource",
    "BytesReadSource",
    # Configuration
    "ChunkConfig",
    "ChunkThresholdMode",
    "Order",
    "ReadStrategy",
    "strategy_for_latency",
    "strategy_for_money",
    "strategy_for_blended",
    # Sampling
    "SampleQuery",
    "SampleResult",
    "Frame",
    "DEFAULT_SAMPLE_STRATEGY",
    "lin_space_timestamps",
    # Format types (also returned by Reader.summary())
    "Footer",
    "Summary",
    "TopicsInfo",
    "TopicMetadata",
    "IndexChunkInfo",
    "IndexChunk",
    "TopicIndex",
    "MessageIndex",
    # Errors
    "TurbodataError",
    "TopicAlreadyOpenError",
    "TopicAlreadyClosedError",
    "TopicNotOpenedError",
    "TopicNotClosedError",
    "TopicNotRegisteredError",
    "NamesMetadatasMismatchError",
    "NoTopicsToOpenError",
    "TimestampDecreasesError",
    "InvalidMagicError",
    "FileTooSmallError",
    "SampleValidationError",
    "VideoGroupMustBeSingleTopicError",
    "VideoTopicCannotBeCompressedError",
    "FirstVideoMessageMustBeKeyFrameError",
    "WriteMessageOnVideoTopicError",
    "WriteVideoMessageOnNonVideoTopicError",
]

__version__ = "0.1.0"
