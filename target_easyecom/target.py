"""Easyecom target class."""
from target_hotglue.target import TargetHotglue

from target_easyecom.sinks import (
    BuyOrdersSink,
)

class TargetEasyecom(TargetHotglue):
    """Sample target for Easyecom."""

    name = "target-easyecom"

    MAX_PARALLELISM = 1

    SINK_TYPES = [
        BuyOrdersSink,
    ]

    def get_sink_class(self, stream_name: str):
        return next(
            (
                sink_class
                for sink_class in self.SINK_TYPES
                if sink_class.name.lower() == stream_name.lower()
            ),
            None,
        )

if __name__ == "__main__":
    TargetEasyecom.cli()
