"""Easyecom target class."""

from singer_sdk.target_base import Target
from target_hotglue.target import TargetHotglue

from target_easyecom.sinks import (
    BuyOrdersSink,
)

class TargetEasyecom(Target, TargetHotglue):
    """Sample target for Easyecom."""

    name = "target-easyecom"

    MAX_PARALLELISM = 1

    SINK_TYPES = [
        BuyOrdersSink,
    ]

    def __init__(
        self,
        config,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)

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
