"""
producer/kafka_producer.py

Production-ready ingestion entry point for the Real-Time Flight Analytics
Platform.

Flow:

    OpenSky API
        ↓
    OpenSkyClient
        ↓
    Add ingestion metadata
        ↓
    Confluent Kafka Producer
        ↓
    Azure Event Hubs Kafka endpoint
        ↓
    flight_raw

Important:
    Azure Event Hubs supports the Kafka protocol. This producer uses
    confluent-kafka with SASL_SSL + PLAIN authentication.

Environment variables:

    OPENSKY_CLIENT_ID
    OPENSKY_CLIENT_SECRET
    EVENTHUB_CONNECTION_STRING

Run:

    python -m producer.kafka_producer
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from confluent_kafka import KafkaException, Producer

from config.config_loader import load_config
from producer.opensky_client import OpenSkyClient


logger = logging.getLogger(__name__)


# ============================================================
# Global shutdown state
# ============================================================

_shutdown_requested = False


# ============================================================
# Logging
# ============================================================

def _configure_logging(cfg: dict) -> None:
    """
    Configure application-wide logging from config.yaml.
    """

    logging_cfg = cfg.get("logging", {})

    level_name = logging_cfg.get("level", "INFO").upper()

    level = getattr(
        logging,
        level_name,
        logging.INFO,
    )

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# ============================================================
# Graceful shutdown
# ============================================================

def _handle_shutdown_signal(signum: int, frame: Any) -> None:
    """
    Handle SIGINT/SIGTERM so the producer can stop cleanly.
    """

    global _shutdown_requested

    if not _shutdown_requested:
        logger.info(
            "Shutdown signal received (signal=%s). "
            "Finishing current work and shutting down...",
            signum,
        )

    _shutdown_requested = True


def _register_signal_handlers() -> None:
    """
    Register graceful shutdown handlers.
    """

    signal.signal(
        signal.SIGINT,
        _handle_shutdown_signal,
    )

    signal.signal(
        signal.SIGTERM,
        _handle_shutdown_signal,
    )


# ============================================================
# Environment validation
# ============================================================

def _get_required_environment_variable(name: str) -> str:
    """
    Read a required environment variable.

    Raises:
        RuntimeError: if the variable is missing or empty.
    """

    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable '{name}' is not set."
        )

    return value


# ============================================================
# Kafka producer configuration
# ============================================================

def build_kafka_producer(cfg: dict) -> Producer:
    """
    Build a Confluent Kafka Producer configured for Azure Event Hubs.

    Azure Event Hubs Kafka authentication:

        security.protocol = SASL_SSL
        sasl.mechanisms   = PLAIN
        sasl.username     = $ConnectionString
        sasl.password     = EVENTHUB_CONNECTION_STRING

    The Event Hubs connection string is intentionally NOT stored
    in config.yaml or source code.
    """

    kafka_cfg = cfg["kafka"]
    producer_cfg = kafka_cfg["producer"]

    # --------------------------------------------------------
    # Load Event Hubs connection string
    # --------------------------------------------------------

    connection_string = _get_required_environment_variable(
        "EVENTHUB_CONNECTION_STRING"
    )

    # --------------------------------------------------------
    # Validate bootstrap server
    # --------------------------------------------------------

    bootstrap_servers = kafka_cfg["bootstrap_servers"]

    if not bootstrap_servers:
        raise RuntimeError(
            "Kafka bootstrap_servers is missing from config.yaml."
        )

    # --------------------------------------------------------
    # Build Confluent Kafka configuration
    # --------------------------------------------------------

    producer_args = {
        "bootstrap.servers": bootstrap_servers,

        # Azure Event Hubs Kafka authentication
        "security.protocol": kafka_cfg.get(
            "security",
            {},
        ).get(
            "protocol",
            "SASL_SSL",
        ),

        "sasl.mechanisms": kafka_cfg.get(
            "security",
            {},
        ).get(
            "mechanism",
            "PLAIN",
        ),

        "sasl.username": "$ConnectionString",
        "sasl.password": connection_string,

        # Reliability
        "acks": producer_cfg.get(
            "acks",
            "all",
        ),

        "retries": producer_cfg.get(
            "retries",
            5,
        ),

        "linger.ms": producer_cfg.get(
            "linger_ms",
            100,
        ),

        # Give Event Hubs enough time to respond.
        "socket.timeout.ms": 30000,

        # Maximum time a message can remain queued before
        # the delivery callback reports a failure.
        "message.timeout.ms": 120000,

        # Keep connections alive.
        "connections.max.idle.ms": 240000,

        # Reduce noisy metadata behavior.
        "metadata.max.age.ms": 180000,

        # Ensure delivery callbacks are processed.
        "enable.idempotence": True,
    }

    # --------------------------------------------------------
    # Optional compression
    # --------------------------------------------------------

    compression_type = producer_cfg.get(
        "compression_type"
    )

    if compression_type:
        producer_args["compression.type"] = compression_type

    # --------------------------------------------------------
    # Create producer
    # --------------------------------------------------------

    logger.info(
        "Creating Azure Event Hubs Kafka producer: %s",
        bootstrap_servers,
    )

    producer = Producer(producer_args)

    logger.info(
        "Confluent Kafka producer created successfully."
    )

    return producer


# ============================================================
# Delivery callback
# ============================================================

def _delivery_report(err: Optional[KafkaException], msg: Any) -> None:
    """
    Delivery callback invoked by confluent-kafka.

    This is where we determine whether a message was actually
    delivered to Azure Event Hubs.
    """

    if err is not None:

        logger.error(
            "Message delivery failed: topic=%s key=%s error=%s",
            msg.topic(),
            msg.key(),
            err,
        )

        return

    logger.debug(
        "Message delivered successfully: "
        "topic=%s partition=%s offset=%s key=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
        msg.key(),
    )


# ============================================================
# Ingestion metadata
# ============================================================

def _enrich_with_ingestion_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Attach producer-side ingestion metadata.

    event time:
        time_position / last_contact from OpenSky

    ingestion time:
        when this pipeline received the record

    Downstream Spark processing can use these timestamps for
    event-time processing, freshness checks, and watermarking.
    """

    enriched = dict(record)

    enriched["ingestion_timestamp"] = (
        datetime.now(timezone.utc).isoformat()
    )

    return enriched


# ============================================================
# Publish one record
# ============================================================

def _publish_record(
    producer: Producer,
    topic: str,
    record: Dict[str, Any],
) -> bool:
    """
    Publish a single aircraft record.

    The aircraft ICAO24 identifier is used as the Kafka key so
    that updates for the same aircraft consistently map to the
    same Kafka partition.
    """

    icao24 = record.get("icao24")

    if not icao24:
        logger.warning(
            "Skipping aircraft record without icao24."
        )

        return False

    enriched = _enrich_with_ingestion_metadata(
        record
    )

    try:

        producer.produce(
            topic=topic,
            key=str(icao24),
            value=json.dumps(
                enriched,
                separators=(",", ":"),
            ),
            callback=_delivery_report,
        )

        return True

    except BufferError:

        logger.warning(
            "Kafka producer queue is full. "
            "Polling producer before retrying..."
        )

        producer.poll(0)

        return False

    except KafkaException as exc:

        logger.error(
            "Kafka exception while publishing "
            "icao24=%s: %s",
            icao24,
            exc,
        )

        return False


# ============================================================
# Flush pending messages
# ============================================================

def _flush_producer(
    producer: Producer,
    timeout_seconds: float = 30.0,
) -> bool:
    """
    Flush pending Kafka messages.

    Returns:
        True if all queued messages were delivered before timeout.
        False if messages remained after timeout.
    """

    remaining = producer.flush(
        timeout_seconds
    )

    if remaining > 0:

        logger.error(
            "%d message(s) remained undelivered "
            "after %.1f seconds.",
            remaining,
            timeout_seconds,
        )

        return False

    return True


# ============================================================
# Main producer loop
# ============================================================

def run_producer_loop() -> None:
    """
    Main long-running ingestion loop.

    Flow:

        OpenSky API
            ↓
        Fetch aircraft states
            ↓
        Validate icao24
            ↓
        Add ingestion timestamp
            ↓
        Serialize JSON
            ↓
        Confluent Kafka Producer
            ↓
        Azure Event Hubs
            ↓
        flight_raw
    """

    global _shutdown_requested

    # --------------------------------------------------------
    # Load configuration
    # --------------------------------------------------------

    cfg = load_config()

    _configure_logging(cfg)

    _register_signal_handlers()

    # --------------------------------------------------------
    # Read configuration
    # --------------------------------------------------------

    opensky_cfg = cfg["opensky"]
    kafka_cfg = cfg["kafka"]

    poll_interval = opensky_cfg[
        "poll_interval_seconds"
    ]

    topic = kafka_cfg[
        "topic_raw"
    ]

    # --------------------------------------------------------
    # Create OpenSky client
    # --------------------------------------------------------

    logger.info(
        "Creating OpenSky client..."
    )

    opensky = OpenSkyClient()

    # --------------------------------------------------------
    # Create Kafka producer
    # --------------------------------------------------------

    producer = build_kafka_producer(
        cfg
    )

    logger.info(
        "Starting producer loop: "
        "topic=%s, poll_interval=%ss",
        topic,
        poll_interval,
    )

    total_published = 0
    total_skipped = 0
    total_cycles = 0

    try:

        while not _shutdown_requested:

            loop_start = time.monotonic()

            total_cycles += 1

            logger.info(
                "Starting poll cycle #%d...",
                total_cycles,
            )

            try:

                # ------------------------------------------------
                # Fetch current aircraft states
                # ------------------------------------------------

                states = opensky.fetch_states()

                logger.info(
                    "Fetched %d aircraft states.",
                    len(states),
                )

                sent_count = 0
                skipped_count = 0

                # ------------------------------------------------
                # Publish aircraft records
                # ------------------------------------------------

                for state in states:

                    if _shutdown_requested:
                        break

                    if not state.get("icao24"):
                        skipped_count += 1
                        continue

                    published = _publish_record(
                        producer=producer,
                        topic=topic,
                        record=state,
                    )

                    if published:
                        sent_count += 1
                    else:
                        skipped_count += 1

                    # ------------------------------------------------
                    # Serve delivery callbacks and internal events.
                    # ------------------------------------------------

                    producer.poll(0)

                # ------------------------------------------------
                # Wait for all messages from this cycle
                # ------------------------------------------------

                flush_success = _flush_producer(
                    producer,
                    timeout_seconds=30.0,
                )

                total_published += sent_count
                total_skipped += skipped_count

                if flush_success:

                    logger.info(
                        "Poll cycle #%d completed: "
                        "published=%d skipped=%d "
                        "total_published=%d",
                        total_cycles,
                        sent_count,
                        skipped_count,
                        total_published,
                    )

                else:

                    logger.error(
                        "Poll cycle #%d completed with "
                        "undelivered messages.",
                        total_cycles,
                    )

            except Exception as exc:

                # ------------------------------------------------
                # Temporary OpenSky/network/Event Hubs failures
                # must not kill the long-running producer.
                # ------------------------------------------------

                logger.exception(
                    "Unexpected error during poll cycle #%d: %s",
                    total_cycles,
                    exc,
                )

            # ----------------------------------------------------
            # Maintain configured polling interval.
            #
            # If the API call + Kafka publishing took 3 seconds
            # and interval is 10 seconds, sleep approximately
            # 7 seconds.
            # ----------------------------------------------------

            elapsed = (
                time.monotonic()
                - loop_start
            )

            sleep_time = max(
                0.0,
                poll_interval - elapsed,
            )

            if sleep_time > 0:

                logger.debug(
                    "Sleeping %.2f seconds "
                    "before next poll.",
                    sleep_time,
                )

                # Use small increments so SIGTERM/SIGINT can
                # be detected without waiting the entire interval.
                end_time = (
                    time.monotonic()
                    + sleep_time
                )

                while (
                    not _shutdown_requested
                    and time.monotonic() < end_time
                ):

                    producer.poll(0)

                    time.sleep(
                        min(
                            0.5,
                            max(
                                0.0,
                                end_time
                                - time.monotonic(),
                            ),
                        )
                    )

    finally:

        logger.info(
            "Closing Kafka producer..."
        )

        # --------------------------------------------------------
        # Final flush
        # --------------------------------------------------------

        try:

            remaining = producer.flush(
                30.0
            )

            if remaining > 0:

                logger.error(
                    "Producer shutdown completed with "
                    "%d undelivered message(s).",
                    remaining,
                )

            else:

                logger.info(
                    "All pending Kafka messages delivered."
                )

        except Exception as exc:

            logger.exception(
                "Error while flushing producer during shutdown: %s",
                exc,
            )

        # --------------------------------------------------------
        # Close producer
        # --------------------------------------------------------

        try:

            producer = None

            logger.info(
                "Producer closed."
            )

        except Exception as exc:

            logger.exception(
                "Error while closing producer: %s",
                exc,
            )

        logger.info(
            "Producer shutdown complete. "
            "poll_cycles=%d total_published=%d total_skipped=%d",
            total_cycles,
            total_published,
            total_skipped,
        )


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":

    try:

        run_producer_loop()

    except KeyboardInterrupt:

        logger.info(
            "Producer stopped by user."
        )

    except Exception as exc:

        logger.exception(
            "Fatal producer error: %s",
            exc,
        )

        raise