#!/usr/bin/env python3
"""Poll messages from Kafka and display them as markdown tables."""

import uuid
from typing import Optional

import typer

app = typer.Typer(help="Poll Kafka messages and display as markdown tables")


def parse_topic(topic_spec: str):
    """
    Parse a topic specification in the format: topic_name:policy[:time_ms]

    Examples:
        topic_1:latest
        topic_2:earliest
        topic_3:committed
        topic_4:relative_time:3600000
        topic_5:absolute_time:1704067200000
    """
    from kafkars import SourceTopic

    parts = topic_spec.split(":")
    if len(parts) < 2:
        raise typer.BadParameter(
            f"Invalid topic format: '{topic_spec}'. "
            "Expected format: topic_name:policy[:time_ms]"
        )

    name = parts[0]
    policy = parts[1].lower()

    if policy == "latest":
        return SourceTopic.from_latest(name)
    elif policy == "earliest":
        return SourceTopic.from_earliest(name)
    elif policy == "committed":
        return SourceTopic.from_committed(name)
    elif policy == "relative_time":
        if len(parts) < 3:
            raise typer.BadParameter(
                f"relative_time policy requires time_ms: '{topic_spec}'"
            )
        time_ms = int(parts[2])
        return SourceTopic.from_relative_time(name, time_ms)
    elif policy == "absolute_time":
        if len(parts) < 3:
            raise typer.BadParameter(
                f"absolute_time policy requires time_ms: '{topic_spec}'"
            )
        time_ms = int(parts[2])
        return SourceTopic.from_absolute_time(name, time_ms)
    else:
        raise typer.BadParameter(
            f"Unknown policy: '{policy}'. "
            "Expected: latest, earliest, committed, relative_time, absolute_time"
        )


@app.command()
def poll(
    bootstrap_servers: str = typer.Argument(
        ..., help="Kafka bootstrap servers (e.g., localhost:9092)"
    ),
    topics: list[str] = typer.Argument(
        ...,
        help="Topics with policy: topic:policy[:time_ms]. "
        "Policies: latest, earliest, committed, relative_time, absolute_time. "
        "Examples: topic_1:latest, topic_2:earliest, topic_3:relative_time:3600000",
    ),
    timeout_ms: int = typer.Option(
        1000, "--timeout", "-t", help="Poll timeout in milliseconds"
    ),
    max_batches: Optional[int] = typer.Option(
        None, "--max-batches", "-n", help="Maximum number of batches to consume"
    ),
    batch_size: int = typer.Option(
        1000, "--batch-size", "-b", help="Maximum messages per batch"
    ),
    show_state: bool = typer.Option(
        False, "--show-state", "-s", help="Show partition state after each poll"
    ),
) -> None:
    """Poll messages from Kafka topics and display them as markdown tables."""
    from kafkars import ConsumerManager

    # Parse topic specifications
    source_topics = [parse_topic(t) for t in topics]

    # Build consumer config
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": str(uuid.uuid4()),
        "enable.auto.commit": "false",
    }

    # Create consumer manager
    # Using a far-future cutoff to avoid stopping early
    cutoff_ms = 2**62

    typer.secho(f"Connecting to {bootstrap_servers}...", fg=typer.colors.CYAN)
    for topic_spec in topics:
        typer.secho(f"  Subscribing: {topic_spec}", fg=typer.colors.GREEN)
    typer.secho("Press Ctrl+C to stop\n", fg=typer.colors.BRIGHT_BLACK)

    manager = ConsumerManager(config, source_topics, cutoff_ms, batch_size)

    batch_count = 0
    total_messages = 0

    try:
        while True:
            batch = manager.poll(timeout_ms)

            if batch.num_rows > 0:
                typer.echo(f"\n{batch.to_pandas().to_markdown()}")
                typer.secho(
                    f"\n{batch.num_rows} message(s) received",
                    fg=typer.colors.YELLOW,
                    bold=True,
                )

                if show_state:
                    state = manager.partition_state()
                    typer.secho("\nPartition State:", fg=typer.colors.BLUE, bold=True)
                    typer.echo(state.to_pandas().to_markdown())

                typer.echo()
                batch_count += 1
                total_messages += batch.num_rows

                if max_batches and batch_count >= max_batches:
                    typer.secho(
                        f"Reached maximum batch count ({max_batches})",
                        fg=typer.colors.MAGENTA,
                    )
                    break

    except KeyboardInterrupt:
        typer.secho(
            f"\nStopped. Received {total_messages} total messages in {batch_count} batches.",
            fg=typer.colors.RED,
            bold=True,
        )


if __name__ == "__main__":
    app()
