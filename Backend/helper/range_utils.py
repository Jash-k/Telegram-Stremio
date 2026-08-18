def stream_read_ahead(
    part_count: int,
    max_parallelism: int = 4,
    max_prefetch: int = 8,
) -> tuple[int, int]:
    """Return bounded parallel Telegram reads and ordered queue capacity."""
    parts = max(1, int(part_count or 1))
    parallelism = min(max(1, int(max_parallelism)), parts)
    prefetch = min(max(1, int(max_prefetch)), parts, max(2, parallelism * 2))
    return parallelism, max(1, prefetch)


def chunk_window(start: int, end: int, chunk_size: int) -> tuple[int, int, int, int]:
    """Return Telegram chunk parameters for an inclusive HTTP byte range.

    The returned tuple is ``(offset, first_cut, last_cut, part_count)``.
    ``start`` and ``end`` are inclusive, while Python slice bounds are
    exclusive; this is why ``last_cut`` includes one extra byte.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if start < 0 or end < start:
        raise ValueError("invalid inclusive byte range")

    offset = start - (start % chunk_size)
    first_cut = start - offset
    last_cut = (end % chunk_size) + 1

    # Both bounds are inclusive.  Using ceil(end / chunk_size) returns zero
    # for bytes=0-0 and undercounts ranges ending exactly on a chunk boundary.
    part_count = (end // chunk_size) - (offset // chunk_size) + 1
    return offset, first_cut, last_cut, part_count
