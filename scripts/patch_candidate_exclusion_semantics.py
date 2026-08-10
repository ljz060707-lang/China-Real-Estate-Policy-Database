from datetime import datetime
from pathlib import Path

PATH = Path(
    r"D:\Codex\projects\Documents-Codex\2026-07-13"
    r"\text-20260705-xlsx-text-data-raw\policy-database"
    r"\src\policydb\source_slots.py"
)


def replace_once(
    text: str,
    old: str,
    new: str,
    label: str,
) -> str:
    if new in text:
        print(f"already patched: {label}")
        return text

    if old not in text:
        raise RuntimeError(
            f"未找到目标代码块：{label}"
        )

    print(f"patched: {label}")
    return text.replace(old, new, 1)


def main() -> None:
    text = PATH.read_text(
        encoding="utf-8-sig"
    )

    helper_marker = '''def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(frame, path, {"module": "source_slots"})


def _normalize_excel_core_properties'''

    helper_replacement = '''def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(frame, path, {"module": "source_slots"})


EXCLUDED_REVIEW_PREFIX = "excluded_"


def _active_candidates(
    frame: pl.DataFrame,
) -> pl.DataFrame:
    """Exclude curated-invalid candidates from operational calculations."""
    if (
        frame.is_empty()
        or "manual_review_status" not in frame.columns
    ):
        return frame

    return frame.filter(
        ~pl.col("manual_review_status")
        .fill_null("")
        .cast(pl.String)
        .str.starts_with(
            EXCLUDED_REVIEW_PREFIX
        )
    )


def _normalize_excel_core_properties'''

    text = replace_once(
        text,
        helper_marker,
        helper_replacement,
        "active candidate helper",
    )

    old_slots = '''    existing_candidates = (
        read_parquet_snapshot(candidate_path)
        if candidate_path.exists()
        else pl.DataFrame()
    )
    registry = load_registry(settings)
'''

    new_slots = '''    existing_candidates = (
        read_parquet_snapshot(candidate_path)
        if candidate_path.exists()
        else pl.DataFrame()
    )
    existing_candidates = _active_candidates(
        existing_candidates
    )
    registry = load_registry(settings)
'''

    text = replace_once(
        text,
        old_slots,
        new_slots,
        "exclude curated rows from 525 slots",
    )

    start = text.index(
        "def verify_candidates("
    )

    end = text.index(
        "\n\ndef rebuild_verification_audit",
        start,
    )

    verify_block = text[start:end]

    verify_block = replace_once(
        verify_block,
        '''    all_candidates = list_candidates(settings=settings)
''',
        '''    all_candidates = _active_candidates(
        list_candidates(settings=settings)
    )
''',
        "exclude curated rows from duplicate universe",
    )

    verify_block = replace_once(
        verify_block,
        '''    frame = list_candidates(
        city=city,
        source_id=source_id,
        candidate_id=candidate_id,
        slot_id=slot_id,
        settings=settings,
    )
    if candidate_ids is not None:
''',
        '''    frame = list_candidates(
        city=city,
        source_id=source_id,
        candidate_id=candidate_id,
        slot_id=slot_id,
        settings=settings,
    )
    frame = _active_candidates(frame)
    if candidate_ids is not None:
''',
        "skip curated rows during verification",
    )

    text = (
        text[:start]
        + verify_block
        + text[end:]
    )

    old_preserve = '''            if (
                not authoritative_review
                and previous.get("manual_review_status") in {"approved", "verified"}
            ):
                merged["manual_review_status"] = previous["manual_review_status"]
'''

    new_preserve = '''            previous_review_status = str(
                previous.get(
                    "manual_review_status"
                )
                or ""
            )
            if (
                not authoritative_review
                and (
                    previous_review_status
                    in {"approved", "verified"}
                    or previous_review_status.startswith(
                        EXCLUDED_REVIEW_PREFIX
                    )
                )
            ):
                merged["manual_review_status"] = (
                    previous_review_status
                )
'''

    text = replace_once(
        text,
        old_preserve,
        new_preserve,
        "preserve exclusions during rediscovery",
    )

    backup = PATH.with_name(
        "source_slots.py.bak_"
        + datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )
    )

    backup.write_text(
        PATH.read_text(
            encoding="utf-8-sig"
        ),
        encoding="utf-8",
    )

    PATH.write_text(
        text,
        encoding="utf-8",
    )

    print(f"backup={backup}")
    print(f"patched={PATH}")


if __name__ == "__main__":
    main()
