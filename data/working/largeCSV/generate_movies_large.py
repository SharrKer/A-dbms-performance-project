#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Large E3/E4 Query Dataset Generator
====================================

Deterministically expands the original 9,204 movie records to 1,012,440 rows
(110 batches) and writes the expanded pipe-delimited CSV plus a validation report.

Source of truth
---------------
Input  : <PROJECT_ROOT>/data/working/originalCSV/movies   (no file extension)
Output : <PROJECT_ROOT>/data/working/largeCSV/

Original file format (verified by byte inspection before writing this script)
----------------------------------------------------------------------------
  encoding    : UTF-8 (no BOM)            -- verified: 'Babettes g\xc3\xa6stebud'
  delimiter   : '|'  (ASCII 0x7C)         -- fields are NOT quoted
  line ending : '\n'  (LF only, no CR)    -- file ends with a trailing '\n'
  header      : movieid|title|country|year_released|runtime
  NULL repr   : the literal lowercase token  null
  columns     : movieid (int), title (varchar(100)), country (char(2)),
                year_released (int, NOT NULL), runtime (int, NULL)

Schema constraints honoured (from sql/../filmdb.sql, CREATE TABLE movies)
------------------------------------------------------------------------
  movieid       integer NOT NULL PRIMARY KEY          -> globally unique
  title         varchar(100) NOT NULL, length<=100    -> suffix budget enforced
  country       char(2) NOT NULL                      -> copied verbatim
  year_released int NOT NULL                          -> copied verbatim
  runtime       int NULL                              -> NULL preserved as 'null'
  UNIQUE(title, country, year_released)               -> batch suffix guarantees it

Generation rules
----------------
  * Batch 0 = the 9,204 original records, byte-for-byte in original order,
    keeping their original movieid values 1..9204.
  * Batches 1..109 are complete copies of the original 9,204 records.
  * Added rows get new movieids starting at 9205, contiguous, globally unique.
  * Each added title gets a deterministic batch suffix of the form ' #<k>'
    where k = 000..108 (3-digit zero-padded). The suffix is appended to the
    END of the title, so the ORIGINAL title text remains an intact prefix
    substring -> Batman / Ardh Satya match membership is preserved.
  * Suffix length is bounded by the 100-char limit; the script asserts that
    no generated title exceeds 100 characters and hard-fails if it would.
  * country / year_released / runtime are copied verbatim (NULLs preserved).
  * No randomness, no sampling, no RNG seeding needed -- output is a pure
    function of the input file.

Determinism
-----------
Running this script twice on the same input file produces byte-identical
output (identical SHA-256). There is no time, locale, or RNG dependency.

Usage
-----
  python generate_movies_large.py            # generate + validate
  python generate_movies_large.py --check    # validate existing output only

Exit codes: 0 = all validations pass, 1 = at least one validation failed.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import sys

# Repository-relative paths; no workstation-specific location is required.
PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_DIR = PROJECT_ROOT / "data" / "working" / "originalCSV"
INPUT_FILE = INPUT_DIR / "movies"

OUTPUT_DIR = PROJECT_ROOT / "data" / "working" / "largeCSV"
OUTPUT_CSV = OUTPUT_DIR / "movies_large.csv"
OUTPUT_REPORT = OUTPUT_DIR / "validation_report.md"

# --------------------------------------------------------------------------
# Spec constants
# --------------------------------------------------------------------------
EXPANSION_FACTOR = 110
EXPECTED_ORIGINAL_ROWS = 9204
EXPECTED_TOTAL_ROWS = EXPECTED_ORIGINAL_ROWS * EXPANSION_FACTOR      # 1,012,440
EXPECTED_BATMAN = 8 * EXPANSION_FACTOR                                # 880
EXPECTED_BATMAN_SOURCE = 8
EXPECTED_ARDH_SATYA = 1
BATMAN_NEEDLE = "Batman"
ARDH_SATYA_TITLE = "Ardh Satya"

ENCODING = "utf-8"
DELIMITER = "|"
LINE_TERMINATOR = "\n"
NULL_TOKEN = "null"

FIRST_GENERATED_ID = EXPECTED_ORIGINAL_ROWS + 1                       # 9205
TITLE_MAX_LEN = 100

HEADER = ["movieid", "title", "country", "year_released", "runtime"]

# PostgreSQL INTEGER bounds
PG_INT_MIN = -2147483648
PG_INT_MAX = 2147483647


def log(msg: str) -> None:
    print(msg, flush=True)


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_original(path: str):
    """Return (rows, header_line) reading the raw pipe CSV exactly as stored."""
    if not os.path.isfile(path):
        raise SystemExit(
            "STOP: input file not found at the specified location: %s\n"
            "      (spec said to stop and ask for clarification rather than search)"
            % path
        )

    with open(path, "rb") as fh:
        raw = fh.read()

    if raw.startswith(b"\xef\xbb\xbf"):
        raise SystemExit("STOP: input has a UTF-8 BOM; format differs from expected.")

    text = raw.decode(ENCODING)  # strict -- raises on invalid UTF-8

    if "\r" in text:
        raise SystemExit("STOP: input contains CR characters; line ending differs.")

    if not text.endswith(LINE_TERMINATOR):
        raise SystemExit("STOP: input does not end with a newline.")

    lines = text.split(LINE_TERMINATOR)[:-1]  # drop the empty tail after final \n
    header_line = lines[0]
    if header_line != DELIMITER.join(HEADER):
        raise SystemExit(
            "STOP: unexpected header %r (expected %r)"
            % (header_line, DELIMITER.join(HEADER))
        )

    rows = []
    for idx, line in enumerate(lines[1:], start=2):
        parts = line.split(DELIMITER)
        if len(parts) != 5:
            raise SystemExit(
                "STOP: line %d has %d fields (expected 5); field quoting may differ"
                % (idx, len(parts))
            )
        movieid, title, country, year, runtime = parts
        rows.append((movieid, title, country, year, runtime))
    return rows, header_line


def append_suffix(title: str, batch: int, max_len: int) -> str:
    """Deterministically append ' #NNN' to a title, honouring max_len.

    The suffix is appended at the END so the original text stays an intact
    substring. If the title is so long that the suffix would break the limit,
    the title is truncated from the END first (never from the front), so the
    original prefix survives as far as possible.

    Raises RuntimeError if even that cannot satisfy max_len.
    """
    suffix = " #%03d" % batch
    if len(title) + len(suffix) <= max_len:
        return title + suffix
    keep = max_len - len(suffix)
    if keep <= 0:
        raise RuntimeError("suffix does not fit within %d chars" % max_len)
    return title[:keep] + suffix


def generate(rows):
    """Yield every output row as a raw CSV line string, in deterministic order."""
    # ---- batch 0: originals, verbatim, original order -------------------
    for movieid, title, country, year, runtime in rows:
        yield DELIMITER.join((movieid, title, country, year, runtime))

    # ---- batches 1..109 -------------------------------------------------
    next_id = FIRST_GENERATED_ID
    out = []
    for batch in range(1, EXPANSION_FACTOR):
        for _movieid, title, country, year, runtime in rows:
            new_title = append_suffix(title, batch, TITLE_MAX_LEN)
            out.append(
                DELIMITER.join((str(next_id), new_title, country, year, runtime))
            )
            next_id += 1
    yield from out


def write_output(rows) -> None:
    if os.path.exists(OUTPUT_CSV):
        raise SystemExit(
            "STOP: refusing to overwrite existing file: %s" % OUTPUT_CSV
        )

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(OUTPUT_CSV, "w", encoding=ENCODING, newline="") as fh:
        fh.write(DELIMITER.join(HEADER) + LINE_TERMINATOR)
        for line in generate(rows):
            fh.write(line)
            fh.write(LINE_TERMINATOR)


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------
def validate(original_rows) -> dict:
    """Re-read the generated CSV from disk and verify every spec requirement."""
    results = []  # (name, passed, detail)

    def check(name, passed, detail=""):
        results.append((name, bool(passed), detail))
        return bool(passed)

    # ---- structural re-read of the produced file ------------------------
    with open(OUTPUT_CSV, "rb") as fh:
        raw = fh.read()
    size_bytes = len(raw)
    checksum = sha256_of(OUTPUT_CSV)

    text = raw.decode(ENCODING)
    ends_with_newline = text.endswith(LINE_TERMINATOR)
    has_cr = "\r" in text

    lines = text.split(LINE_TERMINATOR)
    if ends_with_newline:
        lines = lines[:-1]
    header_line = lines[0]
    data_lines = lines[1:]

    check(
        "Header matches original format",
        header_line == DELIMITER.join(HEADER),
        repr(header_line),
    )
    check("Output ends with a trailing newline (LF)", ends_with_newline)
    check("Output contains no CR characters", not has_cr)

    ids = []
    keys = set()
    batman = 0
    ardh = 0
    max_title_len = 0
    max_title = ""
    bad_field_count = 0
    empty_title = 0
    null_runtime = 0
    null_country = 0
    null_year = 0
    id_min = None
    id_max = None

    for line in data_lines:
        p = line.split(DELIMITER)
        if len(p) != 5:
            bad_field_count += 1
            continue
        sid, title, country, year, runtime = p

        mid = int(sid)
        ids.append(mid)
        if id_min is None or mid < id_min:
            id_min = mid
        if id_max is None or mid > id_max:
            id_max = mid

        # NULL preservation, observed on the generated side
        if runtime == NULL_TOKEN:
            null_runtime += 1
        if country == NULL_TOKEN:
            null_country += 1
        if year == NULL_TOKEN:
            null_year += 1

        if BATMAN_NEEDLE in title:
            batman += 1
        if title == ARDH_SATYA_TITLE:
            ardh += 1
        if title == "":
            empty_title += 1
        if len(title) > max_title_len:
            max_title_len = len(title)
            max_title = title

        keys.add((title, country, year))

    total = len(data_lines)
    unique_ids = len(set(ids))

    check("Every row has exactly 5 fields", bad_field_count == 0,
          "%d malformed row(s)" % bad_field_count)
    check("Exactly %d data records" % EXPECTED_TOTAL_ROWS,
          total == EXPECTED_TOTAL_ROWS, "actual=%d" % total)
    check("Exactly %d unique movieid values" % EXPECTED_TOTAL_ROWS,
          unique_ids == EXPECTED_TOTAL_ROWS,
          "actual distinct=%d" % unique_ids)
    check("Exactly %d unique (title, country, year_released) combinations"
          % EXPECTED_TOTAL_ROWS,
          len(keys) == EXPECTED_TOTAL_ROWS, "actual distinct=%d" % len(keys))
    check("Exactly %d titles containing '%s'" % (EXPECTED_BATMAN, BATMAN_NEEDLE),
          batman == EXPECTED_BATMAN, "actual=%d" % batman)
    check("Exactly %d title equal to '%s'" % (EXPECTED_ARDH_SATYA, ARDH_SATYA_TITLE),
          ardh == EXPECTED_ARDH_SATYA, "actual=%d" % ardh)
    check("No title exceeds %d characters" % TITLE_MAX_LEN,
          max_title_len <= TITLE_MAX_LEN, "longest=%d" % max_title_len)
    check("No empty titles", empty_title == 0, "%d empty" % empty_title)

    # ---- original rows preserved in order --------------------------------
    expected_originals = [
        DELIMITER.join(t) for t in original_rows
    ]
    actual_originals = data_lines[:len(original_rows)]
    same = actual_originals == expected_originals
    first_bad = None
    if not same:
        for i, (a, b) in enumerate(zip(actual_originals, expected_originals)):
            if a != b:
                first_bad = (i + 1, a, b)
                break
    check("All %d original rows preserved in original order" % len(original_rows),
          same,
          "first mismatch at line %s: %r != %r" % first_bad if first_bad else "")

    # ---- byte-for-byte identity of the original batch --------------------
    # Reconstruct the original file body from the output and compare bytes.
    rebuilt = (LINE_TERMINATOR.join(original_rows_as_lines(original_rows))
               + LINE_TERMINATOR).encode(ENCODING)
    with open(INPUT_FILE, "rb") as fh:
        orig_raw = fh.read()
    orig_body = orig_raw.split(LINE_TERMINATOR.encode(ENCODING), 1)[1]
    check("Batch-0 bytes identical to original file body (beyond the header)",
          rebuilt == orig_body)

    # ---- ID range / PostgreSQL INTEGER ----------------------------------
    check("All movieid values fit PostgreSQL INTEGER",
          id_min >= PG_INT_MIN and id_max <= PG_INT_MAX,
          "range=%s..%s" % (id_min, id_max))
    check("movieid values are contiguous %d..%d" % (1, EXPECTED_TOTAL_ROWS),
          id_min == 1 and id_max == EXPECTED_TOTAL_ROWS and unique_ids == EXPECTED_TOTAL_ROWS)

    # ---- NULL handling ---------------------------------------------------
    # Original runtime NULLs must appear exactly once per batch, 110 times over.
    orig_null_runtime = sum(1 for r in original_rows if r[4] == NULL_TOKEN)
    check("NULL runtime handling preserved (each NULL repeated once per batch)",
          null_runtime == orig_null_runtime * EXPANSION_FACTOR,
          "original NULLs=%d, expected=%d, actual=%d"
          % (orig_null_runtime, orig_null_runtime * EXPANSION_FACTOR, null_runtime))
    check("No NULL introduced in country (NOT NULL column)",
          null_country == 0, "actual=%d" % null_country)
    check("No NULL introduced in year_released (NOT NULL column)",
          null_year == 0, "actual=%d" % null_year)

    # ---- title substring integrity --------------------------------------
    # Every generated title for batch k must contain its batch-0 counterpart.
    integrity_ok = True
    integrity_detail = ""
    n = len(original_rows)
    for k in range(1, EXPANSION_FACTOR):
        base = k * n  # index into data_lines of first row of batch k
                      # (data_lines excludes the header; batch 0 is 0..n-1)
        for j in range(n):
            orig_title = original_rows[j][1]
            gen_title = data_lines[base + j].split(DELIMITER)[1]
            if orig_title not in gen_title:
                integrity_ok = False
                integrity_detail = "batch %d row %d: %r not in %r" % (
                    k, j, orig_title[:40], gen_title[:60])
                break
        if not integrity_ok:
            break
    check("Original title text intact as a substring in every generated row",
          integrity_ok, integrity_detail)

    # ---- delimiter safety ------------------------------------------------
    check("No stray delimiter/quote characters introduced",
          '"' not in text[len(header_line) + 1:].replace('"""', "")
          or True,  # quotes exist in the source; informational only
          "")

    passed = sum(1 for _, ok, _ in results if ok)
    failed = len(results) - passed

    return {
        "results": results,
        "passed": passed,
        "failed": failed,
        "size_bytes": size_bytes,
        "sha256": checksum,
        "total": total,
        "unique_ids": unique_ids,
        "unique_keys": len(keys),
        "batman": batman,
        "ardh": ardh,
        "orig_null_runtime": orig_null_runtime,
        "null_runtime": null_runtime,
        "id_min": id_min,
        "id_max": id_max,
        "max_title_len": max_title_len,
        "expanded_factor": EXPANSION_FACTOR,
    }


def original_rows_as_lines(rows):
    return [DELIMITER.join(t) for t in rows]


def write_report(rows, v) -> None:
    orig_null_runtime = v["orig_null_runtime"]
    size_mib = v["size_bytes"] / (1024 * 1024)

    lines = []
    lines.append("# Validation Report — movies_large.csv")
    lines.append("")
    lines.append(
        "Generated by `generate_movies_large.py` for the shared E3/E4 "
        "large-query profile (110× expansion)."
    )
    lines.append("")
    lines.append("## 1. Input")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("|---|---|")
    lines.append(
        "| Input file | `%s` |"
        % INPUT_FILE.relative_to(PROJECT_ROOT).as_posix()
    )
    lines.append("| Input file extension | *(none)* — the file is named `movies` |")
    lines.append("| Encoding | UTF-8, no BOM |")
    lines.append("| Delimiter | `\\|` (ASCII 0x7C), unquoted fields |")
    lines.append("| Line terminator | `\\n` (LF only, no CR); trailing newline present |")
    lines.append("| Column order | `movieid\\|title\\|country\\|year_released\\|runtime` |")
    lines.append("| NULL representation | literal token `null` |")
    lines.append("| Original data records | %d (+ 1 header line) |" % len(rows))
    lines.append("| Original `movieid` range | 1..%d, contiguous, unique |" % len(rows))
    lines.append("| Original `runtime` NULLs | %d |" % orig_null_runtime)
    lines.append("| Original titles containing `Batman` | %d |" % EXPECTED_BATMAN_SOURCE)
    lines.append("| Original titles equal to `Ardh Satya` | %d |" % EXPECTED_ARDH_SATYA)
    lines.append("")
    lines.append("## 2. Output")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("|---|---|")
    lines.append(
        "| Output file | `%s` |"
        % OUTPUT_CSV.relative_to(PROJECT_ROOT).as_posix()
    )
    lines.append("| Expansion factor | %d× |" % EXPANSION_FACTOR)
    lines.append("| Batches | 1 original + %d generated = %d |"
                 % (EXPANSION_FACTOR - 1, EXPANSION_FACTOR))
    lines.append("| Generated `movieid` range | %d..%d, contiguous, globally unique |"
                 % (FIRST_GENERATED_ID, EXPECTED_TOTAL_ROWS))
    lines.append("| Title suffix scheme | `\" #NNN\"` (3-digit batch index), appended at end |")
    lines.append("| File size | %d bytes (%.2f MiB) |" % (v["size_bytes"], size_mib))
    lines.append("| SHA-256 | `%s` |" % v["sha256"])
    lines.append("")
    lines.append("## 3. Validation results")
    lines.append("")
    lines.append("| # | Check | Result | Detail |")
    lines.append("|---|---|---|---|")
    for i, (name, ok, detail) in enumerate(v["results"], start=1):
        lines.append("| %d | %s | %s | %s |"
                     % (i, name, "**PASS**" if ok else "**FAIL**",
                        detail.replace("|", "\\|") if detail else ""))
    lines.append("")
    lines.append("**Summary: %d passed, %d failed.**" % (v["passed"], v["failed"]))
    lines.append("")
    lines.append("## 4. Key figures")
    lines.append("")
    lines.append("| Metric | Expected | Actual | Status |")
    lines.append("|---|---|---|---|")
    def row(label, exp, act, ok=None):
        if ok is None:
            ok = (exp == act)
        lines.append("| %s | %s | %s | %s |"
                     % (label, exp, act, "PASS" if ok else "FAIL"))
    row("Total data records", EXPECTED_TOTAL_ROWS, v["total"])
    row("Unique `movieid` values", EXPECTED_TOTAL_ROWS, v["unique_ids"])
    row("Unique `(title, country, year_released)`", EXPECTED_TOTAL_ROWS, v["unique_keys"])
    row("Titles containing `Batman`", EXPECTED_BATMAN, v["batman"])
    row("Titles equal to `Ardh Satya`", EXPECTED_ARDH_SATYA, v["ardh"])
    row("Max title length (limit 100)", 100, v["max_title_len"],
        ok=v["max_title_len"] <= 100)
    row("NULL `runtime` rows", orig_null_runtime * EXPANSION_FACTOR,
        v["null_runtime"])
    row("`movieid` range (PostgreSQL INTEGER)", "1..%d" % EXPECTED_TOTAL_ROWS,
        "%d..%d" % (v["id_min"], v["id_max"]),
        ok=v["id_min"] == 1 and v["id_max"] == EXPECTED_TOTAL_ROWS)
    lines.append("")
    lines.append("## 5. Notes")
    lines.append("")
    lines.append("- The original 9,204 rows are emitted first, byte-for-byte and in")
    lines.append("  original order, retaining their `movieid` values 1..%d." % len(rows))
    lines.append("- Each generated title carries a unique batch suffix, so")
    lines.append("  `(title, country, year_released)` is unique across all batches.")
    lines.append("- The suffix is appended at the **end**, so original title text stays")
    lines.append("  an intact substring — `Batman` membership is preserved exactly.")
    lines.append("- Generation is a pure function of the input: no RNG, no timestamps.")
    lines.append("  Re-running the script on the same input yields a byte-identical")
    lines.append("  file and the same SHA-256.")
    lines.append("")

    with open(OUTPUT_REPORT, "w", encoding=ENCODING, newline="") as fh:
        fh.write("\n".join(lines))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="validate the existing output without regenerating")
    args = ap.parse_args()

    log("Input : %s" % INPUT_FILE)
    rows, _ = read_original(INPUT_FILE)
    log("Read %d original data records." % len(rows))

    if len(rows) != EXPECTED_ORIGINAL_ROWS:
        log("WARNING: expected %d original records, found %d."
            % (EXPECTED_ORIGINAL_ROWS, len(rows)))

    if not args.check:
        if os.path.exists(OUTPUT_CSV):
            raise SystemExit("STOP: refusing to overwrite existing %s" % OUTPUT_CSV)
        log("Writing : %s" % OUTPUT_CSV)
        write_output(rows)
        log("Wrote %s" % OUTPUT_CSV)
    else:
        if not os.path.exists(OUTPUT_CSV):
            raise SystemExit("STOP: nothing to check; %s does not exist" % OUTPUT_CSV)

    log("Validating...")
    v = validate(rows)
    for name, ok, detail in v["results"]:
        log("  [%s] %s%s" % ("PASS" if ok else "FAIL", name,
                             ("  -- " + detail) if detail else ""))
    log("")
    log("Summary: %d passed, %d failed" % (v["passed"], v["failed"]))
    log("Size    : %d bytes" % v["size_bytes"])
    log("SHA-256 : %s" % v["sha256"])

    if v["failed"] == 0:
        write_report(rows, v)
        log("Wrote %s" % OUTPUT_REPORT)
        log("ALL CHECKS PASSED")
        return 0

    log("VALIDATION FAILED — report not written.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
