"""
Centralized Logging & Artifact Naming Utilities
=================================================

Provides:
- Date-only artifact naming (YYYYMMDD, no HHMMSS)
- CSV-based structured logging (15-column schema)
- stdout/stderr tee capture into CSV log
- Reusable across MSPBNA_CR_Normalized.py and report_generator.py

CSV Log Schema (15 columns):
    timestamp, run_date, script_name, run_id, level, phase, component,
    function, line_no, event_type, message, exception_type, exception_message,
    traceback, context_json

Event types:
    CONFIG, FILE_DISCOVERED, FILE_WRITTEN, DATAFRAME_SHAPE, VALIDATION_WARNING,
    VALIDATION_ERROR, EXCEPTION, STDOUT, STDERR, CHART_SKIPPED, TABLE_SKIPPED,
    METRIC_SUPPRESSED, PRECHECK_FAIL, PRECHECK_WARN

Safe lifecycle:
    - CsvLogger.log() is a no-op after close (never raises)
    - TeeToLogger.write() always writes to the original stream first, then
      attempts CSV logging; if the logger is closed, CSV logging is silently
      skipped — console output is never interrupted
    - CsvLogger.close() restores sys.stdout/sys.stderr before closing the file
    - close() and shutdown() are idempotent
    - Logging failures never crash the pipeline or mask real exceptions
"""

import csv
import io
import json
import os
import sys
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
#  PART 1 — Date-Only Artifact Naming
# ---------------------------------------------------------------------------

def get_run_date_str() -> str:
    """Return today's date as YYYYMMDD string for artifact naming."""
    return datetime.now().strftime("%Y%m%d")


def build_artifact_filename(
    prefix: str,
    suffix: str,
    ext: str = ".xlsx",
    output_dir: Optional[str] = None,
) -> str:
    """
    Build a date-stamped artifact filename.

    Parameters
    ----------
    prefix : str
        Base name, e.g. "Bank_Performance_Dashboard"
    suffix : str
        Descriptor after the date stamp, e.g. "standard_credit_chart".
        Pass empty string for no suffix.
    ext : str
        File extension including the dot, e.g. ".xlsx", ".png", ".html"
    output_dir : str or None
        Directory to prepend. If None, returns bare filename.

    Returns
    -------
    str
        e.g. "output/Bank_Performance_Dashboard_20260311.xlsx"
        or   "output/Peers/charts/stem_standard_credit_chart_20260311.png"
    """
    date_str = get_run_date_str()
    if suffix:
        name = f"{prefix}_{suffix}_{date_str}{ext}"
    else:
        name = f"{prefix}_{date_str}{ext}"
    if output_dir:
        return str(Path(output_dir) / name)
    return name


# ---------------------------------------------------------------------------
#  PART 2-3 — CSV Log Schema & Writer
# ---------------------------------------------------------------------------

CSV_LOG_COLUMNS = [
    "timestamp",
    "run_date",
    "script_name",
    "run_id",
    "level",
    "phase",
    "component",
    "function",
    "line_no",
    "event_type",
    "message",
    "exception_type",
    "exception_message",
    "traceback",
    "context_json",
]

# Valid event types
EVENT_TYPES = frozenset({
    "CONFIG",
    "FILE_DISCOVERED",
    "FILE_WRITTEN",
    "DATAFRAME_SHAPE",
    "VALIDATION_WARNING",
    "VALIDATION_ERROR",
    "EXCEPTION",
    "STDOUT",
    "STDERR",
    "CHART_SKIPPED",
    "TABLE_SKIPPED",
    "METRIC_SUPPRESSED",
    "PRECHECK_FAIL",
    "PRECHECK_WARN",
})


class CsvLogger:
    """
    Structured CSV logger for pipeline scripts.

    Each script gets its own CSV log file named:
        <script_name>_YYYYMMDD_log.csv

    The log is reset (overwritten) each run.

    Safe lifecycle guarantees:
    - log() is a no-op after close (never raises)
    - close() restores stdout/stderr before closing the file
    - close() and shutdown() are idempotent
    """

    def __init__(
        self,
        script_name: str,
        log_dir: str = "logs",
    ):
        self.script_name = script_name
        self.run_id = uuid.uuid4().hex[:12]
        self.run_date = get_run_date_str()
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.log_filename = str(
            self.log_dir / f"{script_name}_{self.run_date}_log.csv"
        )

        self._closed = False

        # Track original streams for restoration on close
        self._original_stdout = None
        self._original_stderr = None

        # Open in write mode (reset each run) with newline='' for csv module
        self._file = open(self.log_filename, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._file, fieldnames=CSV_LOG_COLUMNS, extrasaction="ignore"
        )
        self._writer.writeheader()
        self._file.flush()

    @property
    def is_closed(self) -> bool:
        """Whether this logger has been closed."""
        return self._closed

    def log(
        self,
        level: str,
        message: str,
        event_type: str = "CONFIG",
        phase: str = "",
        component: str = "",
        function: str = "",
        line_no: str = "",
        exception_type: str = "",
        exception_message: str = "",
        tb: str = "",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Write a single structured row to the CSV log.

        Safe: no-op if logger is closed. Never raises.
        """
        if self._closed:
            return
        try:
            row = {
                "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                "run_date": self.run_date,
                "script_name": self.script_name,
                "run_id": self.run_id,
                "level": level.upper(),
                "phase": phase,
                "component": component,
                "function": function,
                "line_no": str(line_no) if line_no else "",
                "event_type": event_type,
                "message": message,
                "exception_type": exception_type,
                "exception_message": exception_message,
                "traceback": tb,
                "context_json": json.dumps(context) if context else "",
            }
            self._writer.writerow(row)
            self._file.flush()
        except Exception:
            pass

    # Convenience methods
    def info(self, message: str, **kwargs) -> None:
        self.log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self.log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        self.log("ERROR", message, **kwargs)

    def log_exception(
        self,
        exc: Exception,
        message: str = "",
        phase: str = "",
        component: str = "",
        function: str = "",
    ) -> None:
        """Log an exception with full traceback. Never raises."""
        try:
            tb_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
            self.log(
                level="ERROR",
                message=message or str(exc),
                event_type="EXCEPTION",
                phase=phase,
                component=component,
                function=function,
                exception_type=type(exc).__name__,
                exception_message=str(exc),
                tb="".join(tb_str),
            )
        except Exception:
            pass

    def log_file_written(
        self, filepath: str, phase: str = "", component: str = "",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log a FILE_WRITTEN event."""
        self.info(
            f"File written: {filepath}",
            event_type="FILE_WRITTEN",
            phase=phase,
            component=component,
            context=context,
        )

    def log_df_shape(
        self, name: str, rows: int, cols: int,
        phase: str = "", component: str = "",
    ) -> None:
        """Log a DATAFRAME_SHAPE event."""
        self.info(
            f"{name}: {rows} rows x {cols} cols",
            event_type="DATAFRAME_SHAPE",
            phase=phase,
            component=component,
            context={"df_name": name, "rows": rows, "cols": cols},
        )

    def restore_streams(self) -> None:
        """Restore sys.stdout and sys.stderr to their original streams.

        Only restores if they are currently wrapped by TeeToLogger instances
        that belong to this logger.
        """
        if self._original_stdout is not None:
            if isinstance(sys.stdout, TeeToLogger) and sys.stdout._csv_logger is self:
                sys.stdout = self._original_stdout
            self._original_stdout = None

        if self._original_stderr is not None:
            if isinstance(sys.stderr, TeeToLogger) and sys.stderr._csv_logger is self:
                sys.stderr = self._original_stderr
            self._original_stderr = None

    def close(self) -> None:
        """Restore streams, flush, and close the CSV log file.

        Idempotent — safe to call multiple times. Never raises.
        """
        if self._closed:
            return
        self._closed = True
        self.restore_streams()
        try:
            if self._file and not self._file.closed:
                self._file.flush()
                self._file.close()
        except Exception:
            pass

    def shutdown(self) -> None:
        """Safe shutdown: log final message, restore streams, close file.

        Idempotent. Suppresses all secondary logging exceptions.
        """
        if self._closed:
            return
        try:
            self.log(
                level="INFO",
                message="Logger shutdown",
                event_type="CONFIG",
                phase="shutdown",
            )
        except Exception:
            pass
        self.close()


# ---------------------------------------------------------------------------
#  PART 4 — stdout/stderr Tee Capture
# ---------------------------------------------------------------------------

class TeeToLogger:
    """
    Wraps a stream (stdout or stderr) so that every write() is also
    captured as a CSV log row. Console output is preserved.

    Safe lifecycle guarantees:
    - write() always writes to the original stream first
    - If the CSV logger is closed, CSV logging is silently skipped
    - write() never raises from logging failures

    Usage:
        csv_logger = CsvLogger("my_script")
        sys.stdout = TeeToLogger(sys.stdout, csv_logger, stream_name="STDOUT")
        sys.stderr = TeeToLogger(sys.stderr, csv_logger, stream_name="STDERR")
    """

    # Patterns that indicate a progress bar / tqdm line on stderr (not a real error)
    import re as _re
    _PROGRESS_PATTERNS = _re.compile(
        r"(\d+%\|)"          # tqdm: "  5%|███"
        r"|(\|[▏▎▍▌▋▊▉█ ]+\|)"  # tqdm bar characters
        r"|(it/s|s/it)"      # tqdm throughput suffix
        r"|(\d+/\d+\s*\[)"   # tqdm: "15/100 ["
        r"|(\r)"             # carriage-return progress overwrites
    )

    def __init__(self, original_stream, csv_logger: CsvLogger, stream_name: str = "STDOUT"):
        self._original = original_stream
        self._csv_logger = csv_logger
        self._stream_name = stream_name
        self._event_type = stream_name  # "STDOUT" or "STDERR"
        self._is_stderr = (stream_name == "STDERR")

    def _classify_level(self, text: str) -> str:
        """Classify log level for captured text.

        STDOUT is always INFO. STDERR uses heuristics: progress bars and
        tqdm output are downgraded to INFO; everything else stays WARNING
        (not ERROR — stderr is often used for non-error diagnostics).
        """
        if not self._is_stderr:
            return "INFO"
        # Progress bars / tqdm lines → INFO (not real errors)
        if self._PROGRESS_PATTERNS.search(text):
            return "INFO"
        # Explicit error keywords in the text → ERROR
        text_lower = text.lower()
        if any(kw in text_lower for kw in ("error", "exception", "traceback", "failed")):
            return "ERROR"
        # Default stderr to WARNING (diagnostic, not necessarily an error)
        return "WARNING"

    def write(self, text: str) -> int:
        # Always write to original stream first (preserve console output)
        result = self._original.write(text)
        # Attempt CSV logging only if logger is still open
        if not self._csv_logger.is_closed:
            stripped = text.strip()
            if stripped:
                try:
                    self._csv_logger.log(
                        level=self._classify_level(stripped),
                        message=stripped,
                        event_type=self._event_type,
                        component="console_capture",
                    )
                except Exception:
                    pass
        return result

    def flush(self) -> None:
        self._original.flush()

    def fileno(self):
        return self._original.fileno()

    def isatty(self):
        return self._original.isatty()

    @property
    def encoding(self):
        return getattr(self._original, "encoding", "utf-8")

    def __getattr__(self, name):
        return getattr(self._original, name)


# ---------------------------------------------------------------------------
#  Convenience: Full logging setup for a script
# ---------------------------------------------------------------------------

def setup_csv_logging(
    script_name: str,
    log_dir: str = "logs",
    capture_stdout: bool = True,
    capture_stderr: bool = True,
) -> CsvLogger:
    """
    One-call setup: create CSV logger, tee stdout/stderr, log startup event.

    Safe against nested calls: if stdout/stderr are already TeeToLogger
    instances, they are unwrapped before re-wrapping to prevent stacking.

    Parameters
    ----------
    script_name : str
        Identifies the script, e.g. "MSPBNA_CR_Normalized" or "report_generator"
    log_dir : str
        Directory for log files (created if absent)
    capture_stdout : bool
        Whether to tee stdout into CSV log
    capture_stderr : bool
        Whether to tee stderr into CSV log

    Returns
    -------
    CsvLogger
        The active logger instance
    """
    csv_log = CsvLogger(script_name, log_dir=log_dir)

    if capture_stdout:
        # Unwrap existing TeeToLogger to prevent nesting
        raw_stdout = sys.stdout
        while isinstance(raw_stdout, TeeToLogger):
            raw_stdout = raw_stdout._original
        csv_log._original_stdout = raw_stdout
        sys.stdout = TeeToLogger(raw_stdout, csv_log, stream_name="STDOUT")

    if capture_stderr:
        raw_stderr = sys.stderr
        while isinstance(raw_stderr, TeeToLogger):
            raw_stderr = raw_stderr._original
        csv_log._original_stderr = raw_stderr
        sys.stderr = TeeToLogger(raw_stderr, csv_log, stream_name="STDERR")

    csv_log.info(
        f"Pipeline started: {script_name}",
        event_type="CONFIG",
        phase="startup",
        context={
            "run_id": csv_log.run_id,
            "run_date": csv_log.run_date,
            "log_file": csv_log.log_filename,
            "python_version": sys.version,
        },
    )
    return csv_log
