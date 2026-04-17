"""run_safe_script and the Safe Script Registry whitelist (SDD §8).

Enforces:

* Whitelist — only scripts registered in `SAFE_SCRIPT_REGISTRY` run.
* Path-traversal defence — the resolved script path must stay inside
  `SKILLS_BASE_PATH`.
* Subprocess isolation — the script runs in a fresh Python subprocess with a
  scrubbed environment (no API keys) and a restricted PATH.
* Timeout per script config.
* Structured stdout JSON contract; stderr captured to `last_stderr`.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional


# ── Default registry ───────────────────────────────────────────────────
SAFE_SCRIPT_REGISTRY: Dict[str, Dict[str, Any]] = {
    "transcript_summary": {
        "script": "transcript-summary/scripts/summarize.py",
        "timeout": 120,
        "max_memory_mb": 512,
    },
    "financial_extraction": {
        "script": "financial-extraction/scripts/extract.py",
        "timeout": 90,
    },
}


# Env variables that must NOT leak to script subprocesses.
SENSITIVE_ENV_PREFIXES = ("ANTHROPIC_", "OPENAI_", "GOOGLE_", "DATA_API_")
SENSITIVE_ENV_EXACT = {"API_KEY", "SECRET", "TOKEN"}

# Restricted PATH exposed to script subprocesses.
RESTRICTED_PATH = "/usr/bin:/usr/local/bin"


def _skills_base() -> Path:
    return Path(os.environ.get("SKILLS_BASE_PATH", "/app/skills")).resolve()


def _sanitise_env() -> Dict[str, str]:
    """Copy the current environment minus anything sensitive."""
    clean: Dict[str, str] = {}
    for k, v in os.environ.items():
        if any(k.startswith(p) for p in SENSITIVE_ENV_PREFIXES):
            continue
        if k.upper() in SENSITIVE_ENV_EXACT:
            continue
        clean[k] = v
    clean["PATH"] = RESTRICTED_PATH
    return clean


def run_safe_script(
    name: str,
    params: Optional[Dict[str, Any]] = None,
    input_file: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute a whitelisted script in a subprocess.

    Args:
        name: Registry key (must be in `SAFE_SCRIPT_REGISTRY`).
        params: Dict passed to the script as `--params <json>`.
        input_file: Optional path under the task workspace to pass through.

    Returns:
        `{"success": bool, "stdout": ..., "stderr": ..., "parsed": {...}}`
    """
    from . import get_services, get_workspace_dir

    services = get_services()

    if name not in SAFE_SCRIPT_REGISTRY:
        err = f"Script {name!r} not in registry"
        services.ctx.record_audit("run_safe_script", {"script_name": name}, False, 0, error=err)
        return {"success": False, "error": err}

    spec = SAFE_SCRIPT_REGISTRY[name]
    base = _skills_base()
    script_path = (base / spec["script"]).resolve()

    # Path-traversal defence
    try:
        script_path.relative_to(base)
    except ValueError:
        err = f"Resolved script path {script_path} escapes {base}"
        services.ctx.record_audit("run_safe_script", {"script_name": name}, False, 0, error=err)
        return {"success": False, "error": err}

    if not script_path.exists():
        err = f"Script not found: {script_path}"
        services.ctx.record_audit("run_safe_script", {"script_name": name}, False, 0, error=err)
        return {"success": False, "error": err}

    workspace_dir = get_workspace_dir()

    cmd = [
        sys.executable,
        str(script_path),
        "--workspace",
        workspace_dir,
        "--params",
        json.dumps(params or {}),
    ]
    if input_file:
        cmd.extend(["--input-file", input_file])

    env = _sanitise_env()
    timeout = int(spec.get("timeout", 120))

    import time as _time

    start = _time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        duration_ms = int((_time.perf_counter() - start) * 1000)
        services.ctx.record_audit(
            "run_safe_script", {"script_name": name, "cmd": shlex.join(cmd)},
            False, duration_ms, error=f"timeout after {timeout}s",
        )
        return {"success": False, "error": f"timeout after {timeout}s", "stderr": str(e)}

    duration_ms = int((_time.perf_counter() - start) * 1000)
    success = proc.returncode == 0

    parsed: Any = None
    if proc.stdout.strip():
        try:
            parsed = json.loads(proc.stdout)
        except json.JSONDecodeError:
            parsed = None

    services.ctx.record_audit(
        "run_safe_script",
        {"script_name": name, "returncode": proc.returncode},
        success, duration_ms,
        error=None if success else (proc.stderr[-200:] if proc.stderr else "non-zero exit"),
    )
    return {
        "success": success,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "parsed": parsed,
    }
