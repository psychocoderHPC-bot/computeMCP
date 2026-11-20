# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Hidden terminal prompt for a per-request second factor."""

from __future__ import annotations

import getpass
import sys


class FactorPromptError(RuntimeError):
    """The operator could not be prompted for a second factor."""


def prompt_factor(prompt: str = "2FA token: ") -> str:
    """Read a second factor from the terminal without echoing it.

    The token is never accepted on the command line or read from a pipe.  A
    non-interactive stdin is rejected instead of falling back to echoed input.
    """
    if not sys.stdin.isatty():
        raise FactorPromptError(
            "--2fa requires an interactive terminal; the token is not read "
            "from a pipe or command line"
        )
    try:
        factor = getpass.getpass(prompt)
    except (EOFError, KeyboardInterrupt):
        raise FactorPromptError("2FA prompt cancelled") from None
    if not factor:
        raise FactorPromptError("no 2FA token supplied")
    return factor
