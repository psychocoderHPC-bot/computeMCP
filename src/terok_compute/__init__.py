# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Terok Compute Gateway.

A trusted host-side daemon that owns SSH access to remote compute hosts and a
thin MCP server that runs inside a Terok container and talks only to the
gateway over an authenticated local endpoint.
"""

__version__ = "0.1.0"
