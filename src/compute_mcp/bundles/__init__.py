# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Versioned, deployable helper bundles shipped with the gateway.

Each subdirectory is a self-contained bundle that the gateway can upload to a
login node over the already-open route connection.  The contents are packaged
so the gateway deploys the exact revision it was built from.
"""
