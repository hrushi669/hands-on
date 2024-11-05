# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Atlan Pte. Ltd.
import time
import logging

# logging.basicConfig(level=logging.INFO, filename="lineage.logs", filemode='a', format="%(message)s")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def time_it(action):
    start = time.time()
    o = action()
    elapsed_time = time.time() - start
    logger.info(f"Time taken: {elapsed_time * 1000:.0f} ms")
    return o
