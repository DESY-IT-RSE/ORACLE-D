# SPDX-License-Identifier: Apache-2.0
# Copyright 2023-2026 Deutsches Elektronen Synchrotron DESY 
#                     and the University of Glasgow
# Authors: Dwayne Spiteri and Gordon Stewart.
# For more information about rights and fair use please refer to src/Main.py.
# For full detailed and legal infomration please read the LICENSE and NOTICE
#    files in the main directory 
# ===========================================================================

import datetime
import logging
import os


def __get_fn_log():
    dir_logs = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(dir_logs):
        os.mkdir(dir_logs)
    tag = '{:%Y%m%d-%H%M}'.format(datetime.datetime.now())
    return os.path.join(dir_logs, f'{tag}.log')


def get_logger():
    return logging.getLogger('CLUSTERSIM')


def configure_logger(logger, level = logging.INFO):
    logger.setLevel(level)

    handler = logging.FileHandler(__get_fn_log())
    handler.setFormatter(logging.Formatter('%(asctime)s  %(levelname)s  [%(filename)s:%(funcName)s]  %(message)s'))
    logger.addHandler(handler)