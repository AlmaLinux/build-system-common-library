# -*- mode:python; coding:utf-8; -*-
# author: Eugene Zamriy <ezamriy@cloudlinux.com>
# created: 2017-10-19

"""
Build System build thread implementation.
"""


import logging
import os
import threading

from common_library.utils.file_utils import clean_dir


class BaseSlaveBuilder(threading.Thread):
    """Build thread."""

    def __init__(
        self,
        thread_num,
    ):
        """
        Build thread initialization.

        Parameters
        ----------
        config : build_node.build_node_config.BuildNodeConfig
            Build node configuration object.
        thread_num : int
            Number of a build thread to construct a "unique" name.
        terminated_event : threading.Event
            Shows, if process got "kill -15" signal.
        graceful_terminated_event : threading.Event
            Shows, if process got "kill -10" signal.
        """
        super().__init__(name='Builder-{0}'.format(thread_num))

    @staticmethod
    def init_working_dir(working_dir):
        """
        Creates a non-existent working directory or cleans it up from previous
        builds.
        """
        if os.path.exists(working_dir):
            logging.debug('cleaning the %s working directory', working_dir)
            clean_dir(working_dir)
        else:
            logging.debug('creating the %s working directory', working_dir)
            os.makedirs(working_dir, 0o750)

    @staticmethod
    def init_thread_logger(log_file):
        """
        Build thread logger initialization.

        Parameters
        ----------
        log_file : str
            Log file path.

        Returns
        -------
        logging.Logger
            Build thread logger.
        """
        logger = logging.getLogger(
            'bt-{0}-logger'.format(threading.current_thread().name)
        )
        logger.handlers = []
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)-8s: " "%(message)s", "%H:%M:%S %d.%m.%y"
        )
        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
