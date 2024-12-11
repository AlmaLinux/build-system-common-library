# -*- mode:python; coding:utf-8; -*-
# author: Eugene Zamriy <ezamriy@cloudlinux.com>
# created: 2017-09-28

"""
mock environment wrapper.
"""

import logging
import os
import re

import plumbum

from common_library.utils.file_utils import (
    filter_files,
    rm_sudo,
    sudo_clear_immutable_attr,
)

__all__ = ['MockError', 'MockEnvironment', 'MockResult']


class MockResult():
    """Successful mock command execution result."""

    def __init__(
        self,
        command,
        exit_code,
        stdout,
        stderr,
        mock_config,
        resultdir=None,
    ):
        """
        Mock command execution result initialization.

        Parameters
        ----------
        command : str
            Executed mock command.
        exit_code : int
            Mock command exit code.
        stdout : str
            Mock command stdout.
        stderr : str
            Mock command stderr.
        mock_config : str
            Mock configuration file content.
        resultdir : str, optional
            Output directory.
        """
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.mock_config = mock_config
        self.resultdir = resultdir

    @property
    def rpms(self):
        """
        List of built RPM package paths in the resultdir.

        Returns
        -------
        list
        """
        if not self.resultdir:
            return []
        return filter_files(
            self.resultdir, lambda f: re.search(r'(?<!\.src)\.rpm$', f)
        )

    @property
    def srpm(self):
        """
        Built src-RPM package path in the resultdir.

        Returns
        -------
        str or None
        """
        if not self.resultdir:
            return None
        return next(
            iter(
                filter_files(self.resultdir, lambda f: f.endswith('src.rpm'))
            ),
            None,
        )

    @property
    def mock_logs(self):
        """
        List of mock log files in the resultdir.

        Returns
        -------
        list
        """
        if not self.resultdir:
            return []
        log_files = filter_files(self.resultdir, lambda f: f.endswith('.log'))
        chroot_dir = os.path.join(self.resultdir, 'chroot_scan')
        if os.path.exists(chroot_dir):
            for dir_name, _, files in os.walk(chroot_dir):
                for file in files:
                    if file.endswith('.log'):
                        log_files.append(
                            os.path.join(os.path.abspath(dir_name), file)
                        )
        return log_files


class MockError(Exception, MockResult):
    """Failed mock command execution result."""

    def __init__(
        self,
        command,
        exit_code,
        stdout,
        stderr,
        mock_config,
        resultdir=None,
        message=None,
    ):
        if not message:
            message = 'command "{0}" returned {1}'.format(command, exit_code)
        Exception.__init__(self, message)
        MockResult.__init__(
            self, command, exit_code, stdout, stderr, mock_config, resultdir
        )


class MockEnvironment():
    """mock environment."""

    def __init__(
        self,
        supervisor,
        config_path,
        root,
        mock_root=None,
        mock_cache_root=None,
        dirs_to_umount_on_clean=None,
    ):
        self.__log = logging.getLogger(self.__module__)
        self.__supervisor = supervisor
        self.__mock_root = mock_root or '/var/lib/mock'
        self.__mock_cache_root = mock_cache_root or '/var/cache/mock'
        if isinstance(config_path, bytes):
            config_path = config_path.decode('utf-8')
        if isinstance(root, bytes):
            root = root.decode('utf-8')
        self.__config_path = config_path
        self.__configdir = os.path.split(config_path)[0]
        self.__root = root
        self._dirs_to_umount_on_clean = dirs_to_umount_on_clean or [
            'proc',
            'sys',
            'dev/pts',
            'dev/ptmx',
            'dev/shm',
            'var/lve/dbgovernor-shm',
            'var/run/pesign',
        ]

    def __enter__(self):
        return self

    def clean(self):
        try:
            sudo_clear_immutable_attr(self.root_path)
        except Exception as e:
            self.__log.error(
                'Cannot run clear immutable attr in %s.\nStderr:\n%s',
                self.__root,
                e,
            )

        try:
            self.__unmount_sys_dirs()
        except Exception as e:
            self.__log.error(
                'Cannot run unmount sys dirs %s.\nStderr:\n%s',
                self.__root,
                e,
            )

        try:
            self.__execute_mock(cleanup_after=True, clean='')
        except MockError as e:
            self.__log.error(
                'Cannot run config %s clean. Stdout:\n%s\nStderr:\n%s',
                self.__root,
                e.stdout,
                e.stderr,
            )

    def buildsrpm(
        self,
        spec: str,
        sources: str,
        resultdir=None,
        definitions=None,
        timeout=None,
    ):
        """
        Builds an src-RPM package from the specified spec file and sources.

        Parameters
        ----------
        spec : str
            Spec file path.
        sources : str
            Sources directory path.
        resultdir : str, optional
            Output directory.
        definitions : dict, optional
            RPM macro definitions to pass to the mock process (see `--define`
            argument description in the mock manual).
        timeout : int, optional
            RPM build timeout in seconds.

        Returns
        -------
        MockResult
            mock command execution result.

        Raises
        ------
        MockError
            If mock command returned a non-zero exit code or resultdir wasn't
            found in the mock output.

        Notes
        -----
        See mock(1) man page for --buildsrpm command description.
        """
        return self.__execute_mock_with_result(
            buildsrpm='',
            spec=spec,
            sources=sources,
            resultdir=resultdir,
            definitions=definitions,
            rpmbuild_timeout=timeout,
        )

    def init(self, resultdir=None):
        fn = (
            self.__execute_mock_with_result
            if resultdir
            else self.__execute_mock
        )
        return fn(init='', resultdir=resultdir)

    def install(self, package, resultdir=None):
        """
        Installs the specified package into the mock chroot.

        Parameters
        ----------
        package : str
            Package name.
        resultdir : str, optional
            Output directory.

        Returns
        -------
        MockResult
            mock command execution result.

        Raises
        ------
        MockError
            If mock command returned a non-zero exit code or resultdir wasn't
            found in the mock output.

        Notes
        -----
        See mock(1) man page for --install command description.
        """
        return self.__execute_mock_with_result(
            install=package,
            resultdir=resultdir,
            verbose='',
        )

    def rebuild(
        self,
        srpm_path,
        resultdir=None,
        definitions=None,
        timeout=None,
    ):
        """
        Executes a `mock --rebuild` command in the mock environment.

        Parameters
        ----------
        srpm_path : str
            Source RPM path.
        resultdir : str, optional
            Output directory.
        definitions : dict, optional
            RPM macro definitions to pass to the mock process (see `--define`
            argument description in the mock manual).
        timeout : int, optional
            RPM build timeout in seconds.

        Returns
        -------
        MockResult
            mock command execution result.

        Raises
        ------
        MockError
            If mock command returned a non-zero exit code or resultdir wasn't
            found in the mock output.

        Notes
        -----
        See mock(1) man page for --rebuild command description.
        """
        return self.__execute_mock_with_result(
            rebuild=srpm_path,
            resultdir=resultdir,
            definitions=definitions,
            timeout=timeout,
        )

    def shell(
        self,
        command,
        resultdir=None,
        timeout=None,
    ):
        """
        Executes the specified shell command interactively.

        Parameters
        ----------
        command : str
            Shell command.
        resultdir : str, optional
            Output directory.
        timeout : int, optional
            Passed to plumbum. Kill process if the timeout exceeded. In seconds.
        Returns
        -------
        MockResult
            mock command execution result.

        Raises
        ------
        MockError
            If mock command returned a non-zero exit code or resultdir wasn't
            found in the mock output.

        Notes
        -----
        See mock(1) man page for --shell command description.
        """
        return self.__execute_mock_with_result(
            shell=command,
            resultdir=resultdir,
            timeout=timeout,
        )

    def copyin(self, src, dst):
        """
        Copies source(s) into the chroot.

        Parameters
        ----------
        src : str or list
            Source path(s).
        dst : str
            Target path.
        """
        args = []
        if isinstance(src, str):
            args.append(src)
        else:
            args.extend(src)
        args.append(dst)
        self.__execute_mock(copyin=args)

    def scrub(self, scrub_type):
        """
        Executes a `mock scrub=${scrub_type}` command in the mock environment.

        Parameters
        ----------
        scrub_type : str
            One of "all", "chroot", "cache", "root-cache", "c-cache" or
            "yum-cache". See `man mock` for details.
        """
        remove_message_template = 'Removing mock %s directory for %s'
        if any([
            os.path.exists(self.config_path),
            os.path.exists(self.root_path),
            os.path.exists(self.cache_path),
            os.path.exists(self.bootstrap_root_path),
            os.path.exists(self.bootstrap_cache_path),
        ]):
            try:
                sudo_clear_immutable_attr(self.root_path)
            except Exception as e:
                self.__log.error(
                    'Cannot run clear immutable attr in %s.\nStderr:\n%s',
                    self.__root,
                    e,
                )

            try:
                self.__unmount_sys_dirs()
            except Exception as e:
                self.__log.error(
                    'Cannot run unmount sys dirs %s.\nStderr:\n%s',
                    self.__root,
                    e,
                )

            try:
                self.__execute_mock(cleanup_after=True, scrub=scrub_type)
            except MockError as e:
                self.__log.error(
                    'Cannot run config %s scrub. Stdout:\n%s\n' 'Stderr:\n%s',
                    self.__root,
                    e.stdout,
                    e.stderr,
                )
        if scrub_type == 'all':
            if os.path.exists(self.root_path):
                self.__log.debug(remove_message_template, 'root', self.__root)
                rm_sudo(self.root_path)
            if os.path.exists(self.cache_path):
                self.__log.debug(remove_message_template, 'cache', self.__root)
                rm_sudo(self.cache_path)
            if os.path.exists(self.bootstrap_root_path):
                self.__log.debug(
                    remove_message_template, 'bootstrap root', self.__root
                )
                rm_sudo(self.bootstrap_root_path)
            if os.path.exists(self.bootstrap_cache_path):
                self.__log.debug(
                    remove_message_template, 'bootstrap cache', self.__root
                )
                rm_sudo(self.bootstrap_cache_path)

    def __unmount_sys_dirs(self):
        cmd = plumbum.local['sudo']
        for sys_dir in self._dirs_to_umount_on_clean:
            umount_path = os.path.join(self.root_path, 'root', sys_dir)
            if os.path.exists(umount_path):
                args = (
                    'umount',
                    '-fR',
                    os.path.join(self.root_path, 'root', sys_dir),
                )
                cmd.run(args=args, retcode=None)

    def __execute_mock(self, cleanup_after=False, timeout=None, **kwargs):
        """
        Executes mock with the given command line options.

        Returns
        -------
        tuple
            Executed mock command, its exit code, stdout, stderr and output
            directory (resultdir).

        Raises
        ------
        MockError
            If mock returned a non-zero exit code.
        """
        mock = plumbum.local['mock']
        args = ['--configdir', self.__configdir, '--root', self.__root]
        if self.__log.getEffectiveLevel() <= logging.DEBUG:
            args.append('--verbose')

        resultdir = None
        for option, value in kwargs.items():
            if value is None:
                continue
            elif option == 'definitions':
                for macro_name, macro_value in value.items():
                    args.append('--define')
                    args.append('{0} {1}'.format(macro_name, macro_value))
            elif option == 'copyin':
                args.append('--copyin')
                args.extend(value)
            else:
                args.append('--{0}'.format(option))
                args.append(str(value))
                if option == 'resultdir':
                    resultdir = value
        args = [x for x in args if x]
        if not cleanup_after:
            args.append('--no-cleanup-after')
        command = 'mock {0}'.format(' '.join(args))
        self.__log.debug(
            'executing the following mock command: {0}'.format(command)
        )
        # NOTE: we don't want to spam bash history with mock commands,
        #       LANG definition is required to always receive mock output in
        #       English so that we can parse it.
        with plumbum.local.env(HISTFILE='/dev/null', LANG='C'):
            exit_code, stdout, stderr = mock.run(
                args=args,
                retcode=None,
                timeout=timeout,
            )
            if not resultdir:
                resultdir = self.__parse_mock_resultdir(stderr)
            if exit_code != 0:
                raise MockError(
                    command,
                    exit_code,
                    stdout,
                    stderr,
                    self.config,
                    resultdir=resultdir,
                )
            return command, exit_code, stdout, stderr, resultdir
        raise MockError(
            command,
            -1,
            '',
            '',
            self.config,
            resultdir=resultdir,
            message='unexpected mock execution error',
        )

    def __execute_mock_with_result(self, timeout=None, **kwargs):
        """
        Executes mock with the given command line options and returns its
        execution result.
        Parameters
        ----------
        timeout : int, optional
            Passed to plumbum. Kill process if the timeout exceeded. In seconds.

        Returns
        -------
        MockResult
            Mock command execution result.

        Raises
        ------
        MockError
            If mock returned a non-zero exit code or resultdir wasn't found
            in the mock output.
        """
        (
            command,
            exit_code,
            stdout,
            stderr,
            resultdir,
        ) = self.__execute_mock(
            timeout=timeout,
            **kwargs,
        )
        try:
            sudo_clear_immutable_attr(self.root_path)
        except Exception as e:
            self.__log.error(
                'Cannot run clear immutable attr in %s.\nStderr:\n%s',
                self.__root,
                e,
            )
        if not resultdir:
            raise MockError(
                command,
                exit_code,
                stdout,
                stderr,
                self.config,
                message='resultdir is not found in the mock ' 'output',
            )
        return MockResult(
            command,
            exit_code,
            stdout,
            stderr,
            self.config,
            resultdir=resultdir,
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__supervisor.free_environment(self)

    def __parse_mock_resultdir(self, output):
        """
        Extracts a results directory from the given mock output.

        Parameters
        ----------
        output : str
            mock output.

        Returns
        -------
        str or None
            mock results directory.
        """
        for regex in (
            r'^INFO:\s+Results\s+and/or\s+logs\s+in:\s+(.*)$',
            r'^DEBUG:\s+resultdir\s+=\s+(.*)$',
        ):
            re_rslt = re.search(regex, output, re.MULTILINE)
            if re_rslt:
                return re_rslt.group(1)

    @property
    def config_path(self):
        return self.__config_path

    @property
    def root_path(self):
        return os.path.join(self.__mock_root, self.__root)

    @property
    def bootstrap_root_path(self):
        return os.path.join(self.__mock_root, f'{self.__root}-bootstrap')

    @property
    def cache_path(self):
        return os.path.join(self.__mock_cache_root, self.__root)

    @property
    def bootstrap_cache_path(self):
        return os.path.join(self.__mock_cache_root, f'{self.__root}-bootstrap')

    @property
    def config(self):
        if not os.path.exists(self.__config_path):
            return ''
        with open(self.__config_path, 'r') as conf:
            return conf.read()
