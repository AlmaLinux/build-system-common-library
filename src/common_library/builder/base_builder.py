# -*- mode:python; coding:utf-8; -*-
# author: Vasily Kleschov <vkleschov@cloudlinux.com>
# created: 2017-12-27

"""Basic class for all other builders"""

import copy
import datetime
import os
import shutil
import tarfile
import traceback
from functools import wraps
from tempfile import TemporaryDirectory

import yaml

from common_library.builder.mock.mock_config import (
    MockBindMountPluginConfig,
    MockChrootFile,
    MockConfig,
)
from common_library.builder.mock.yum_config import (
    YumConfig,
    YumRepositoryConfig,
)
from common_library.builder.models import Task
from common_library.errors import LockError
from common_library.utils.git_utils import (
    MirroredGitRepo,
    WrappedGitRepo,
    git_checkout,
    git_get_commit_id,
    git_submodule_init,
    git_submodule_update,
)


def measure_stage(stage):
    """
    Records a stage start and end time.

    Parameters
    ----------
    stage : str
        Stage name.

    Returns
    -------

    """

    def wrapper(fn):
        @wraps(fn)
        def wrapped(self, *args, **kwargs):
            start_time = datetime.datetime.utcnow()
            try:
                return fn(self, *args, **kwargs)
            except Exception as e:
                print(str(e))
                traceback.print_exc()
                raise e
            finally:
                end_time = datetime.datetime.utcnow()
                self._build_stats[stage] = {
                    'start_ts': str(start_time),
                    'end_ts': str(end_time),
                    'delta': str(end_time - start_time),
                }

        return wrapped

    return wrapper


class BaseBuilder():

    def __init__(
        self,
        config,
        logger,
        task,
        task_dir,
        artifacts_dir,
    ):
        """
        Builder initialization.

        Parameters
        ----------
        config : BuildSlaveConfig
            Build slave configuration object.
        logger : logging.Logger
            Current build thread logger.
        task : Task or dict
            Build task.
        task_dir : str
            Build task working directory.
        artifacts_dir : str
            Build artifacts (src-RPM, RPM(s), logs, etc) output directory.
        """
        self.config = config
        self.logger = logger
        self.task = task
        self.task_dir = task_dir
        self.artifacts_dir = artifacts_dir
        # created git tag name
        self.created_tag = None
        self._build_stats = {}
        if isinstance(task, Task):
            self._pre_build_hook_target_arch = self.config.base_arch
        else:
            self.builder_kwargs = (
                task['build'].get('builder', {}).get('kwargs', {})
            )
            if self.config.arm64_support:
                self._pre_build_hook_target_arch = 'aarch64'
            else:
                self._pre_build_hook_target_arch = 'x86_64'

    @staticmethod
    def repo_has_submodules(git_sources_dir: str) -> bool:
        return os.path.exists(os.path.join(git_sources_dir, '.gitmodules'))

    def checkout_git_sources(
        self,
        git_sources_dir,
        ref,
        ref_type,
        uri,
        **kwargs,
    ):
        """
        Checkouts a project sources from the specified git repository.

        Parameters
        ----------
        git_sources_dir : str
            Target directory path.
        ref : str
            Git (gerrit) reference.
        ref_type : str
            Git (gerrit) reference type (e.g. gerrit_change).
        uri : str
            Git repository URL.
        kwargs : dict
            Optional keyword arguments list. It should be removed after a
            build job structure refactoring because we don't need other
            information in the build["git"] item's field.

        Returns
        -------
        WrappedGitRepo
            Git repository wrapper.
        """

        self.logger.info(
            'checking out {0} {1} from {2}'.format(ref_type, ref, uri)
        )
        if ref_type == 'gerrit_change':
            WrappedGitRepo.clone_from(uri, git_sources_dir, depth=1)
            repo = WrappedGitRepo(git_sources_dir)
            repo.fetch(uri, ref)
            git_checkout(git_sources_dir, 'FETCH_HEAD')
        else:
            try:
                with MirroredGitRepo(
                    uri,
                    self.config.git_repos_cache_dir,
                    self.config.git_cache_locks_dir,
                    timeout=60,
                ) as cached_repo:
                    repo = cached_repo.clone_to(git_sources_dir)
                    repo.checkout(ref)
            except LockError:
                self.logger.error(
                    'cannot acquire lock of cached repo, '
                    'cloning it from remote source instead'
                )
                WrappedGitRepo.clone_from(uri, git_sources_dir, depth=1)
                repo = WrappedGitRepo(git_sources_dir)
                repo.fetch(uri, ref)
                git_checkout(git_sources_dir, 'FETCH_HEAD')
        self.__log_commit_id(git_sources_dir)
        if self.repo_has_submodules(git_sources_dir):
            git_submodule_init(git_sources_dir, logger=self.logger)
            git_submodule_update(git_sources_dir, logger=self.logger)
        return repo

    def get_build_stats(self):
        """
        Returns build time statistics.

        Returns
        -------
        dict
            Dictionary where keys are build stage names and values are tuples
            of start and end time.
        """
        return copy.copy(self._build_stats)

    @staticmethod
    def init_artifacts_dir(task_dir):
        """
        Creates a build task artifacts output directory.

        Parameters
        ----------
        task_dir : str
            Build task working directory.

        Returns
        -------
        str
            Build artifacts directory path.
        """
        artifacts_dir = os.path.join(task_dir, 'artifacts')
        os.makedirs(artifacts_dir)
        return artifacts_dir

    @staticmethod
    def configure_npm_proxy(*args, **kwargs):
        raise NotImplementedError('NPM proxy setup is not implemented')

    def build(self):
        """
        Builds binary packages from sources. Actual implementation is unknown.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError('build method is not implemented')

    @staticmethod
    def _gen_pre_build_hook_profile(
        macros,
        platform,
        project_name,
        build_type=None,
    ):
        """
        Generates a bash profile with mock macro definitions for a pre-build
        hook environment.

        Parameters
        ----------
        macros : dict
            Mock macro definitions.
        platform : str
            Build system platform name.
        project_name : str
            Build system project name.
        build_type : str
            Build system build type.

        Returns
        -------
        MockChrootFile
            Bash profile chroot file.
        """
        profile = '#!/bin/bash\n'
        export_template = 'export {0}="{1}"\n'
        for name, value in macros.items():
            profile += export_template.format(name, value)
        profile += export_template.format('BUILD_PLATFORM', platform)
        profile += export_template.format('BUILD_PROJECT', project_name)
        if build_type:
            profile += export_template.format('BUILD_TYPE', build_type)
        return MockChrootFile('etc/profile.d/buildsys_vars.sh', profile)

    def _gen_pre_build_hook_yum_config(self):
        """
        Generates yum configuration based on AlmaLinux/AlmaLinux 8 stable
        for a pre-build hook chroot environment.

        Returns
        -------
        YumConfig
            Yum configuration.
        """

        # FIXME: Make repository configs in smarter way to avoid errors with
        #  package installation
        if self._pre_build_hook_target_arch in ('aarch64', 'x86_64'):
            arch = self._pre_build_hook_target_arch
        else:
            arch = 'x86_64'
        yum_repos = [
            YumRepositoryConfig(
                repositoryid='alma8-baseos',
                name='alma8-baseos',
                baseurl=f'https://alma-repo.atm.svcs.io/8/BaseOS/{arch}/os/',
            ),
            YumRepositoryConfig(
                repositoryid='alma8-appstream',
                name='alma8-appstream',
                baseurl=f'https://alma-repo.atm.svcs.io/8/AppStream/{arch}/os/',
            ),
            YumRepositoryConfig(
                repositoryid='alma8-powertools',
                name='alma8-powertools',
                baseurl=f'https://alma-repo.atm.svcs.io/8/PowerTools/{arch}/os/',
            ),
        ]
        if arch == 'x86_64':
            yum_repos.extend([
                YumRepositoryConfig(
                    repositoryid='cl8-ext',
                    name='cl8-ext',
                    baseurl='https://koji-proxy.atm.svcs.io/pub/cloudlinux/'
                    '8/cloudlinux-x86_64-server-8/x86_64/os/',
                ),
                YumRepositoryConfig(
                    repositoryid='bs-internal',
                    name='bs-internal',
                    baseurl=(
                        f'https://koji-proxy.atm.svcs.io/pub/cloudlinux/'
                        f'build_system/8/{arch}/'
                    ),
                ),
            ])
        return YumConfig(repositories=yum_repos)

    def _gen_pre_build_hook_mock_config(self, git_sources_dir):
        """
        Generates mock configuration for a pre-build hook chroot environment.

        Parameters
        ----------
        git_sources_dir : str
            Git repository path.

        Returns
        -------
        MockConfig
            Mock configuration.
        """
        target_arch = self._pre_build_hook_target_arch
        yum_config = self._gen_pre_build_hook_yum_config()
        chroot_setup_cmd = (
            'install bash bzip2 zlib coreutils cpio diffutils '
            'findutils gawk gcc gcc-c++ grep gzip info '
            'make patch redhat-rpm-config rpm-build sed shadow-utils tar '
            'unzip util-linux-ng which xz scl-utils scl-utils-build'
        )
        if target_arch == 'x86_64':
            chroot_setup_cmd += ' cloudlinux-release alt-dbpp'
        else:
            chroot_setup_cmd += ' almalinux-release'
        mock_config = MockConfig(
            target_arch=target_arch,
            dist='el8',
            releasever='8',
            chroot_setup_cmd=chroot_setup_cmd,
            use_bootstrap=False,
            use_bootstrap_image=False,
            use_nspawn=False,
            rpmbuild_networking=True,
            use_host_resolv=True,
            yum_config=yum_config,
            basedir=self.config.mock_basedir,
            cachedir=self.config.mock_cachedir,
            package_manager='dnf',
        )
        bind_plugin = MockBindMountPluginConfig(
            True, [(git_sources_dir, '/srv/pre_build/')]
        )
        mock_config.add_plugin(bind_plugin)
        macros = self.task['build'].get('definitions')
        platform = self.task['meta']['platform']
        project_name = self.task['build']['project_name']
        build_type = self.task['build']['build_type']
        mock_config.add_file(
            self._gen_pre_build_hook_profile(
                macros,
                platform,
                project_name,
                build_type,
            )
        )
        return mock_config

    def _get_pre_build_hook_deps(self, git_sources_dir):
        """
        Extracts a list of pre-build hook dependencies from a
        buildsys-pre-build.yml file located in the root of a repository.

        Parameters
        ----------
        git_sources_dir : str
            Git repository path.

        Returns
        -------
        list of str
            List of RPM package names to install before a pre-build hook
            execution.
        """
        config_path = os.path.join(git_sources_dir, 'buildsys-pre-build.yml')
        if not os.path.exists(config_path):
            return []
        with open(config_path, 'r') as fd:
            try:
                return yaml.safe_load(fd).get('dependencies', [])
            except Exception:
                self.logger.exception(
                    'Cannot parse pre-build hook dependencies',
                )
                return []

    def __log_commit_id(self, git_sources_dir):
        """
        Prints a current (HEAD) git repository commit id to a build log.

        Parameters
        ----------
        git_sources_dir : str
            Git repository path.
        """
        try:
            commit_id = git_get_commit_id(git_sources_dir)
            if isinstance(self.task, Task):
                self.task.ref.git_commit_hash = commit_id
            self.logger.info('git commit id: {0}'.format(commit_id))
        except Exception as e:
            msg = 'can not get git commit id: {0}. Traceback:\n{1}'
            self.logger.error(msg.format(str(e), traceback.format_exc()))

    def compress_custom_logs(self, files, dir_name, split_str):
        """
        Creates a tar.gz file with custom logs.

        Parameters
        ----------
        files : list of str
            Files that are going to be included in the resulting tar.gz
        dir_name : str
            Name of the directory where the log files are going to.
        split_str : str
            String that is going to be used when splitting the filepath
            into a shorter name.
        """
        tgz_name = f'{dir_name}.tar.gz'
        with TemporaryDirectory() as tmp_dir:
            dir_path = os.path.join(tmp_dir, dir_name)
            os.makedirs(dir_path)
            for f in files:
                # f_name is for:
                #   * replacing / with .
                #   * keep only the relevant path
                if split_str not in f:
                    continue
                f_name = f.replace('/', '.')[1:].split(split_str + '.')[1]
                f_dst = os.path.join(dir_path, f_name)
                shutil.copyfile(f, f_dst)
                # We delete the files already moved into the folder
                os.unlink(f)
            tgz_tmp = os.path.join(tmp_dir, tgz_name)
            tgz_dst = os.path.join(self.artifacts_dir, tgz_name)
            with tarfile.open(tgz_tmp, 'w:gz') as tgz:
                tgz.add(dir_path, arcname=os.path.basename(dir_path))
            try:
                os.link(tgz_tmp, tgz_dst)
            except OSError:
                shutil.copyfile(tgz_tmp, tgz_dst)

    @property
    def build_timeout(self):
        """
        Build timeout in seconds.

        Returns
        -------
        int or None
        """
        return self.task['build'].get('timeout')
