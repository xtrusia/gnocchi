# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import functools
import multiprocessing
import os
import signal
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest import mock

from gnocchi.cli import metricd

from gnocchi.tests import base


class _ForkserverMetricProcessor(metricd.MetricProcessor):
    def __init__(self, worker_id, *args):
        super(_ForkserverMetricProcessor, self).__init__(worker_id, *args)
        self._stop_event = threading.Event()

    def run(self):
        self._stop_event.wait()

    def terminate(self):
        self._stop_event.set()


def _forkserver_worker_ready(ready_event, *args):
    ready_event.set()


def _run_forkserver_probe():
    sys.argv = [sys.argv[0]]
    multiprocessing.set_start_method('forkserver')
    ctx = multiprocessing.get_context('forkserver')
    conf = metricd.prepare_service()
    manager = metricd.MetricdServiceManager(conf)
    service_id = manager.metric_processor_id
    manager._services[service_id].service = _ForkserverMetricProcessor
    ready_event = ctx.Event()
    manager.register_hooks(
        on_new_worker=functools.partial(_forkserver_worker_ready,
                                        ready_event))

    process = None
    try:
        manager._start_worker(service_id, 0)
        process = next(iter(manager._running_services[service_id]))
        if not ready_event.wait(5):
            raise RuntimeError('forkserver worker did not initialize')
    finally:
        if process is not None:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                    process.join()
            process.close()


class BinTestCase(base.BaseTestCase):
    def test_gnocchi_config_generator_run(self):
        with open(os.devnull, 'w') as f:
            subp = subprocess.Popen(['gnocchi-config-generator'], stdout=f)
        self.assertEqual(0, subp.wait())

    def test_metricd_forkserver_starts_worker(self):
        if 'forkserver' not in multiprocessing.get_all_start_methods():
            self.skipTest('forkserver is not available')
        cotyledon_utils = getattr(metricd.cotyledon, '_utils', None)
        if (cotyledon_utils is None or
                not hasattr(cotyledon_utils, 'spawn_process')):
            self.skipTest('cotyledon does not support forkserver workers')

        env = os.environ.copy()
        env['GNOCCHI_INDEXER_URL'] = 'sqlite://'
        subp = subprocess.Popen(
            [sys.executable, '-c',
             'from gnocchi.tests import test_bin; '
             'test_bin._run_forkserver_probe()'],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True)
        try:
            stdout, stderr = subp.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(subp.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = subp.communicate()
            self.fail('forkserver probe timed out\nstdout: %s\nstderr: %s' %
                      (stdout, stderr))

        self.assertEqual(
            0, subp.returncode,
            'forkserver probe failed\nstdout: %s\nstderr: %s' %
            (stdout, stderr))

    @mock.patch.object(metricd.service, 'prepare_service')
    def test_metricd_prepare_service_registers_options(self, prepare_service):
        conf = mock.Mock()
        prepare_service.return_value = conf
        option_groups = metricd.oslo_config_glue.list_opts()

        with mock.patch.object(metricd.cfg, 'ConfigOpts', return_value=conf):
            with mock.patch.object(metricd.oslo_config_glue, 'list_opts',
                                   return_value=option_groups):
                self.assertIs(conf, metricd.prepare_service())

        prepare_service.assert_called_once_with(conf=conf)
        cli_opts = conf.register_cli_opts.call_args.args[0]
        self.assertEqual('stop-after-processing-metrics', cli_opts[0].name)
        self.assertEqual([
            mock.call(options, group=group)
            for group, options in option_groups
        ], conf.register_opts.call_args_list)

    def test_metricd_worker_prepares_its_own_configuration(self):
        conf = mock.Mock()
        conf.metricd = SimpleNamespace(metric_reporting_delay=12)

        with mock.patch.object(metricd, 'prepare_service',
                               return_value=conf) as prepare_service:
            worker = metricd.MetricReporting(1)

        prepare_service.assert_called_once_with()
        self.assertIs(conf, worker.conf)
        self.assertEqual(12, worker.interval_delay)

    def test_metricd_manager_reload_and_worker_arguments(self):
        conf = mock.Mock()
        conf.graceful_shutdown_timeout = 17
        conf.metricd = SimpleNamespace(
            workers=3,
            metric_reporting_delay=1,
            metric_processing_delay=2,
            metric_cleanup_delay=3,
        )

        with mock.patch.object(metricd.cotyledon.ServiceManager, '__init__',
                               return_value=None) as manager_init:
            with mock.patch.object(metricd.cotyledon.ServiceManager, 'add',
                                   side_effect=['processor', 'reporting',
                                                'janitor']) as add:
                with mock.patch.object(metricd.cotyledon.ServiceManager,
                                       'register_hooks') as register_hooks:
                    with mock.patch.object(metricd.oslo_config_glue, 'setup') \
                            as setup:
                        manager = metricd.MetricdServiceManager(conf)

        manager_init.assert_called_once_with(graceful_shutdown_timeout=17)
        setup.assert_not_called()
        add.assert_has_calls([
            mock.call(metricd.MetricProcessor, workers=3),
            mock.call(metricd.MetricReporting),
            mock.call(metricd.MetricJanitor),
        ])
        register_hooks.assert_called_once_with(on_reload=manager.on_reload)

        manager._graceful_shutdown_timeout = 17
        manager.reconfigure = mock.Mock()

        def reload_config_files():
            self.assertEqual(17, manager._graceful_shutdown_timeout)
            conf.graceful_shutdown_timeout = 29
            conf.metricd.workers = 4

        conf.reload_config_files.side_effect = reload_config_files
        manager.on_reload()

        self.assertEqual(29, manager._graceful_shutdown_timeout)
        manager.reconfigure.assert_called_once_with('processor', workers=4)
