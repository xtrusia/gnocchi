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
import os
import subprocess
from unittest import mock

from gnocchi.cli import metricd
from gnocchi.tests import base


class BinTestCase(base.BaseTestCase):
    @mock.patch('multiprocessing.set_start_method')
    @mock.patch.object(metricd, 'MetricdServiceManager')
    @mock.patch.object(metricd.service, 'prepare_service')
    def test_gnocchi_metricd_uses_fork(self, prepare_service,
                                       service_manager, set_start_method):
        conf = mock.Mock(stop_after_processing_metrics=0)
        prepare_service.return_value = conf

        metricd.metricd()

        set_start_method.assert_called_once_with('fork', force=True)
        service_manager.assert_called_once_with(conf)
        service_manager.return_value.run.assert_called_once_with()

    def test_gnocchi_config_generator_run(self):
        with open(os.devnull, 'w') as f:
            subp = subprocess.Popen(['gnocchi-config-generator'], stdout=f)
        self.assertEqual(0, subp.wait())
