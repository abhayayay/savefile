import json
import random
from Queue import Queue
from threading import Thread
import time

from BucketLib.BucketOperations import BucketHelper
from CbasLib.CBASOperations import CBASHelper
from CbasLib.cbas_entity import Dataverse, Synonym, CBAS_Index
from bucket_utils.bucket_ready_functions import DocLoaderUtils
from cbas.cbas_base import CBASBaseTest
from collections_helper.collections_spec_constants import MetaCrudParams
from security.rbac_base import RbacBase
from Jython_tasks.task import RunQueriesTask, CreateDatasetsTask, DropDatasetsTask
from cbas_utils.cbas_utils import CBASRebalanceUtil


class CBASStandaloneCollections(CBASBaseTest):

    def setUp(self):

        super(CBASStandaloneCollections, self).setUp()

        self.iterations = int(self.input.param("iterations", 1))

        # Since all the test cases are being run on 1 cluster only
        self.cluster = self.cb_clusters.values()[0]

        self.run_concurrent_query = self.input.param("run_query", False)
        self.parallel_load_percent = int(self.input.param(
            "parallel_load_percent", 0))
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.setUp.__name__)

    def tearDown(self):
        self.log_setup_status(self.__class__.__name__, "Started",
                              stage=self.tearDown.__name__)
        super(CBASStandaloneCollections, self).tearDown()
        self.log_setup_status(self.__class__.__name__, "Finished",
                              stage=self.tearDown.__name__)

    def setup_for_test(self, update_spec={}):
        wait_for_ingestion = (not self.parallel_load_percent)
        if self.cbas_spec_name:
            self.cbas_spec = self.cbas_util.get_cbas_spec(
                self.cbas_spec_name)
            if update_spec:
                self.cbas_util.update_cbas_spec(
                    self.cbas_spec, update_spec)
            cbas_infra_result = self.cbas_util.create_cbas_infra_from_spec(
                self.cluster, self.cbas_spec, self.bucket_util,
                wait_for_ingestion=wait_for_ingestion)
            if not cbas_infra_result[0]:
                self.fail(
                    "Error while creating infra from CBAS spec -- " +
                    cbas_infra_result[1])

    def create_dataverse(self, parts_in_dv_name=1, dataverse_name=""):
        dataverse_name = dataverse_name
        error_name = ""
        if parts_in_dv_name > 1:
            for i in range(1, parts_in_dv_name):
                name = self.cbas_util.generate_name(
                    name_cardinality=1, max_length=255,
                    fixed_length=True)
                if not error_name:
                    error_name = name
                dataverse_name += "{0}.".format(name)
        dataverse_name = dataverse_name.strip(".")
        dataverse_name = CBASHelper.format_name(dataverse_name)
        if not self.cbas_util.create_dataverse(
                cluster=self.cluster, dataverse_name=dataverse_name,
                validate_error_msg=False,
                expected_error=None,
                expected_error_code=None):
            self.fail(
                "Failed to create dataverse: {}".format(
                    dataverse_name))
        return dataverse_name

    def test_create_standalone_collection(self):
        """
        This testcase verifies dataset creation.
        Supported Test params -
        :testparam cardinality int, accepted values are between 1-3
        :testparam bucket_cardinality int, accepted values are between 1-3
        :testparam invalid_kv_collection, boolean
        :testparam invalid_kv_scope, boolean
        :testparam invalid_dataverse, boolean
        :testparam name_length int, max length of dataverse name
        :testparam no_dataset_name, boolean
        :testparam error str, error msg to validate.
        :testparam validate_error boolean
        :testparam cbas_collection boolean
        """
        self.log.info("Test started")
        dataverse_name = None
        if self.input.param('dv_name', '') or self.input.param('parts_in_dv_name', 1) > 1:
            self.create_dataverse(parts_in_dv_name=self.input.param('parts_in_dv_name', 1),
                                  dataverse_name=self.input.param('dv_name', ''))
        self.cbas_util.create_standalone_obj(
            collection_name=self.input.param('col_name', None),
            name_cardinality=self.input.param('cardinality', 1),
            no_of_obj=self.input.param('no_of_obj', 1),
            name_length=self.input.param('name_length', 30),
            fixed_length=self.input.param('fixed_length', False),
            dataverse_name=dataverse_name,
            storage_format=self.input.param('storage_format', None))

        collection = self.cbas_util.list_all_dataset_objs()[0]
        # Negative scenario
        if self.input.param('error', None):
            error_msg = self.input.param('error', None)
        else:
            error_msg = None
        if self.input.param('invalid_dataverse', False):
            collection.name = "invalid." + collection.name
            collection.dataverse_name = ""
            error_msg = error_msg.format("invalid")
        elif self.input.param('no_dataset_name', False):
            collection.name = ''
        # Negative scenario ends
        key_autogenerated = self.input.param('key_autogenerated', False)

        if not self.cbas_util.create_standalone_collection(
                self.cluster, collection.name, self.input.param('key_field', ''),
                self.input.param('key_field_type', ''),
                collection.dataverse_name,
                validate_error_msg=self.input.param('validate_error_msg', False),
                expected_error=error_msg, key_autogenerated=key_autogenerated):
            self.fail("Dataset creation failed")

        if not self.input.param('validate_error_msg', False):
            if not self.cbas_util.validate_dataset_in_metadata(
                    self.cluster, collection.name, collection.dataverse_name):
                self.fail("Dataset entry not present in Metadata.Dataset")
        self.log.info("Test finished")
