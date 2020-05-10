from .tuq import QueryTests
from pytests.fts.fts_base import CouchbaseCluster
from pytests.fts.fts_base import FTSIndex
from pytests.fts.random_query_generator.rand_query_gen import DATASET
import copy
from couchbase_helper.documentgenerator import JsonDocGenerator
from couchbase_helper.documentgenerator import WikiJSONGenerator
from fts.random_query_generator.rand_query_gen import FTSFlexQueryGenerator
from collections import Mapping, Sequence, Set, deque

class FlexIndexTests(QueryTests):

    users = {}

    def suite_setUp(self):
        super(FlexIndexTests, self).suite_setUp()


    def setUp(self):
        super(FlexIndexTests, self).setUp()
        #self._load_test_buckets()

        self.log.info("==============  FlexIndexTests setuAp has started ==============")
        self.log_config_info()
        self.dataset = self.input.param("flex_dataset", "emp")
        self.use_index_name_in_query = bool(self.input.param("use_index_name_in_query", True))
        self.expected_gsi_index_map = {}
        self.expected_fts_index_map = {}
        self.custom_map = self.input.param("custom_map", False)
        self.flex_query_option = self.input.param("flex_query_option", "flex_use_fts_query")
        self.log.info("==============  FlexIndexTests setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  FlexIndexTests tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  FlexIndexTests tearDown has completed ==============")
        super(FlexIndexTests, self).tearDown()

    def suite_tearDown(self):
        self.log.info("==============  FlexIndexTests suite_tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  FlexIndexTests suite_tearDown has completed ==============")
        super(FlexIndexTests, self).suite_tearDown()

# ============================ # Utils ===========================================

    def _load_emp_dataset(self, op_type="create", expiration=0, start=0,
                              end=1000):
        # Load Emp Dataset
        self.cluster.bucket_flush(self.master)

        if end > 0:
            self._kv_gen = JsonDocGenerator("emp_",
                                            encoding="utf-8",
                                            start=start,
                                            end=end)
            gen = copy.deepcopy(self._kv_gen)

            self._load_bucket(self.buckets[0], self.servers[0], gen, op_type,
                              expiration)

    def _load_wiki_dataset(self, op_type="create", expiration=0, start=0,
                          end=1000):
        # Load Emp Dataset
        #self.cluster.bucket_flush(self.master)

        if end > 0:
            self._kv_gen = WikiJSONGenerator("wiki_",
                                            encoding="utf-8",
                                            start=start,
                                            end=end)
            gen = copy.deepcopy(self._kv_gen)

            self._load_bucket(self.buckets[0], self.servers[0], gen, op_type,
                              expiration)

    def create_index(self, index_name, index_params=None,
                         plan_params=None, doc_count=1000):
        """
        Creates a default index given bucket, index_name and plan_params
        """

        if not self.custom_map:
            index_params = {
                "default_mapping": {
                    "enabled": True,
                    "dynamic": True,
                    "default_analyzer": "keyword"
                }
            }

        if not plan_params:
            plan_params = {'numReplicas': 0}
        fts_index = self.create_fts_index(
            name=index_name,
            source_name='default',
            index_params=index_params,
            plan_params=plan_params)
        indexed_doc_count = 0
        retry_count = 10
        while indexed_doc_count < doc_count and retry_count > 0:
            try:
                self.sleep(10)
                indexed_doc_count = fts_index.get_indexed_doc_count()
            except KeyError as k:
                continue
            retry_count -= 1

        if indexed_doc_count != doc_count:
            self.fail("FTS indexing did not complete. FTS index count : {0}, Bucket count : {1}".format(indexed_doc_count, doc_count))

        return fts_index

    def update_expected_index_map(self, fts_index):
        if not fts_index.smart_query_fields:
            fts_index.smart_query_fields = DATASET.FIELDS["emp"]
        for f, v in fts_index.smart_query_fields.items():
            for field in v:
                if field == "manages_team_size":
                    field = "manages.team_size"
                if field == "manages_reports":
                    field = "manages.reports"
                if field == "revision_timestamp":
                    field = "revision.timestamp"
                if field == "revision_text_text":
                    field = "`revision.text.#text`"
                if field == "revision_contributor_username":
                    field = "revision.contributor.username"
                if field not in self.expected_fts_index_map.keys():
                    self.expected_fts_index_map[field] = [fts_index.name]
                else:
                    self.expected_fts_index_map[field].append(fts_index.name)
        if "type" not in self.expected_fts_index_map.keys():
            self.expected_fts_index_map["type"] = [fts_index.name]
        else:
            self.expected_fts_index_map["type"].append(fts_index.name)
        self.log.info("expected_fts_index_map {0}".format(self.expected_fts_index_map))

    def create_fts_index(self, name, source_type='couchbase',
                         source_name=None, index_type='fulltext-index',
                         index_params=None, plan_params=None,
                         source_params=None, source_uuid=None):
        """Create fts index/alias
        @param node: Node on which index is created
        @param name: name of the index/alias
        @param source_type : 'couchbase' or 'files'
        @param source_name : name of couchbase bucket or "" for alias
        @param index_type : 'fulltext-index' or 'fulltext-alias'
        @param index_params :  to specify advanced index mapping;
                                dictionary overriding params in
                                INDEX_DEFAULTS.BLEVE_MAPPING or
                                INDEX_DEFAULTS.ALIAS_DEFINITION depending on
                                index_type
        @param plan_params : dictionary overriding params defined in
                                INDEX_DEFAULTS.PLAN_PARAMS
        @param source_params: dictionary overriding params defined in
                                INDEX_DEFAULTS.SOURCE_CB_PARAMS or
                                INDEX_DEFAULTS.SOURCE_FILE_PARAMS
        @param source_uuid: UUID of the source, may not be used
        """
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_index = FTSIndex(
            self.cbcluster,
            name,
            source_type,
            source_name,
            index_type,
            index_params,
            plan_params,
            source_params,
            source_uuid,
            self.dataset
        )
        fts_index.create()
        return fts_index

    def generate_random_queries(self, fts_index_fields=None, num_queries=1, query_type=["match"],
                                seed=0):
        """
         Calls FTS-FLex Query Generator for employee dataset
         @param num_queries: number of queries to return
         @query_type: a list of different types of queries to generate
                      like: query_type=["match", "match_phrase","bool",
                                        "conjunction", "disjunction"]
        """
        self.query_gen = FTSFlexQueryGenerator(num_queries, query_type=query_type,
                                        seed=seed, dataset=self.dataset,
                                        fields=fts_index_fields)

    def compare_results_with_gsi(self, flex_query, gsi_query):
        try:
            flex_result = self.run_cbq_query(flex_query)["results"]
            self.log.info("Number of results from flex query: {0} is {1}".format(flex_query, len(flex_result)))
        except Exception as e:
            self.log.info("Failed to run flex query: {0}".format(flex_query))
            self.log.error(e)
            return False

        try:
            gsi_result = self.run_cbq_query(gsi_query)["results"]
            self.log.info("Number of results from gsi query: {0} is {1}".format(gsi_query, len(gsi_result)))
        except Exception as e:
            self.log.info("Failed to run gsi query: {0}".format(gsi_query))
            self.log.error(e)
            return False

        if len(flex_result) != len(gsi_result):
            return False
        else:
            return True

    def get_gsi_fields_partial_sargability(self):
        fts_fields = self.query_gen.fields
        available_fields = DATASET.FIELDS["emp"]
        gsi_fields = {}
        for dt in available_fields.keys():
            print(dt)
            if dt is not "text":
                if dt in fts_fields.keys():
                    ff_dt = fts_fields[dt]
                    gsi_fields[dt] = []
                    for af in available_fields[dt]:
                        if af not in ff_dt:
                            gsi_fields[dt].append(af)
                else:
                    gsi_fields[dt] = available_fields[dt]
        print(gsi_fields)
        return gsi_fields

    def create_gsi_indexes(self, gsi_fields):
        count = 0
        self.expected_gsi_index_map = {}
        for k, v in gsi_fields.items():
            for field in v:
                if k == 'str' and (field == 'manages_reports' or field == 'languages_known'):
                    continue
                field_proxy = field
                if field == "manages_team_size":
                    field_proxy = "manages.team_size"
                    field = "manages.team_size"
                if field == "languages_known":
                    field = "ALL ARRAY v for v in languages_known END"
                if field == "manages_reports":
                    field = "ALL ARRAY v for v in manages.reports END"
                    field_proxy = "manages.reports"
                if field == "revision_timestamp":
                    field_proxy = "revision.timestamp"
                    field = "revision.timestamp"
                if field == "revision_text_text":
                    field_proxy = "`revision.text.#text`"
                    field = "`revision.text.#text`"
                if field == "revision_contributor_username":
                    field_proxy = "revision.contributor.username"
                    field = "revision.contributor.username"
                if field_proxy not in self.expected_gsi_index_map.keys() and field_proxy is not "type":
                    gsi_index_name = "gsi_index_" + str(count)
                    self.run_cbq_query("create index {0} on default({1})".format(gsi_index_name, field))
                    self.expected_gsi_index_map[field_proxy] = [gsi_index_name]
                    count += 1
        self.log.info("expected_gsi_index_map {0}".format(self.expected_gsi_index_map))

    def get_all_indexnames(self, response_json):
        queue = deque([response_json])
        index_names = []
        while queue:
            node = queue.popleft()
            nodevalue = node
            if type(node) is tuple:
                nodekey = node[0]
                nodevalue = node[1]

            if isinstance(nodevalue, Mapping):
                for k, v in nodevalue.items():
                    queue.extend([(k, v)])
            elif isinstance(nodevalue, (Sequence, Set)) and not isinstance(nodevalue, str):
                queue.extend(nodevalue)
            else:
                if nodekey == "index" and nodevalue not in index_names:
                    index_names.append(nodevalue)
        return index_names

    def check_if_expected_index_exist(self, result, expected_indexes):
        actual_indexes = self.get_all_indexnames(result)
        found = True
        self.log.info("Actual indexes present: {0}, Expected Indexes: {1}".format(sorted(actual_indexes), sorted(expected_indexes)))
        for index in actual_indexes:
            if index not in expected_indexes:
                found = False
        return found

    def get_expected_indexes(self, flex_query, expected_index_map):
        available_fields = DATASET.CONSOLIDATED_FIELDS
        expected_indexes = []
        for field in available_fields:
            if field == "manages_team_size":
                field = "manages.team_size"
            if field == "manages_reports":
                field = "manages.reports"
            if field == "revision_timestamp":
                field = "revision.timestamp"
            if field == "revision_text_text":
                field = "`revision.text.#text`"
            if field == "revision_contributor_username":
                field = "revision.contributor.username"
            if " {0}".format(field) in flex_query and field in expected_index_map.keys():
                for index in expected_index_map[field]:
                    expected_indexes.append(index)

        return list(set(expected_indexes))

    def run_queries_and_validate(self):
        iteration = 1
        failed_to_run_query = []
        not_found_index_in_response = []
        result_mismatch = []
        for flex_query_ph, gsi_query in zip(self.query_gen.fts_flex_queries, self.query_gen.gsi_queries):
            query_num = iteration
            iteration += 1
            self.log.info("======== Running Query # {0} =======".format(query_num))
            expected_fts_index = []
            expected_gsi_index = []
            if self.flex_query_option != "flex_use_gsi_query":
                expected_fts_index = self.get_expected_indexes(flex_query_ph, self.expected_fts_index_map)
            expected_gsi_index = self.get_expected_indexes(flex_query_ph, self.expected_gsi_index_map)
            flex_query = self.get_runnable_flex_query(flex_query_ph, expected_fts_index, expected_gsi_index)
            if self.flex_query_option == "flex_use_gsi_query":
                expected_gsi_index.append("primary_gsi_index")
            explain_query = "explain " + flex_query
            self.log.info("Query : {0}".format(explain_query))
            try:
                result = self.run_cbq_query(explain_query)
            except Exception as e:
                self.log.info("Failed to run query")
                self.log.error(e)
                failed_to_run_query.append(query_num)
                continue
            try:
                self.assertTrue(self.check_if_expected_index_exist(result, expected_fts_index + expected_gsi_index))
            except Exception as e:
                self.log.info("Failed to find fts index name in plan query")
                self.log.error(e)
                not_found_index_in_response.append(query_num)
                continue

            if not self.compare_results_with_gsi(flex_query, gsi_query):
                self.log.error("Result mismatch found")
                result_mismatch.append(query_num)

            self.log.info("======== Done =======")

        return failed_to_run_query, not_found_index_in_response, result_mismatch

    def combine_dict(self, smart_fields1, smart_fields2):
        combined_fields = {}
        for key in smart_fields1.keys():
            if key in smart_fields2.keys():
                combined_fields[key] = list(set(smart_fields1[key] + smart_fields2[key]))
            else:
                combined_fields[key] = smart_fields1[key]

        for key in smart_fields2.keys():
            if key not in smart_fields1.keys():
                combined_fields[key] = smart_fields2[key]
        return combined_fields

    def get_runnable_flex_query(self, flex_query_ph, expected_fts_index, expected_gsi_index):
        use_fts_hint = "USING FTS"
        use_gsi_hint = "USING GSI"
        final_hint = ""
        if self.flex_query_option == "flex_use_fts_query" or self.flex_query_option == "flex_use_fts_gsi_query":
            if self.use_index_name_in_query:
                for index in expected_fts_index:
                    if final_hint == "":
                        final_hint = "{0} {1}". format(index, use_fts_hint)
                    else:
                        final_hint = "{0}, {1} {2}".format(final_hint, index, use_fts_hint)
            else:
                final_hint = use_fts_hint

        if self.flex_query_option == "flex_use_gsi_query" or self.flex_query_option == "flex_use_fts_gsi_query":
            if self.use_index_name_in_query:
                for index in expected_gsi_index:
                    if final_hint == "":
                        final_hint = "{0} {1}". format(index, use_gsi_hint)
                    else:
                        final_hint = "{0}, {1} {2}".format(final_hint, index, use_gsi_hint)
            elif final_hint is not "":
                final_hint = "{0}, {1}".format(final_hint, use_gsi_hint)
            else:
                final_hint = use_gsi_hint

        flex_query = flex_query_ph.format(flex_hint=final_hint)

        return flex_query


# ======================== tests =====================================================

    def test_flex_single_typemapping(self):

        self._load_emp_dataset(end=self.num_items)

        fts_index = self.create_index(
            index_name="custom_index")
        self.update_expected_index_map(fts_index)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.generate_random_queries(fts_index.smart_query_fields)
        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

    def test_flex_multi_typemapping(self):

        self._load_emp_dataset(end=(self.num_items/2))
        self._load_wiki_dataset(end=(self.num_items/2))

        fts_index = self.create_index(
            index_name="custom_index")
        self.update_expected_index_map(fts_index)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.generate_random_queries(fts_index.smart_query_fields)
        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))


    def test_flex_default_typemapping(self):

        self._load_emp_dataset(end=self.num_items/2)
        self._load_wiki_dataset(end=(self.num_items/2))

        fts_index = self.create_index(
            index_name="default_index")
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.generate_random_queries()
        fts_index.smart_query_fields = self.query_gen.fields
        self.update_expected_index_map(fts_index)
        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))


    def test_flex_single_typemapping_partial_sargability(self):

        self._load_emp_dataset(end=self.num_items)

        fts_index = self.create_index(
            index_name="custom_index")
        self.update_expected_index_map(fts_index)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.generate_random_queries(fts_index.smart_query_fields)
        gsi_fields = self.get_gsi_fields_partial_sargability()
        self.create_gsi_indexes(gsi_fields)
        self.generate_random_queries()
        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

    def test_flex_multi_typemapping_partial_sargability(self):

        self._load_emp_dataset(end=(self.num_items/2))
        self._load_wiki_dataset(end=(self.num_items/2))

        fts_index = self.create_index(
            index_name="custom_index")
        self.update_expected_index_map(fts_index)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        self.generate_random_queries(fts_index.smart_query_fields)
        gsi_fields = self.get_gsi_fields_partial_sargability()
        self.create_gsi_indexes(gsi_fields)
        self.generate_random_queries()
        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))

    def test_flex_single_typemapping_2_fts_indexes(self):
        self._load_emp_dataset(end=self.num_items)

        fts_index_1 = self.create_index(
            index_name="custom_index_1")
        self.update_expected_index_map(fts_index_1)
        fts_index_2 = self.create_index(
            index_name="custom_index_2")
        self.log.info("Editing custom index with new map...")
        fts_index_2.generate_new_custom_map(seed=fts_index_2.cm_id+10)
        fts_index_2.index_definition['uuid'] = fts_index_2.get_uuid()
        fts_index_2.update()
        self.update_expected_index_map(fts_index_2)
        if not self.is_index_present("default", "primary_gsi_index"):
            self.run_cbq_query("create primary index primary_gsi_index on default")
        smart_fields = self.combine_dict(fts_index_1.smart_query_fields, fts_index_2.smart_query_fields)
        self.generate_random_queries(smart_fields)
        gsi_fields = self.get_gsi_fields_partial_sargability()
        self.create_gsi_indexes(gsi_fields)
        self.generate_random_queries()
        failed_to_run_query, not_found_index_in_response, result_mismatch = self.run_queries_and_validate()
        self.cbcluster.delete_all_fts_indexes()

        if failed_to_run_query or not_found_index_in_response or result_mismatch:
            self.fail("Found queries not runnable: {0} or required index not found in the query resonse: {1} "
                      "or flex query and gsi query results not matching: {2}"
                      .format(failed_to_run_query, not_found_index_in_response, result_mismatch))
        else:
            self.log.info("All {0} queries passed".format(len(self.query_gen.fts_flex_queries)))


