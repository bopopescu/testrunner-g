from tuq import QueryTests
from membase.api.exception import CBQError
from lib.membase.api.rest_client import RestConnection
from pytests.fts.fts_base import CouchbaseCluster
from remote.remote_util import RemoteMachineShellConnection
import json
from pytests.security.rbac_base import RbacBase
from lib.remote.remote_util import RemoteMachineShellConnection
import threading

class N1qlFTSIntegrationPhase2Test(QueryTests):

    users = {}

    def suite_setUp(self):
        super(N1qlFTSIntegrationPhase2Test, self).suite_setUp()

        self._load_test_buckets()

    def setUp(self):
        super(N1qlFTSIntegrationPhase2Test, self).setUp()

        self.log.info("==============  N1qlFTSIntegrationPhase2Test setup has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2Test setup has completed ==============")

    def tearDown(self):
        self.log.info("==============  N1qlFTSIntegrationPhase2Test tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2Test tearDown has completed ==============")
        super(N1qlFTSIntegrationPhase2Test, self).tearDown()


    def suite_tearDown(self):
        self.log.info("==============  N1qlFTSIntegrationPhase2Test suite_tearDown has started ==============")
        self.log_config_info()
        self.log.info("==============  N1qlFTSIntegrationPhase2Test suite_tearDown has completed ==============")
        super(N1qlFTSIntegrationPhase2Test, self).suite_tearDown()


# ======================== tests =====================================================

    def test_keyspace_alias_single_bucket(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name='beer-sample')

        bucket_names = {
            "no_alias": "`beer-sample`",
            "alias": "`beer-sample` t",
            "as_alias": "`beer-sample` as t"
        }
        test_name = self.input.param("test_name", '')
        bucket_name = bucket_names[test_name]
        aliases = []
        if bucket_name == "`beer-sample`":
            aliases = ["", "`beer-sample`"]
        else:
            aliases = ["t"]

        try:
            for alias in aliases:
                dot = ""
                if alias!="":
                    dot="."
                fts_query = "select "+str(alias)+str(dot)+"code, "+str(alias)+str(dot)+"state from "+str(bucket_name)+" " \
                            "where "+str(alias)+str(dot)+"type='brewery' and SEARCH("+alias+dot+"state, 'state:California') order by "+str(alias)+str(dot)+"code"
                n1ql_query = "select code, state from `beer-sample` where type='brewery' and state like '%California%' order by code"

                fts_results = self.run_cbq_query(fts_query)['results']
                n1ql_results = self.run_cbq_query(n1ql_query)['results']

                self.assertEquals(fts_results, n1ql_results, "Incorrect query : "+str(fts_query))

        finally:
            self._remove_all_fts_indexes()



    def test_keyspace_alias_two_buckets(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        test_cases = {
            'test_t1_t2_1': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "t1"},
            'test_t1_t2_2': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "`t1`"},
            'test_t1_t2_3': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "`t1`.state"},
            'test_t1_t2_4': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "t1.`state`"},
            'test_t1_t2_5': {"bucket1_alias": "t1", "bucket2_alias": "t2", "keyspace_param": "`t1`.`state`"},
            'test_ast1_t2_1': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "t1"},
            'test_ast1_t2_2': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "`t1`"},
            'test_ast1_t2_3': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "`t1`.state"},
            'test_ast1_t2_4': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "t1.`state`"},
            'test_ast1_t2_5': {"bucket1_alias": "as t1", "bucket2_alias": "t2", "keyspace_param": "`t1`.`state`"},
            'test_t1_ast2_1': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "t1"},
            'test_t1_ast2_2': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`"},
            'test_t1_ast2_3': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`.state"},
            'test_t1_ast2_4': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "t1.`state`"},
            'test_t1_ast2_5': {"bucket1_alias": "t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`.`state`"},
            'test_ast1_ast2_1': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "t1"},
            'test_ast1_ast2_2': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`"},
            'test_ast1_ast2_3': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`.state"},
            'test_ast1_ast2_4': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "t1.`state`"},
            'test_ast1_ast2_5': {"bucket1_alias": "as t1", "bucket2_alias": "as t2", "keyspace_param": "`t1`.`state`"},
        }

        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name='beer-sample')

        bucket1_alias = test_cases[test_name]["bucket1_alias"]
        bucket2_alias = test_cases[test_name]["bucket2_alias"]
        keyspace_alias = test_cases[test_name]['keyspace_param']

        self.run_cbq_query("create index idx_brewery_id on `beer-sample`(brewery_id)")
        self.run_cbq_query("create index idx_type on `beer-sample`(type)")
        self.run_cbq_query("create index idx_code on `beer-sample`(code)")
        self.wait_for_all_indexes_online()

        fts_query = "select t1.code, t1.state, t1.city, t2.name from `beer-sample` "+bucket1_alias+ \
                    " inner join `beer-sample` "+bucket2_alias+" on t1.code=t2.brewery_id where t1.type='brewery' and t2.type='beer' " \
                    "and SEARCH("+keyspace_alias+", 'state:California') order by t1.code, t2.name"
        n1ql_query = "select t1.code, t1.state, t1.city, t2.name from `beer-sample` t1 inner join " \
                        "`beer-sample` t2 on t1.code=t2.brewery_id where t1.type='brewery' " \
                        " and t2.type='beer' and t1.state like '%California%' order by t1.code, t2.name"
        fts_results = None
        n1ql_results = None
        try:
            fts_results = self.run_cbq_query(fts_query)['results']
            n1ql_results = self.run_cbq_query(n1ql_query)['results']
        except CBQError, err:
            self._remove_all_fts_indexes()
            raise Exception("Query: "+fts_query+" is failed.")

        self._remove_all_fts_indexes()
        self.drop_index_safe('beer-sample', 'idx_brewery_id')
        self.drop_index_safe('beer-sample', 'idx_type')
        self.drop_index_safe('beer-sample', 'idx_code')

        self.assertEquals(fts_results, n1ql_results, "Incorrect query : "+str(fts_query))



    def test_keyspace_alias_two_buckets_negative(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        test_cases = {
            "test1": {"bucket_name": "`beer-sample` t1", "search_alias": "`state`"},
            "test2": {"bucket_name": "`beer-sample` t1", "search_alias": "state"},
            "test3": {"bucket_name": "`beer-sample` as t1", "search_alias": "state"},
            "test4": {"bucket_name": "`beer-sample` as t1", "search_alias": "`state`"},
        }
        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name='beer-sample')
        bucket_name = test_cases[test_name]["bucket_name"]
        search_alias = test_cases[test_name]["search_alias"]

        fts_query = "select t1.code, t1.state, t1.city, t2.name from "+bucket_name+" inner join `beer-sample` t2 on t1.code=t2.brewery_id " \
                    "where t1.type='brewery' and t2.type='beer' and SEARCH("+search_alias+", 'state:California') order by t2.name"
        try:
            self.run_cbq_query(fts_query)
        except CBQError, err:
            self._remove_all_fts_indexes()
            self.assertTrue("Ambiguous reference to field" in str(err), "Unexpected error message is found - "+str(err))

        self._remove_all_fts_indexes()


    def test_keyspace_alias_1_bucket_negative(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        test_cases = {
            "star": "t.state[*]",
            "object_values": "OBJECT_VALUES(t.state)",
            "array": "t.[state]"
        }
        search_alias = test_cases[test_name]

        fts_query = "select t.code, t.state from `beer-sample` t where t.type='brewery' and SEARCH("+search_alias+", 'France') order by t.code"
        try:
            self.run_cbq_query(fts_query)
        except CBQError, ex:
            self._remove_all_fts_indexes()
            self.assertTrue("Ambiguous reference to field" in str(ex), "Unexpected error message is found - "+str(ex))

        self._remove_all_fts_indexes()

    #MB-33876
    def test_search_options_index_name(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        test_cases = {
            "index_not_exists": { # MB-33876
                "expected_result": "fail"
            },
            "single_fts_index": {
                "expected_result": "success",
                "index_in_explain": "idx_beer_sample_fts"
            },
            "two_fts_indexes": {
                "expected_result": "success",
                "index_in_explain": "idx_beer_sample_fts"
            },
            "fts_index_is_not_optimal": {
                "expected_result": "success",
                "index_in_explain": "idx_beer_sample_fts"
            }
        }

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"}, {\"index\":\"idx_beer_sample_fts\"})"
        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name='beer-sample')

        if test_name == "index_not_exists":
            self._delete_fts_index(index_name="idx_beer_sample_fts")
        else:
            if test_name == "two_fts_indexes":
                self._create_fts_index(index_name='idx_beer_sample_fts_1', doc_count=7303, source_name='beer-sample')
            elif test_name == "fts_index_is_not_optimal":
                more_suitable_index = self._create_fts_index(index_name='idx_beer_sample_fts_name', doc_count=7303,
                                                             source_name='beer-sample')
                more_suitable_index.add_child_field_to_default_mapping(field_name="name", field_type="text")
                more_suitable_index.index_definition['uuid'] = more_suitable_index.get_uuid()
                more_suitable_index.update()
        if test_cases[test_name]["expected_result"] == "fail":
            result = self.run_cbq_query(fts_query)
            self.assertEquals(result['status'], "errors", "Running SEARCH() query without fts index is failed.")
        elif test_cases[test_name]["expected_result"] == "success":
            result = self.run_cbq_query("explain " + fts_query)
            self._remove_all_fts_indexes()
            self.assertEquals(result['results'][0]['plan']['~children'][0]['index'], test_cases[test_name]["index_in_explain"])

        self._remove_all_fts_indexes()

    # 10 results problem
    def test_search_options(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name='idx_beer_sample_fts', doc_count=7303, source_name='beer-sample')
        test_cases = {
            # 10 results
            "explain": ["true", "false"],
            # 10 results
            "fields": ["[\"*\"]", "[\"name\"]"],
            # 10 results
            "highlight": ["{\"style\":\"html\", \"fields\":[\"*\"]}", "{\"style\":\"html\", \"fields\":[\"name\"]}", "{\"style\":\"ansi\", \"fields\":[\"name\"]}", "{\"style\":\"ansi\", \"fields\":[\"*\"]}"],
            # 10 results
            "analyzer": ["{\"match\": \"California\", \"field\": \"state\", \"analyzer\": \"standard\"}", "{\"match\": \"California\", \"field\": \"state\", \"analyzer\": \"html\"}"],
            # 10 results
            "size": [10, 100],
            # 10 results
            "sort": ["[{\"by\": \"field\", \"field\": \"name\", \"mode\":\"max\", \"missing\": \"last\"}]"],
            # 10 results
            "query": ["{\"match\": \"California\", \"field\": \"state\"}", "{\"match\": \"Texas\", \"field\": \"state\"}"]
        }

        for option_val in test_cases[test_name]:
            n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"}, {\""+test_name+"\": "+str(option_val)+"})"
            fts_request = "'{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"size\":10000, \""+test_name+"\":"+str(option_val)+"}'"
            n1ql_results = self.run_cbq_query(n1ql_query)['results']
            fts_results = self._run_fts_request(request=fts_request)

            self._remove_all_fts_indexes()

            comparison_result = self._compare_n1ql_results_against_fts(n1ql_results, fts_results)
            self.assertEquals(comparison_result, "OK", comparison_result)


    def test_use_index_hint(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)

        test_cases = {
            "fts_index_exists" : {
                "hint_content" : "idx_beer_sample_fts USING FTS",
                "expected_result" : "positive",
                "options_content": ""
            },
            "fts_index_does_not_exist" : {
                "hint_content" : "idx_beer_sample_fts_fake USING FTS",
                "expected_result" : "negative",
                "options_content": ""
            },
            "fts_index_busy" : {
                "hint_content" : "idx_beer_sample_fts USING FTS",
                "expected_result" : "positive",
                "options_content": ""
            },
            "fts_gsi_indexes_use" : {
                "hint_content" : "idx_beer_sample_fts USING FTS, beer_primary using GSI",
                "expected_result" : "positive",
                "options_content": ""
            },
            "same_hint_options" : {
                "hint_content" : "idx_beer_sample_fts USING FTS",
                "expected_result" : "positive",
                "options_content" : ", {\"index\":\"idx_beer_sample_fts\"}"
            },
            "not_same_hint_options" : {
                "hint_content" : "idx_beer_sample_fts USING FTS",
                "expected_result" : "positive",
                "options_content" : ", {\"index\":\"idx_beer_sample_fts_1\"}"
            },
            "hint_good_options_bad": {
                "hint_content": "idx_beer_sample_fts USING FTS",
                "expected_result": "negative",
                "options_content": ", {\"index\":\"idx_beer_sample_fts_fake\"}"
            },
            "hint_bad_options_good": {
                "hint_content": "idx_beer_sample_fts_fake USING FTS",
                "expected_result": "negative",
                "options_content": ", {\"index\":\"idx_beer_sample_fts\"}"
            },
            "hint_bad_options_bad": {
                "hint_content": "idx_beer_sample_fts_fake USING FTS",
                "expected_result": "negative",
                "options_content": ", {\"index\":\"idx_beer_sample_fts_fake\"}"
            },
        }

        try:
            test_results = {}
            test_passed = True
            negatives_expected = 0
            negatives_found = 0

            self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

            if test_name == "not_same_hint_options":
                self._create_fts_index(index_name='idx_beer_sample_fts_1', doc_count=7303, source_name='beer-sample')

            test_case_dict = test_cases[test_name]
            options_content = test_case_dict['options_content']
            if test_case_dict['expected_result'] == "negative":
                negatives_expected = 1

            n1ql_query = "select meta().id from `beer-sample` USE INDEX ("+test_case_dict['hint_content']+") " \
                            "where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"}"+options_content+")"
            try:
                n1ql_explain_query = "explain " + n1ql_query
                self.run_cbq_query(n1ql_query)
                result = self.run_cbq_query(n1ql_explain_query)
                if test_name == "not_same_hint_options":
                    self.assertTrue(result['results'][0]['plan']['~children'][0]['index'] in ["idx_beer_sample_fts", "idx_beer_sample_fts_1"])
                else:
                    self.assertEquals(result['results'][0]['plan']['~children'][0]['index'], "idx_beer_sample_fts")
            except CBQError, e:
                negatives_found = 1
                test_passed = False

            test_results[test_name] = test_passed

            if test_name == "not_same_hint_options":
                self._delete_fts_index(index_name='idx_beer_sample_fts_1')

        finally:
            self._remove_all_fts_indexes()

        self.assertEquals(negatives_found, negatives_expected, "Some test case results differ from expected.")


    def test_index_selection(self):
        # gsi indexes - primary, secondary - field, seconadary - field,field
        # fts indexes - default, field, type->field

        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        self.run_cbq_query("create index idx_state on `beer-sample`(state)")
        self.run_cbq_query("create index idx_state_city on `beer-sample`(state, city)")
        self.wait_for_all_indexes_online()

        more_suitable_fts_index = self._create_fts_index(index_name='idx_beer_sample_fts_name', doc_count=7303, source_name='beer-sample')
        more_suitable_fts_index.add_child_field_to_default_mapping(field_name="state", field_type="text")
        more_suitable_fts_index.index_definition['uuid'] = more_suitable_fts_index.get_uuid()
        more_suitable_fts_index.update()

        test_cases = {
            # index specified in SEARCH() or in USE INDEX hint must be used
            "use_index_fts":{
                "query": "explain select meta().id from `beer-sample` USE INDEX (idx_beer_sample_fts using fts) where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"})",
                "index": "idx_beer_sample_fts"
            },
            # MB-33677
            "use_index_gsi": {
                "query": "explain select meta().id from `beer-sample` USE INDEX (idx_state_city using gsi) where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"})",
                "index": "idx_state_city"
            },
            "search_hint":{
                "query": "explain select meta().id from `beer-sample` where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"}, {\"index\":\"idx_beer_sample_fts\"})",
                "index": "idx_beer_sample_fts"
            },
            "shortest_fts": {
                "query": "explain select meta().id from `beer-sample` where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"})",
                "index": "idx_beer_sample_fts_name"
            },
            # MB-33678
            "shortest_gsi":{
                "query": "explain select meta().id from `beer-sample` where search(`beer-sample`, {\"field\": \"name\", \"match\":\"California\"})",
                "index": "idx_name"
            },
            "primary_gsi":{
                "query": "explain select meta().id from `beer-sample` where search(`beer-sample`, {\"field\": \"category\", \"match\":\"British\"})",
                "index": "PrimaryScan3"
            }
        }

        if test_name == "shortest_gsi":
            self._delete_fts_index("idx_beer_sample_fts")
            self.run_cbq_query("create index idx_name on `beer-sample`(name)")
            self.run_cbq_query("create index idx_state_name on `beer-sample`(state, name)")
            self.wait_for_all_indexes_online()
        if test_name == "primary_gsi":
            self._delete_fts_index("idx_beer_sample_fts")

        n1ql_query = test_cases[test_name]["query"]
        result = self.run_cbq_query(n1ql_query)
        if test_name == "primary_gsi":
            self.assertEquals(result['results'][0]['plan']['~children'][0]['#operator'], "PrimaryScan3")
        else:
            self.assertEquals(result['results'][0]['plan']['~children'][0]['index'], test_cases[test_name]["index"])


        self._remove_all_fts_indexes()
        self.drop_index_safe('beer-sample', 'idx_state')
        self.drop_index_safe('beer-sample', 'idx_state_city')
        self.drop_index_safe('beer-sample', 'idx_name')
        self.drop_index_safe('beer-sample', 'idx_state_name')

    # 10 results
    def test_logical_predicates(self):
        test_cases = [" = true ", " in [true] ", " in [true, true, true] "]

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        fts_request = "'{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"size\":10000}'"

        for test_case in test_cases:
            n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"}) "+ test_case

            n1ql_results = self.run_cbq_query(n1ql_query)['results']
            fts_results = self._run_fts_request(request=fts_request)
            comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, fts_results)
            self.assertEquals(comparison_results, "OK", comparison_results)

        n1ql_query = "select meta().id from `beer-sample` where not(not(search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"}))) "
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        fts_results = self._run_fts_request(request=fts_request)
        comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, fts_results)
        self.assertEquals(comparison_results, "OK", comparison_results)

        self._remove_all_fts_indexes()

    def test_logical_predicates_negative(self):
        test_cases = [" = false ", "  !=false ", " in [false] ", " in [true, 1, 2] ", " not in [false] ",
                      " = \'xyz\' ", " <= \'xyz\' ", " >= \'xyz\' ", " > \'xyz\' ", " < \'xyz\' ", " like \'xyz\' ", " not like \'xyz\' "]

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        fts_request = "'{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"size\":10000}'"

        for test_case in test_cases:
            n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"}) "+ test_case
            n1ql_results = self.run_cbq_query(n1ql_query)['results']
            fts_results = self._run_fts_request(request=fts_request)
            comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, fts_results)
            self.assertEquals(comparison_results, "OK", comparison_results)

            result = self.run_cbq_query(n1ql_query)
            self.assertEquals(result['status'], "success")

        n1ql_query = "select meta().id from `beer-sample` where not(search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"})) "
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        fts_results = self._run_fts_request(request=fts_request)
        comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, fts_results)
        self.assertEquals(comparison_results, "OK", comparison_results)

        self._remove_all_fts_indexes()

    def test_n1ql_syntax_select_from_let(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        n1ql_query = "select meta().id from `beer-sample` let res=true where search(`beer-sample`, {\"field\": \"state\", \"match\":\"California\"})=res"
        fts_request = "'{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"size\":10000}'"
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        fts_results = self._run_fts_request(request=fts_request)
        comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, fts_results)
        self.assertEquals(comparison_results, "OK", comparison_results)

        self._remove_all_fts_indexes()

    def test_n1ql_syntax_select_from_2_buckets(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        self.run_cbq_query("create index idx_state on `beer-sample`(state)")
        self.run_cbq_query("create index idx_city on `beer-sample`(city)")
        self.wait_for_all_indexes_online()

        n1ql_query = "select `beer-sample`.id, `beer-sample`.country, `beer-sample`.city, t2.name from `beer-sample` " \
                     "inner join `beer-sample` t2 on `beer-sample`.state=t2.state and `beer-sample`.city=t2.city " \
                     "where SEARCH(`beer-sample`, 'state:California')"
        n1ql_results = self.run_cbq_query(n1ql_query)
        self.assertEquals(n1ql_results['status'], 'success')

        self._remove_all_fts_indexes()
        self.drop_index_safe('beer-sample', 'idx_state')
        self.drop_index_safe('beer-sample', 'idx_city')


    def test_n1ql_syntax_select_from_double_search_call(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        self.run_cbq_query("create index idx_state on `beer-sample`(state)")
        self.run_cbq_query("create index idx_city on `beer-sample`(city)")
        self.wait_for_all_indexes_online()

        n1ql_query = "select `beer-sample`.id, `beer-sample`.country, `beer-sample`.city, t2.name from `beer-sample` " \
                     "inner join `beer-sample` t2 on `beer-sample`.state=t2.state and `beer-sample`.city=t2.city " \
                     "where SEARCH(t2, 'state:California') and SEARCH(`beer-sample`, 'state:California')"
        n1ql_results = self.run_cbq_query(n1ql_query)
        self.assertEquals(n1ql_results['status'], 'success')

        self._remove_all_fts_indexes()
        self.drop_index_safe('beer-sample', 'idx_state')
        self.drop_index_safe('beer-sample', 'idx_city')


    def test_n1ql_syntax_from_select(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        n1ql_query = "from (select meta().id mt from `beer-sample` where search(`beer-sample`, 'state:California')) as t select t.mt as id"
        fts_request = "'{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"size\":10000}'"
        n1ql_results = self.run_cbq_query(n1ql_query)['results']
        fts_results = self._run_fts_request(request=fts_request)
        comparison_results = self._compare_n1ql_results_against_fts(n1ql_results, fts_results)
        self.assertEquals(comparison_results, "OK", comparison_results)

        self._remove_all_fts_indexes()


    def test_n1ql_syntax_union_intersect_except(self):
        test_cases = {
            "same_buckets_same_idx": {
                "query_left":  "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California') ",
                "query_right":  " select meta().id from `beer-sample` where search(`beer-sample`, 'state:Georgia')"
            },
            "same_buckets_different_idx": {
                "query_left": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California') ",
                "query_right": " select meta().id from `beer-sample` where search(`beer-sample`, 'name:Amendment')"
            },
            "different_buckets_different_idx": {
                "query_left": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California') ",
                "query_right": " select meta().id from `default` where search(`default`, 'job_title:Engeneer')"
            }
        }

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_name_index =  self._create_fts_index(index_name="idx_beer_sample_fts_name", doc_count=7303, source_name='beer-sample')
        fts_name_index.add_child_field_to_default_mapping(field_name="name", field_type="text")
        fts_name_index.index_definition['uuid'] = fts_name_index.get_uuid()
        fts_name_index.update()

        fts_state_index =  self._create_fts_index(index_name="idx_beer_sample_fts_state", doc_count=7303, source_name='beer-sample')
        fts_state_index.add_child_field_to_default_mapping(field_name="state", field_type="text")
        fts_state_index.index_definition['uuid'] = fts_state_index.get_uuid()
        fts_state_index.update()

        fts_job_index =  self._create_fts_index(index_name="idx_default_fts_job_title", doc_count=2016, source_name='default')
        union_intersect_except = [" union ", " intersect ", " except "]
        test_name = self.input.param("test_name", '')

        for uie in union_intersect_except:
            full_results = self.run_cbq_query(test_cases[test_name]['query_left']+uie+test_cases[test_name]['query_right'])['results']
            left_results = self.run_cbq_query(test_cases[test_name]['query_left'])['results']
            right_results = self.run_cbq_query(test_cases[test_name]['query_right'])['results']
            left_right_results = []
            if uie == ' union ':
                left_right_results = left_results
                for r in right_results:
                    if r not in left_right_results:
                        left_right_results.append(r)
            elif uie == ' intersect ':
                for r in left_results:
                    if r in right_results and r not in left_right_results:
                        left_right_results.append(r)
            elif uie == ' except ':
                for r in left_results:
                    if r not in right_results:
                        left_right_results.append(r)

            self.assertEquals(len(full_results), len(left_right_results),
                        "Results count does not match for test "+test_name+", operation - "+uie+". Full query - " + str(
                            len(full_results)) + ", sum of 2 queries - " + str(len(left_right_results)))
            self.assertEquals(sorted(full_results), sorted(left_right_results),
                        "Found mismatch in results for test "+test_name+", operation - "+uie+".")

        self._remove_all_fts_indexes()


    def test_prepareds(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        test_cases = {
            "simple_prepared": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California')",
                "params": "",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California')",
                "expected_result": "success"
            },
            # MB-33724
            "named_prepared_query_definition": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:$state_val')",
                "params": "$state_val=\"California\"",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California')",
                "expected_result": "success"
            },
            # MB-33724
            "named_prepared_option_index_name": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'index': '$idx_name'})",
                "params": "$idx_name=\"idx_beer_sample_fts\"",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'index': 'idx_beer_sample_fts'})",
                "expected_result": "success"
            },
            "named_prepared_option_settings": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'size': $size})",
                "params": "$size=15",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'size': 15})",
                "expected_result": "success"
            },
            "named_prepared_option_out": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'out': $out_val})",
                "params": "$out=\"out_values\"",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'out': 'out_values'})",
                "expected_result": "cannot_prepare"
            },
            # MB-33724
            "positional_prepared_query_definition": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:$1')",
                "params": "args=[\"California\"]",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California')",
                "expected_result": "success"
            },
            # MB-33724
            "positional_prepared_option_index_name": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'index': '$1'})",
                "params": "args=[\"idx_beer_sample_fts\"]",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'index': 'idx_beer_sample_fts'})",
                "expected_result": "cannot_execute"
            },
            "positional_prepared_option_settings": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'size': $1})",
                "params": "args=[15]",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'size': 15})",
                "expected_result": "success"
            },
            "positional_prepared_option_out": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'out': $1})",
                "params": "args=[\"out_values\"]",
                "n1ql": "",
                "expected_result": "cannot_prepare"
            }
        }

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_name_index = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        self.run_cbq_query("delete from system:prepareds")

        #1 create prepared
        create_prepared = "prepare "+test_name+" from "+test_cases[test_name]['prepared']
        if test_cases[test_name]["expected_result"] == "cannot_prepare":
            try:
                self.run_cbq_query(create_prepared)
            except CBQError, err:
                self.assertEquals(True, True)
                return
        else:
            self.run_cbq_query(create_prepared)

        #2 call prepared
        call_query = "execute "+test_name
        if test_cases[test_name]["params"]!="":
            call_query = call_query + "&" + test_cases[test_name]["params"]

        prepared_results = self.run_cbq_query_curl(query="'"+call_query+"'")['results']

        #3 compare to n1ql query
        n1ql_results = self.run_cbq_query(test_cases[test_name]['n1ql'])['results']

        prepared_doc_ids = []
        for result in prepared_results:
            prepared_doc_ids.append(result['id'])

        n1ql_doc_ids = []
        for result in n1ql_results:
            n1ql_doc_ids.append(result['id'])

        self.assertEquals(len(n1ql_doc_ids), len(prepared_doc_ids),
                            "Results count does not match for test . N1QL - " + str(
                                len(n1ql_doc_ids)) + ", Prepareds - " + str(len(prepared_doc_ids)))
        self.assertEquals(sorted(prepared_doc_ids), sorted(n1ql_doc_ids),
                            "Found mismatch in results for test .")

        self._remove_all_fts_indexes()

    def test_parameterized_queries(self):
        test_name = self.input.param("test_name", '')
        if test_name == '':
            raise Exception("Invalid test configuration! Test name should not be empty.")

        test_cases = {
            # MB-33724
            "named_prepared_query_definition": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, \"state:$state_val\")",
                "params": "$state_val=\"California\"",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California')",
                "expected_result": "success"
            },
            # MB-33724
            "named_prepared_option_settings": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, \"state:California\", {\"size\": $size})",
                "params": "$size=15",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'size': 15})",
                "expected_result": "success"
            },
            "named_prepared_option_out": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, \"state:California\", {\"out\": $out_val})",
                "params": "$out=\"out_values\"",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'out': 'out_values'})",
                "expected_result": "success"
            },
            "positional_prepared_query_definition": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, \"state:$1\")",
                "params": "args=[\"California\"]",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California')",
                "expected_result": "success"
            },
            "positional_prepared_option_index_name": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, \"state:California\", {\"index\": \"$1\"})",
                "params": "args=[\"idx_beer_sample_fts\"]",
                "n1ql": "",
                "expected_result": "success"
            },
            "positional_prepared_option_settings": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, \"state:California\", {\"size\": $1})",
                "params": "args=[15]",
                "n1ql": "select meta().id from `beer-sample` where search(`beer-sample`, 'state:California', {'size': 15})",
                "expected_result": "success"
            },
            "positional_prepared_option_out": {
                "prepared": "select meta().id from `beer-sample` where search(`beer-sample`, \"state:California\", {\"out\": $1})",
                "params": "args=[\"out_values\"]",
                "n1ql": "",
                "expected_result": "success"
            }
        }

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_name_index = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        if test_cases[test_name]['expected_result'] == "success":
            call_query = test_cases[test_name]['prepared']
            if test_cases[test_name]["params"]!="":
                call_query = call_query + "&" + test_cases[test_name]["params"]
            prepared_results = self.run_cbq_query_curl(query="'"+call_query+"'")['results']
            #3 compare to n1ql query
            n1ql_results = self.run_cbq_query(test_cases[test_name]['n1ql'])['results']

            prepared_doc_ids = []
            for result in prepared_results:
                prepared_doc_ids.append(result['id'])

            n1ql_doc_ids = []
            for result in n1ql_results:
                n1ql_doc_ids.append(result['id'])

            self.assertEquals(len(n1ql_doc_ids), len(prepared_doc_ids),
                                "Results count does not match for test . N1QL - " + str(
                                    len(n1ql_doc_ids)) + ", Prepareds - " + str(len(prepared_doc_ids)))
            self.assertEquals(sorted(prepared_doc_ids), sorted(n1ql_doc_ids),
                              "Found mismatch in results for test .")

        self._remove_all_fts_indexes()

    def test_rbac(self):
        user = self.input.param("user", '')
        if user == '':
            raise Exception("Invalid test configuration! User name should not be empty.")

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_name_index = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        self._create_all_users()

        username = self.users[user]['username']
        password = self.users[user]['password']
        query = "select meta().id from `beer-sample` where search(`beer-sample`, \"state:California\")"

        master_result = self.run_cbq_query(query=query, server=self.master, username=username, password=password)
        self.assertEquals(master_result['status'], 'success', username+" query run failed on non-fts node")

        self._remove_all_fts_indexes()

    # 10 results in fts
    def test_sorting_pagination(self):
        query = "select meta().id from `beer-sample` where search(`beer-sample`, \"state:California\")"
        # inner sort modes: asc, desc
        # inner sort fields: single field, multiple fields, score, id
        # missing values: first, last
        # mode: min, max, offset
        inner_sorting_field_values = ["", "city"]
        inner_sorting_order_values = ["", "min", "max"]
        inner_offset_values = ["", "10"]
        outer_sorting_field_values = ["", "city", "name"]
        outer_sorting_order_values = ["", "asc", "desc"]
        outer_offset_values = ["", "10"]

        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_name_index = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        for inner_field in inner_sorting_field_values:
            for inner_order in inner_sorting_order_values:
                for inner_offset in inner_offset_values:
                    for outer_field in outer_sorting_field_values:
                        for outer_order in outer_sorting_order_values:
                            for outer_offset in outer_offset_values:
                                inner_sort_expression = ""
                                if inner_field != "":
                                    inner_sort_expression = ", \"sort\": [{\"by\": \"field\", \"field\": \""+inner_field+"\""
                                    if inner_order != "":
                                        inner_sort_expression = inner_sort_expression + ", \"mode\": \""+inner_order+"\""
                                    if inner_offset != "":
                                        inner_sort_expression = inner_sort_expression + ", \"offset\": "+inner_offset+""
                                    inner_sort_expression = inner_sort_expression + "}]"

                                outer_sort_expression = ""
                                if outer_field != "":
                                    outer_sort_expression = "order by "+outer_field +" "+outer_order
                                if outer_offset != "":
                                    outer_sort_expression = outer_sort_expression + " offset "+outer_offset

                                search_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"query\": {\"field\": \"state\", \"match\": \"California\"}"+inner_sort_expression+"}) "+outer_sort_expression
                                search_results = self.run_cbq_query(search_query)['results']
                                if outer_sort_expression == "":
                                    if inner_sort_expression != "":
                                        fts_request = "'{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"size\":1000,"+inner_sort_expression+"}'"
                                    else:
                                        fts_request = "'{\"query\":{\"field\": \"state\", \"match\":\"California\"}, \"size\":10000}'"
                                    fts_results = self._run_fts_request(request=fts_request)
                                    comparison_results = self._compare_n1ql_results_against_fts(search_results, fts_results)
                                    self.assertEquals(comparison_results, "OK", comparison_results)
                                else:
                                    n1ql_query = "select meta().id from `beer-sample` where search(`beer-sample`, {\"query\": {\"field\": \"state\", \"match\": \"California\"}}) "+outer_sort_expression
                                    n1ql_results = self.run_cbq_query(n1ql_query)['results']
                                    self.assertEquals(search_results, n1ql_results)

        self._remove_all_fts_indexes()


    def test_scan_consistency(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_name_index = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')

        scan_val = self.input.param("scan_type", '')
        count_before_update = self.run_cbq_query("select count(*) from `beer-sample` where search(`beer-sample`, \"state:California\")")['results'][0]
        self.scan_consistency = scan_val

        update_query = "update `beer-sample` set state='Califffornia' where meta().id in ( select raw meta().id from `beer-sample` b where search(b, {\"query\": {\"field\": \"state\", \"match\": \"California\"}, \"sort\": [{\"by\": \"field\", \"field\": \"city\"}]}))"
        select_query = "select meta().id from `beer-sample` where search(`beer-sample`, \"state:California\")"

        threads = []
        t = threading.Thread(target=self._update_parallel, args=(update_query, "UPDATE", count_before_update['$1'], scan_val))
        t1 = threading.Thread(target=self._check_scan_parallel, args=(select_query, count_before_update['$1'], scan_val))
        t.daemon = True
        t1.daemon = True
        threads.append(t)
        threads.append(t1)
        t.start()
        t1.start()
        for th in threads:
            th.join()
            threads.remove(th)

        update_query = "update `beer-sample` set state='California' where state='Califffornia'"
        self.run_cbq_query(update_query)

        self._remove_all_fts_indexes()

    def test_drop_fts_index(self):
        self.cbcluster = CouchbaseCluster(name='cluster', nodes=self.servers, log=self.log)
        fts_name_index = self._create_fts_index(index_name="idx_beer_sample_fts", doc_count=7303, source_name='beer-sample')
        select_query = "select meta().id from `beer-sample` where search(b, {\"query\": {\"field\": \"state\", \"match\": \"California\"}, \"sort\": [{\"by\": \"field\", \"field\": \"city\"}]})"
        expected_count = self.run_cbq_query(select_query)['metrics']['resultCount']
        threads = []
        for i in range(0, 20):
            t = threading.Thread(target=self._select_parallel, args=(select_query, expected_count))
            t.daemon = True
            threads.append(t)
            t.start()

        t = threading.Thread(target=self._delete_fts_index, args=("idx_beer_sample_fts",))
        t.daemon = True
        t.start()
        threads.append(t)

        for i in range(0, 20):
            t = threading.Thread(target=self._select_parallel, args=(select_query,expected_count))
            t.daemon = True
            t.start()
            threads.append(t)

        for th in threads:
            th.join()
            threads.remove(th)


# ============================================ utils =================================
    def _compare_n1ql_results_against_fts(self, n1ql_results, fts_results):
        n1ql_doc_ids = []
        for result in n1ql_results:
            n1ql_doc_ids.append(result['id'])

        hits = fts_results['hits']
        fts_doc_ids = []
        for hit in hits:
            fts_doc_ids.append(hit['id'])

        if len(n1ql_doc_ids) != len(fts_doc_ids):
            return "Results count does not match for test . FTS - " + str(len(fts_doc_ids)) + ", N1QL - " + str(len(n1ql_doc_ids))
        if sorted(fts_doc_ids) != sorted(n1ql_doc_ids):
            return "Found mismatch in results for test ."
        return "OK"

    def _check_scan_parallel(self, query, expected_count, scan_type):
        try:
            search_results = self.run_cbq_query(query)['metrics']['resultCount']
            self.assertEquals(expected_count - int(search_results) > 0, True, "Query result is incorrect for "+scan_type+": \n"
                                "Results before update - "+str(expected_count)+", count during update - "+str(search_results))
        except CBQError, e:
            self.assertEquals('True', 'False', 'Wrong query - '+str(query))

    def _update_parallel(self, query, operation, expected_count, scan_type):
        try:
            self.run_cbq_query(query)
        except CBQError, e:
            self.assertEquals('True', 'False', 'Wrong query - '+str(query))

    def _select_parallel(self, query, expected_count):
        try:
            search_results = self.run_cbq_query(query)['metrics']['resultCount']
            self.assertEquals(expected_count, search_results, "Query result is incorrect")
        except CBQError, e:
            self.assertEquals('True', 'False', 'Wrong query - '+str(query))

    def _load_test_buckets(self):
        self.rest.load_sample("beer-sample")
        self.wait_for_buckets_status({"beer-sample": "healthy"}, 5, 120)
        self.wait_for_bucket_docs({"beer-sample": 7303}, 5, 120)

        self.run_cbq_query("create index beer_sample_code_idx on `beer-sample` (`beer-sample`.code)")
        self.run_cbq_query("create index beer_sample_brewery_id_idx on `beer-sample` (`beer-sample`.brewery_id)")
        self.wait_for_all_indexes_online()

    def _create_fts_index(self, index_name='', doc_count=0, source_name=''):
        fts_index = self.cbcluster.create_fts_index(name=index_name, source_name=source_name)
        indexed_doc_count = 0
        while indexed_doc_count < doc_count:
            try:
                indexed_doc_count = fts_index.get_indexed_doc_count()
            except KeyError, k:
                continue

        return fts_index

    def _delete_fts_index(self, index_name=''):
        shell = RemoteMachineShellConnection(self.master)
        shell.execute_command("curl -X DELETE -u Administrator:password http://"+self.master.ip+":8094/api/index/"+index_name)

    def _open_curl_access(self):
        shell = RemoteMachineShellConnection(self.master)

        cmd = (self.curl_path + ' -u ' + self.master.rest_username + ':' + self.master.rest_password + ' http://' + self.master.ip + ':' + self.master.port + '/settings/querySettings/curlWhitelist -d \'{"all_access":true}\'')
        shell.execute_command(cmd)

    def _create_all_users(self):
        admin_user = [{'id': 'admin_user', 'name': 'admin_user', 'password': 'password'}]
        rolelist = [{'id': 'admin_user', 'name': 'admin_user', 'roles': 'admin'}]
        RbacBase().create_user_source(admin_user, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['admin_user'] = {'username': 'admin_user', 'password': 'password'}

        all_buckets_data_reader_search_admin = [{'id': 'all_buckets_data_reader_search_admin', 'name': 'all_buckets_data_reader_search_admin', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_search_admin', 'name': 'all_buckets_data_reader_search_admin', 'roles': 'query_select[*],fts_admin[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_search_admin, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_search_admin'] = {'username': 'all_buckets_data_reader_search_admin', 'password': 'password'}

        all_buckets_data_reader_search_reader = [{'id': 'all_buckets_data_reader_search_reader', 'name': 'all_buckets_data_reader_search_reader', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_search_reader', 'name': 'all_buckets_data_reader_search_reader', 'roles': 'query_select[*],fts_searcher[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_search_reader, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_search_reader'] = {'username': 'all_buckets_data_reader_search_reader', 'password': 'password'}

        test_bucket_data_reader_search_admin = [{'id': 'test_bucket_data_reader_search_admin', 'name': 'test_bucket_data_reader_search_admin', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_search_admin', 'name': 'test_bucket_data_reader_search_admin', 'roles': 'query_select[beer-sample],fts_admin[beer-sample],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_search_admin, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_search_admin'] = {'username': 'test_bucket_data_reader_search_admin', 'password': 'password'}

        test_bucket_data_reader_null = [{'id': 'test_bucket_data_reader_null', 'name': 'test_bucket_data_reader_null', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_null', 'name': 'test_bucket_data_reader_null', 'roles': 'query_select[beer-sample],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_null, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_null'] = {'username': 'test_bucket_data_reader_null', 'password': 'password'}

        test_bucket_data_reader_search_reader = [{'id': 'test_bucket_data_reader_search_reader', 'name': 'test_bucket_data_reader_search_reader', 'password': 'password'}]
        rolelist = [{'id': 'test_bucket_data_reader_search_reader', 'name': 'test_bucket_data_reader_search_reader', 'roles': 'query_select[beer-sample],fts_searcher[beer-sample],query_external_access'}]
        RbacBase().create_user_source(test_bucket_data_reader_search_reader, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['test_bucket_data_reader_search_reader'] = {'username': 'test_bucket_data_reader_search_reader', 'password': 'password'}

        all_buckets_data_reader_null = [{'id': 'all_buckets_data_reader_null', 'name': 'all_buckets_data_reader_null', 'password': 'password'}]
        rolelist = [{'id': 'all_buckets_data_reader_null', 'name': 'all_buckets_data_reader_null', 'roles': 'query_select[*],query_external_access'}]
        RbacBase().create_user_source(all_buckets_data_reader_null, 'builtin', self.master)
        RbacBase().add_user_role(rolelist, RestConnection(self.master), 'builtin')
        self.users['all_buckets_data_reader_null'] = {'username': 'all_buckets_data_reader_null', 'password': 'password'}


    def _run_fts_request(self, request=""):
        cmd = "curl -XPOST -H \"Content-Type: application/json\" -u "+self.username+":"+self.password+" " \
                            "http://"+self.master.ip+":8094/api/index/idx_beer_sample_fts/query -d " + request



        shell = RemoteMachineShellConnection(self.master)

        output, error = shell.execute_command(cmd)
        json_output_str = ''
        for s in output:
            json_output_str += s
        result =  json.loads(json_output_str)
        return result

    def _remove_all_fts_indexes(self):
        indexes = self.cbcluster.get_indexes()
        for index in indexes:
            self._delete_fts_index(index.name)
