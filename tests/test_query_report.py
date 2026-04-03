#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
es_query_report.py 单元测试
"""

import unittest
from pathlib import Path
import sys

# 添加 query-report 目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "query-report"))

from es_query_report import (
    parse_requests,
    is_msearch_path,
    extract_target_names,
    flatten_dict_with_prefix,
    format_count,
    json_pretty,
    body_pretty,
    body_compact,
    RequestCase,
)


class TestIsMsearchPath(unittest.TestCase):
    """测试 is_msearch_path 函数"""

    def test_msearch_path(self):
        self.assertTrue(is_msearch_path("/_msearch"))
        self.assertTrue(is_msearch_path("_msearch"))
        self.assertTrue(is_msearch_path("index/_msearch"))

    def test_non_msearch_path(self):
        self.assertFalse(is_msearch_path("/_search"))
        self.assertFalse(is_msearch_path("index/_search"))
        self.assertFalse(is_msearch_path("_cluster/health"))

    def test_msearch_with_params(self):
        self.assertTrue(is_msearch_path("/_msearch?timeout=1s"))


class TestParseRequests(unittest.TestCase):
    """测试 parse_requests 函数"""

    def test_single_request(self):
        text = """#1 test query
GET index/_search
{"query": {"match_all": {}}}
"""
        cases = parse_requests(text)
        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0].title, "1 test query")
        self.assertEqual(cases[0].method, "GET")
        self.assertEqual(cases[0].path, "index/_search")
        self.assertEqual(cases[0].body, {"query": {"match_all": {}}})

    def test_multiple_requests(self):
        text = """#1 first query
GET index1/_search
{"query": {"match_all": {}}}

#2 second query
GET index2/_search
{"query": {"match": {"title": "test"}}}
"""
        cases = parse_requests(text)
        self.assertEqual(len(cases), 2)
        self.assertEqual(cases[0].title, "1 first query")
        self.assertEqual(cases[1].title, "2 second query")

    def test_request_without_body(self):
        text = """#1 no body
GET _cluster/health
"""
        cases = parse_requests(text)
        self.assertEqual(len(cases), 1)
        self.assertIsNone(cases[0].body)

    def test_msearch_request(self):
        text = """#1 msearch
GET _msearch
{"index": "test"}
{"query": {"match_all": {}}}
"""
        cases = parse_requests(text)
        self.assertEqual(len(cases), 1)
        self.assertTrue(cases[0].body_is_ndjson)
        self.assertTrue(cases[0].body.endswith("\n"))

    def test_different_methods(self):
        text = """#1 get request
GET index/_doc/1

#2 post request
POST index/_search
{"query": {"match_all": {}}}

#3 delete request
DELETE index/_doc/1
"""
        cases = parse_requests(text)
        self.assertEqual(cases[0].method, "GET")
        self.assertEqual(cases[1].method, "POST")
        self.assertEqual(cases[2].method, "DELETE")

    def test_empty_title(self):
        text = """#
GET index/_search
"""
        cases = parse_requests(text)
        self.assertEqual(cases[0].title, "Query 1")

    def test_invalid_format_no_method(self):
        text = """#1 invalid
"""
        with self.assertRaisesRegex(ValueError, "缺少方法行"):
            parse_requests(text)

    def test_invalid_json_body(self):
        text = """#1 bad json
GET index/_search
{invalid json}
"""
        with self.assertRaisesRegex(ValueError, "JSON body 解析失败"):
            parse_requests(text)


class TestExtractTargetNames(unittest.TestCase):
    """测试 extract_target_names 函数"""

    def test_single_index(self):
        self.assertEqual(extract_target_names("movies/_search"), ["movies"])

    def test_multiple_indices(self):
        self.assertEqual(extract_target_names("index1,index2/_search"), ["index1", "index2"])

    def test_system_api(self):
        self.assertEqual(extract_target_names("_cluster/health"), [])
        self.assertEqual(extract_target_names("/_cat/indices"), [])

    def test_with_query_params(self):
        self.assertEqual(extract_target_names("movies/_search?size=10"), ["movies"])

    def test_empty_path(self):
        self.assertEqual(extract_target_names(""), [])
        self.assertEqual(extract_target_names("/"), [])


class TestFlattenDictWithPrefix(unittest.TestCase):
    """测试 flatten_dict_with_prefix 函数"""

    def test_simple_dict(self):
        data = {"key": "value"}
        result = flatten_dict_with_prefix(data)
        self.assertEqual(result, {"key": "value"})

    def test_nested_dict(self):
        data = {"index": {"number_of_shards": "1", "number_of_replicas": "0"}}
        result = flatten_dict_with_prefix(data)
        self.assertEqual(result, {
            "index.number_of_shards": "1",
            "index.number_of_replicas": "0"
        })

    def test_deeply_nested(self):
        data = {"a": {"b": {"c": "value"}}}
        result = flatten_dict_with_prefix(data)
        self.assertEqual(result, {"a.b.c": "value"})

    def test_with_prefix(self):
        data = {"key": "value"}
        result = flatten_dict_with_prefix(data, "prefix")
        self.assertEqual(result, {"prefix.key": "value"})


class TestFormatCount(unittest.TestCase):
    """测试 format_count 函数"""

    def test_positive_integer(self):
        self.assertEqual(format_count(8516), "8,516")

    def test_negative_integer(self):
        self.assertEqual(format_count(-1000), "-1,000")

    def test_none_value(self):
        self.assertEqual(format_count(None), "-")

    def test_string_number(self):
        self.assertEqual(format_count("12345"), "12,345")

    def test_non_numeric_string(self):
        self.assertEqual(format_count("abc"), "abc")


class TestJsonPretty(unittest.TestCase):
    """测试 json_pretty 函数"""

    def test_dict(self):
        data = {"key": "value"}
        result = json_pretty(data)
        self.assertIn('"key": "value"', result)

    def test_list(self):
        data = [1, 2, 3]
        result = json_pretty(data)
        self.assertEqual("[\n  1,\n  2,\n  3\n]", result)

    def test_unicode(self):
        data = {"中文": "测试"}
        result = json_pretty(data)
        self.assertIn("中文", result)
        self.assertIn("测试", result)


class TestBodyPretty(unittest.TestCase):
    """测试 body_pretty 函数"""

    def test_none_body(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="test",
            body_raw="", body=None, body_is_ndjson=False
        )
        self.assertEqual(body_pretty(case), "")

    def test_dict_body(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="test",
            body_raw='{"key": "value"}', body={"key": "value"}, body_is_ndjson=False
        )
        result = body_pretty(case)
        self.assertIn('"key": "value"', result)

    def test_ndjson_body(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="_msearch",
            body_raw='{"index": "test"}\n{"query": {}}\n',
            body='{"index": "test"}\n{"query": {}}\n', body_is_ndjson=True
        )
        result = body_pretty(case)
        self.assertEqual(result, '{"index": "test"}\n{"query": {}}')


class TestBodyCompact(unittest.TestCase):
    """测试 body_compact 函数"""

    def test_dict_body_compact(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="test",
            body_raw='{"key": "value"}', body={"key": "value"}, body_is_ndjson=False
        )
        result = body_compact(case)
        self.assertEqual(result, '{"key":"value"}')

    def test_ndjson_body_compact(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="_msearch",
            body_raw='{"index": "test"}\n{"query": {}}\n',
            body='{"index": "test"}\n{"query": {}}\n', body_is_ndjson=True
        )
        result = body_compact(case)
        self.assertEqual(result, '{"index": "test"}\\n{"query": {}}')


if __name__ == "__main__":
    unittest.main()
