#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
es_query_report.py 单元测试
"""

import json
import pytest
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


class TestIsMsearchPath:
    """测试 is_msearch_path 函数"""

    def test_msearch_path(self):
        assert is_msearch_path("/_msearch") is True
        assert is_msearch_path("_msearch") is True
        assert is_msearch_path("index/_msearch") is True

    def test_non_msearch_path(self):
        assert is_msearch_path("/_search") is False
        assert is_msearch_path("index/_search") is False
        assert is_msearch_path("_cluster/health") is False

    def test_msearch_with_params(self):
        assert is_msearch_path("/_msearch?timeout=1s") is True


class TestParseRequests:
    """测试 parse_requests 函数"""

    def test_single_request(self):
        text = """#1 test query
GET index/_search
{"query": {"match_all": {}}}
"""
        cases = parse_requests(text)
        assert len(cases) == 1
        assert cases[0].title == "1 test query"
        assert cases[0].method == "GET"
        assert cases[0].path == "index/_search"
        assert cases[0].body == {"query": {"match_all": {}}}

    def test_multiple_requests(self):
        text = """#1 first query
GET index1/_search
{"query": {"match_all": {}}}

#2 second query
GET index2/_search
{"query": {"match": {"title": "test"}}}
"""
        cases = parse_requests(text)
        assert len(cases) == 2
        assert cases[0].title == "1 first query"
        assert cases[1].title == "2 second query"

    def test_request_without_body(self):
        text = """#1 no body
GET _cluster/health
"""
        cases = parse_requests(text)
        assert len(cases) == 1
        assert cases[0].body is None

    def test_msearch_request(self):
        text = """#1 msearch
GET _msearch
{"index": "test"}
{"query": {"match_all": {}}}
"""
        cases = parse_requests(text)
        assert len(cases) == 1
        assert cases[0].body_is_ndjson is True
        assert cases[0].body.endswith("\n")

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
        assert cases[0].method == "GET"
        assert cases[1].method == "POST"
        assert cases[2].method == "DELETE"

    def test_empty_title(self):
        text = """#
GET index/_search
"""
        cases = parse_requests(text)
        assert cases[0].title == "Query 1"

    def test_invalid_format_no_method(self):
        # 只有标题没有方法行
        text = """#1 invalid
"""
        with pytest.raises(ValueError, match="缺少方法行"):
            parse_requests(text)

    def test_invalid_json_body(self):
        text = """#1 bad json
GET index/_search
{invalid json}
"""
        with pytest.raises(ValueError, match="JSON body 解析失败"):
            parse_requests(text)


class TestExtractTargetNames:
    """测试 extract_target_names 函数"""

    def test_single_index(self):
        assert extract_target_names("movies/_search") == ["movies"]

    def test_multiple_indices(self):
        assert extract_target_names("index1,index2/_search") == ["index1", "index2"]

    def test_system_api(self):
        assert extract_target_names("_cluster/health") == []
        assert extract_target_names("/_cat/indices") == []

    def test_with_query_params(self):
        assert extract_target_names("movies/_search?size=10") == ["movies"]

    def test_empty_path(self):
        assert extract_target_names("") == []
        assert extract_target_names("/") == []


class TestFlattenDictWithPrefix:
    """测试 flatten_dict_with_prefix 函数"""

    def test_simple_dict(self):
        data = {"key": "value"}
        result = flatten_dict_with_prefix(data)
        assert result == {"key": "value"}

    def test_nested_dict(self):
        data = {"index": {"number_of_shards": "1", "number_of_replicas": "0"}}
        result = flatten_dict_with_prefix(data)
        assert result == {
            "index.number_of_shards": "1",
            "index.number_of_replicas": "0"
        }

    def test_deeply_nested(self):
        data = {"a": {"b": {"c": "value"}}}
        result = flatten_dict_with_prefix(data)
        assert result == {"a.b.c": "value"}

    def test_with_prefix(self):
        data = {"key": "value"}
        result = flatten_dict_with_prefix(data, "prefix")
        assert result == {"prefix.key": "value"}


class TestFormatCount:
    """测试 format_count 函数"""

    def test_positive_integer(self):
        assert format_count(8516) == "8,516"

    def test_negative_integer(self):
        assert format_count(-1000) == "-1,000"

    def test_none_value(self):
        assert format_count(None) == "-"

    def test_string_number(self):
        assert format_count("12345") == "12,345"

    def test_non_numeric_string(self):
        assert format_count("abc") == "abc"


class TestJsonPretty:
    """测试 json_pretty 函数"""

    def test_dict(self):
        data = {"key": "value"}
        result = json_pretty(data)
        assert '"key": "value"' in result

    def test_list(self):
        data = [1, 2, 3]
        result = json_pretty(data)
        assert "[\n  1,\n  2,\n  3\n]" == result

    def test_unicode(self):
        data = {"中文": "测试"}
        result = json_pretty(data)
        assert "中文" in result
        assert "测试" in result


class TestBodyPretty:
    """测试 body_pretty 函数"""

    def test_none_body(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="test",
            body_raw="", body=None, body_is_ndjson=False
        )
        assert body_pretty(case) == ""

    def test_dict_body(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="test",
            body_raw='{"key": "value"}', body={"key": "value"}, body_is_ndjson=False
        )
        result = body_pretty(case)
        assert '"key": "value"' in result

    def test_ndjson_body(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="_msearch",
            body_raw='{"index": "test"}\n{"query": {}}\n',
            body='{"index": "test"}\n{"query": {}}\n', body_is_ndjson=True
        )
        result = body_pretty(case)
        assert result == '{"index": "test"}\n{"query": {}}'


class TestBodyCompact:
    """测试 body_compact 函数"""

    def test_dict_body_compact(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="test",
            body_raw='{"key": "value"}', body={"key": "value"}, body_is_ndjson=False
        )
        result = body_compact(case)
        assert result == '{"key":"value"}'

    def test_ndjson_body_compact(self):
        case = RequestCase(
            seq=1, title="test", method="GET", path="_msearch",
            body_raw='{"index": "test"}\n{"query": {}}\n',
            body='{"index": "test"}\n{"query": {}}\n', body_is_ndjson=True
        )
        result = body_compact(case)
        assert result == '{"index": "test"}\\n{"query": {}}'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
