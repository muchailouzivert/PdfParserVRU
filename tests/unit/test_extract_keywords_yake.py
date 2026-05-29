import json
import pytest

yake_module = pytest.importorskip("analysis.extract_keywords_yake")


def test_num_keywords_minimum_value():
    assert yake_module.num_keywords(100) == 5
    assert yake_module.num_keywords(499) == 5


def test_num_keywords_middle_value():
    assert yake_module.num_keywords(1200) == 12


def test_num_keywords_maximum_value():
    assert yake_module.num_keywords(10000) == 50


def test_extract_yake_returns_keywords(monkeypatch):
    class FakeKeywordExtractor:
        def __init__(self, lan, n, dedupLim, top):
            self.lan = lan
            self.n = n
            self.dedupLim = dedupLim
            self.top = top

        def extract_keywords(self, text):
            return [
                ("податкова політика", 0.01),
                ("державний бюджет", 0.02),
                ("законопроєкт", 0.03),
            ]

    monkeypatch.setattr(yake_module.yake, "KeywordExtractor", FakeKeywordExtractor)

    result = yake_module.extract_yake(
        ("doc_1", "Текст про податкову політику та державний бюджет.", 500)
    )

    assert result["doc_id"] == "doc_1"
    assert result["num_keywords"] == 3
    assert json.loads(result["keywords_yake"]) == [
        "податкова політика",
        "державний бюджет",
        "законопроєкт",
    ]


def test_extract_yake_returns_empty_list_on_extractor_error(monkeypatch):
    class BrokenKeywordExtractor:
        def __init__(self, lan, n, dedupLim, top):
            pass

        def extract_keywords(self, text):
            raise RuntimeError("YAKE error")

    monkeypatch.setattr(yake_module.yake, "KeywordExtractor", BrokenKeywordExtractor)

    result = yake_module.extract_yake(("doc_2", "bad text", 500))

    assert result["doc_id"] == "doc_2"
    assert result["num_keywords"] == 0
    assert json.loads(result["keywords_yake"]) == []