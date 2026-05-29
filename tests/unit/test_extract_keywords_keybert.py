import json
import pytest

keybert_module = pytest.importorskip("analysis.extract_keywords_keybert")


def test_num_keywords_minimum_value():
    assert keybert_module.num_keywords(100) == 5
    assert keybert_module.num_keywords(499) == 5


def test_num_keywords_middle_value():
    assert keybert_module.num_keywords(2500) == 25


def test_num_keywords_maximum_value():
    assert keybert_module.num_keywords(10000) == 50


def test_process_batch_returns_keywords():
    class FakeKeyBERT:
        def extract_keywords(
            self,
            texts,
            keyphrase_ngram_range,
            stop_words,
            top_n,
            use_mmr,
            diversity,
        ):
            assert len(texts) == 2
            assert top_n == 5

            return [
                [
                    ("податкова політика", 0.91),
                    ("державний бюджет", 0.82),
                    ("законопроєкт", 0.71),
                ],
                [
                    ("правова політика", 0.88),
                    ("комітет", 0.77),
                ],
            ]

    batch = [
        ("doc_1", "Текст першого документа", 500),
        ("doc_2", "Текст другого документа", 300),
    ]

    result = keybert_module.process_batch(batch, FakeKeyBERT())

    assert len(result) == 2
    assert result[0]["doc_id"] == "doc_1"
    assert json.loads(result[0]["keywords_keybert"]) == [
        "податкова політика",
        "державний бюджет",
        "законопроєкт",
    ]
    assert result[0]["num_keywords"] == 3

    assert result[1]["doc_id"] == "doc_2"
    assert json.loads(result[1]["keywords_keybert"]) == [
        "правова політика",
        "комітет",
    ]
    assert result[1]["num_keywords"] == 2


def test_process_batch_returns_empty_keywords_on_model_error():
    class BrokenKeyBERT:
        def extract_keywords(self, *args, **kwargs):
            raise RuntimeError("model error")

    batch = [
        ("doc_1", "Текст першого документа", 500),
        ("doc_2", "Текст другого документа", 500),
    ]

    result = keybert_module.process_batch(batch, BrokenKeyBERT())

    assert len(result) == 2
    assert result[0]["doc_id"] == "doc_1"
    assert result[1]["doc_id"] == "doc_2"
    assert json.loads(result[0]["keywords_keybert"]) == []
    assert json.loads(result[1]["keywords_keybert"]) == []
    assert result[0]["num_keywords"] == 0
    assert result[1]["num_keywords"] == 0