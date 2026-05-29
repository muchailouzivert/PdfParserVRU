import pandas as pd

from parsers.unite_parsed import get_final_status


def test_get_final_status_classifies_accepted_status():
    passage = pd.DataFrame(
        {
            "card_id": [1, 1],
            "status_date": ["01.01.2024", "05.01.2024"],
            "status_text": [
                "Одержано Верховною Радою",
                "Прийнято в цілому",
            ],
        }
    )

    result = get_final_status(passage)

    assert len(result) == 1
    assert result.loc[0, "card_id"] == 1
    assert result.loc[0, "outcome"] == "accepted"
    assert result.loc[0, "num_stages"] == 2


def test_get_final_status_classifies_rejected_status():
    passage = pd.DataFrame(
        {
            "card_id": [2, 2],
            "status_date": ["01.02.2024", "10.02.2024"],
            "status_text": [
                "Передано на розгляд",
                "Відхилено",
            ],
        }
    )

    result = get_final_status(passage)

    assert result.loc[0, "outcome"] == "rejected"
    assert result.loc[0, "final_status"] == "Відхилено"


def test_get_final_status_classifies_in_progress_status():
    passage = pd.DataFrame(
        {
            "card_id": [3, 3],
            "status_date": ["01.03.2024", "15.03.2024"],
            "status_text": [
                "Одержано Верховною Радою",
                "Передано на розгляд комітету",
            ],
        }
    )

    result = get_final_status(passage)

    assert result.loc[0, "outcome"] == "in_progress"
    assert result.loc[0, "num_stages"] == 2


def test_get_final_status_classifies_unknown_when_status_is_missing():
    passage = pd.DataFrame(
        {
            "card_id": [4],
            "status_date": ["01.04.2024"],
            "status_text": [None],
        }
    )

    result = get_final_status(passage)

    assert result.loc[0, "outcome"] == "unknown"


def test_get_final_status_uses_latest_status_by_date():
    passage = pd.DataFrame(
        {
            "card_id": [5, 5, 5],
            "status_date": ["10.01.2024", "01.01.2024", "20.01.2024"],
            "status_text": [
                "Передано на розгляд",
                "Одержано Верховною Радою",
                "Відхилено",
            ],
        }
    )

    result = get_final_status(passage)

    assert result.loc[0, "final_status"] == "Відхилено"
    assert result.loc[0, "outcome"] == "rejected"
    assert result.loc[0, "num_stages"] == 3


def test_get_final_status_handles_multiple_cards():
    passage = pd.DataFrame(
        {
            "card_id": [1, 1, 2, 2],
            "status_date": [
                "01.01.2024",
                "05.01.2024",
                "01.02.2024",
                "05.02.2024",
            ],
            "status_text": [
                "Одержано Верховною Радою",
                "Прийнято",
                "Одержано Верховною Радою",
                "Знято з розгляду",
            ],
        }
    )

    result = get_final_status(passage).sort_values("card_id").reset_index(drop=True)

    assert len(result) == 2
    assert result.loc[0, "card_id"] == 1
    assert result.loc[0, "outcome"] == "accepted"
    assert result.loc[1, "card_id"] == 2
    assert result.loc[1, "outcome"] == "rejected"