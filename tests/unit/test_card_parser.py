from datetime import date

from bs4 import BeautifulSoup

from parsers.card_parser import parse_date_ua, find_row_value, parse_card


def test_parse_date_ua_valid_date():
    result = parse_date_ua("15.03.2024")

    assert result == date(2024, 3, 15)


def test_parse_date_ua_invalid_date_returns_none():
    result = parse_date_ua("wrong-date")

    assert result is None


def test_find_row_value_returns_matching_value():
    html = """
    <div class="info">
        <div class="row">
            <div class="col">Номер, дата реєстрації</div>
            <div class="col">1234 від 15.03.2024</div>
        </div>
    </div>
    """

    soup = BeautifulSoup(html, "html.parser")
    result = find_row_value(soup, "Номер, дата реєстрації")

    assert result == "1234 від 15.03.2024"


def test_find_row_value_returns_none_when_label_missing():
    html = """
    <div class="info">
        <div class="row">
            <div class="col">Інше поле</div>
            <div class="col">Тестове значення</div>
        </div>
    </div>
    """

    soup = BeautifulSoup(html, "html.parser")
    result = find_row_value(soup, "Рубрика законопроекту")

    assert result is None


def test_parse_card_extracts_basic_metadata():
    html = """
    <div class="info">
        <div class="row">
            <div class="col">Номер, дата реєстрації</div>
            <div class="col">1234 від 15.03.2024</div>
        </div>
        <div class="row">
            <div class="col">Рубрика законопроекту</div>
            <div class="col">Економічна політика</div>
        </div>
        <div class="row">
            <div class="col">Ініціатор(и) законопроекту</div>
            <div class="col">Іваненко І. І.</div>
        </div>
        <div class="row">
            <div class="col">Головний комітет</div>
            <div class="col">Комітет з питань фінансів</div>
        </div>
    </div>
    """

    result = parse_card(html)

    assert result["reg_num"] == "1234"
    assert result["reg_date"] == date(2024, 3, 15)
    assert result["rubric"] == "Економічна політика"
    assert result["initiators"] == "Іваненко І. І."
    assert result["main_committee"] == "Комітет з питань фінансів"
    assert result["comparative_tables"] == []
    assert result["passage"] == []


def test_parse_card_extracts_comparative_tables_only():
    html = """
    <a class="downloadFile"
       data-id="555"
       data-ext=".docx"
       data-file-name="table_1234">
       Порівняльна таблиця до другого читання
    </a>

    <a class="downloadFile"
       data-id="777"
       data-ext=".pdf"
       data-file-name="note_1234">
       Пояснювальна записка
    </a>
    """

    result = parse_card(html)

    assert len(result["comparative_tables"]) == 1

    table = result["comparative_tables"][0]
    assert table["file_id"] == 555
    assert table["ext"] == ".docx"
    assert table["file_name"] == "table_1234"
    assert "Порівняльна таблиця" in table["title"]


def test_parse_card_extracts_passage_rows():
    html = """
    <div id="nav-tab1">
        <table>
            <tbody>
                <tr>
                    <td>15.03.2024</td>
                    <td>Одержано Верховною Радою</td>
                </tr>
                <tr>
                    <td>20.03.2024</td>
                    <td>Передано на розгляд комітету</td>
                </tr>
            </tbody>
        </table>
    </div>
    """

    result = parse_card(html)

    assert result["passage"] == [
        {
            "date": "15.03.2024",
            "status": "Одержано Верховною Радою",
        },
        {
            "date": "20.03.2024",
            "status": "Передано на розгляд комітету",
        },
    ]


def test_parse_card_handles_missing_fields():
    html = "<html><body>empty card</body></html>"

    result = parse_card(html)

    assert result["reg_num"] is None
    assert result["reg_date"] is None
    assert result["rubric"] is None
    assert result["initiators"] is None
    assert result["main_committee"] is None
    assert result["comparative_tables"] == []
    assert result["passage"] == []