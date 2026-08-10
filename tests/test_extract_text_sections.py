"""
Unit tests for the section text extractor.
"""

import logging

import pytest
from bs4 import BeautifulSoup
from bs4.element import Tag

from rechtspraak_extractor.extract_text_sections import SectionExtractor

# Define test input variables and XML data
_KNOWN_SECTION_TITLES = {"this is a known title", "also a known title", "context"}

_test_xml_no_uitspraak_node = b"<root />"

_test_xml_no_data = b"""\
    <uitspraak id="test:id:0"> Test data.
    </uitspraak>
"""

_test_xml_section_no_title_no_role = b"""\
    <uitspraak id="test:id:1">
        <section>
            <para>Test</para>
        </section>
    </uitspraak>
"""

_test_xml_section_role_no_title = b"""\
    <uitspraak id="test:id:2">
        <section role="Role">
            <para>Test</para>
        </section>
    </uitspraak>
"""

_test_xml_section_role_and_title = b"""\
    <uitspraak id="test:id:3">
        <section role="Role">
            <title>Title</title>
            <para>Test</para>
        </section>
    </uitspraak>
"""

_test_xml_unnested_sections = b"""\
    <uitspraak id="test:id:nested">
        <section>
            <title> Section 1</title>
            <para>Section 1 body.</para>
        </section>
        <section>
            <title> Section 2</title>
            <para>Section 2 body.</para>
        </section>
    </uitspraak>
"""

_test_xml_nested_sections = b"""\
    <uitspraak id="test:id:nested">
        <section>
            <title>Parent</title>
            <para>Parent body.</para>
            <section>
                <title>Child</title>
                <para>Child body.</para>
                <section role="grandchild">
                    <para>Grandchild body.</para>
                </section>
            </section>
        </section>
    </uitspraak>
"""

_test_xml_standard_format = b"""\
    <uitspraak id="test:id:1">
        <uitspraak.info>
            <para>uitspraak </para>
            <para />
            <bridgehead role="bold">College van Beroep</bridgehead>
            <para />
            <parablock>
            <para>zaaknummer: 14/803</para>
            </parablock>
        </uitspraak.info>
        <section role="procesverloop">
            <para />
            <parablock>
            <para>Course of proceedings part I.</para>
            </parablock>
            <para />
        </section>
        <section>
            <title>Beslissing </title>
            <para />
            <para>1. Decision I. </para>
            <para />
        </section>
        <section>
            <title>Procesverloop </title>
            <para>Course of proceedings part II.</para>
        </section>
        <section>
            <title>Beslissing </title>
            <para>2. Decision II.</para>
        </section>
    </uitspraak>
"""

_test_xml_short_numbered_sentences = b"""\
        <uitspraak>
            <parablock>
                <para>gronden:</para>
                <para>9. Het beroep is ongegrond.</para>
                <para>proceskosten:</para>
                <para>Voor een proceskostenveroordeling bestaat geen aanleiding.</para>
            </parablock>
        </uitspraak>
"""

_test_xml_no_section_titles = b"""\
    <uitspraak id="test:id:2">
        <para>College van Beroep voor</para>
    <parablock>
      <para>1.	De procedure</para>
      <para> The proceedings.</para>
      <para> 1.1 Context </para>
      <para>Context text.</para>
      <para>1. De procedure </para>
      <para>Proceedings continued.</para>
    </parablock>
    <para />
    <parablock>
      <para>2.	Beslissing</para>
      <para>2.1	The decision is</para>
    </parablock>
</uitspraak>
"""


def return_uitspraak_node_children(xml: bytes) -> list[Tag]:
    """
    Returns the children of the <uitspraak> node in the given XML.

    Args:
        xml (bytes): The XML data as bytes.

    Returns:
        A list of Tag objects representing the children of the <uitspraak> node.

    Raises:
        ValueError: If no <uitspraak> node is found in the XML.
    """
    soup = BeautifulSoup(xml, features="xml")
    uitspraak_node = soup.find("uitspraak")
    if uitspraak_node is None:
        raise ValueError("No <uitspraak> node found in the XML.")

    return [child for child in uitspraak_node.children if isinstance(child, Tag)]


@pytest.fixture
def section_extractor() -> SectionExtractor:
    """
    Fixture that provides a SectionExtractor instance for testing.
    """
    dummy_xml = b"""<test id="dummy:id:1"><para>Dummy text</para></test>"""
    dummy_soup = BeautifulSoup(dummy_xml, features="xml")
    return SectionExtractor(dummy_soup, _KNOWN_SECTION_TITLES)


@pytest.mark.unit
def test_normalize_xml_text_collapses_whitespace():
    """
    Test that the _normalize_xml_text method correctly collapses whitespace in the input text.
    """
    raw_text = "  Procesverloop \n\n   Bij   besluit\tvan  28 maart 2014  "
    normalized = SectionExtractor._normalize_xml_text(raw_text)
    assert normalized == "Procesverloop Bij besluit van 28 maart 2014"


@pytest.mark.unit
@pytest.mark.parametrize(
    "xml, expected_title",
    [
        (_test_xml_section_no_title_no_role, "no_section_title_found"),
        (_test_xml_section_role_no_title, "Role"),
        (_test_xml_section_role_and_title, "Title"),
    ],
)
def test_get_section_title(section_extractor, xml, expected_title):
    """
    Test the _get_section_title method to ensure it correctly extracts and normalizes section titles from <section> tags.
    """
    soup = BeautifulSoup(xml, features="xml")
    section_tag = soup.find("section")
    title = section_extractor._get_section_title(section_tag)
    assert title == expected_title


@pytest.mark.unit
@pytest.mark.parametrize(
    "raw_title_text, expected_clean_title_text",
    [
        ("1) Title", "title"),
        ("I   Title", "title"),
        ("II. Title:", "title"),
        ("Title:", "title"),
        ("  2.   Title.  ", "title"),
        ("2.3 Title", "title"),
        ("2.3.4 Title", "title"),
        ("1. Title text with number 2", "title text with number 2"),
    ],
)
def test_clean_title_text(section_extractor, raw_title_text, expected_clean_title_text):
    """
    Test the _clean_title_text method to ensure it correctly cleans and normalizes title text.
    """
    assert (
        section_extractor._clean_title_text(raw_title_text) == expected_clean_title_text
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "current_title, text, initial_sections, expected_sections",
    [
        (None, "Case text", {}, {}),
        (None, "   ", {}, {}),
        ("Section title", "   ", {"Section title": []}, {"Section title": []}),
        (
            "Section title",
            "Case text",
            {"Section title": []},
            {"Section title": ["Case text"]},
        ),
    ],
)
def test_add_line(
    section_extractor, current_title, text, initial_sections, expected_sections
):
    """
    Test the _add_line method to ensure it correctly adds lines of text to the appropriate section.
    """
    sections = {key: value[:] for key, value in initial_sections.items()}
    section_extractor._add_line(sections, current_title, text)

    assert sections == expected_sections


@pytest.mark.unit
@pytest.mark.parametrize(
    "xml, expected_sections",
    [
        (
            _test_xml_unnested_sections,
            {
                "section 1": ["Section 1 body."],
                "section 2": ["Section 2 body."],
            },
        ),
        (
            _test_xml_nested_sections,
            {
                "parent": ["Parent body."],
                "child": ["Child body."],
                "grandchild": ["Grandchild body."],
            },
        ),
    ],
)
def test_extract_standard_section(
    section_extractor,
    xml,
    expected_sections,
):
    """
    Test the _extract_standard_section method to ensure it correctly extracts sections in standard format.
    It should handle both unnested and nested sections.
    """
    # Start with an empty section_titles_text dictionary.
    section_titles_text: dict[str, list[str]] = {}
    # Get the top-level children of the <uitspraak> node
    uitspraak_children = return_uitspraak_node_children(xml)

    # Loop through each section tag similar to how the method would be called in the actual extraction process
    for section_tag in uitspraak_children:
        section_extractor._extract_standard_section(section_tag, section_titles_text)

    assert section_titles_text == expected_sections


@pytest.mark.unit
@pytest.mark.parametrize(
    ("xml", "expected_keys", "expected_values"),
    [
        # Test case with no data, expecting empty sections
        (_test_xml_no_data, set(), []),  
        # Test case with standard format XML, expecting specific section keys and values
        (
            _test_xml_standard_format,  
            {"uitspraak.info", "procesverloop", "beslissing"},
            [
                ("uitspraak.info", "uitspraak College van Beroep zaaknummer: 14/803"),
                (
                    "procesverloop",
                    "Course of proceedings part I. Course of proceedings part II.",
                ),
                ("beslissing", "1. Decision I. 2. Decision II."),
            ],
        ),
    ],
)
def test_extract_full_text_sections_standard_format(
    section_extractor, xml, expected_keys, expected_values
):
    """
    Test the _extract_full_text_sections_standard_format method with different XML inputs.
    """
    uitspraak_children = return_uitspraak_node_children(xml)
    
    sections = section_extractor._extract_full_text_sections_standard_format(
        uitspraak_children
    )

    assert set(sections.keys()) == expected_keys
    for section_name, expected_text in expected_values:
        assert expected_text == sections[section_name]


@pytest.mark.unit
@pytest.mark.parametrize(
    "text, expected_result",
    [
        # Test case with a known title that is passed to SectionExtractor, expecting a match
        ("This is a known title", True),
        ("This is, a known title.", True),
        ("this is a known title 1", True),
        # Test case with a known title that has special characters, expecting a match
        ("$this is a known title$", True),
        ("Also a known title", True),
        ("Random Title", False),
        ("", False),
        ("   ", False),
    ],
)
def test_match_title_candidate(section_extractor, text, expected_result):
    """
    Test the _match_title_candidate method with various texts to determine if they match known section titles.
    """
    assert section_extractor._match_title_candidate(text) is expected_result


@pytest.mark.unit
@pytest.mark.parametrize(
    "text, expected",
    [
        ("1. Section title", True),
        ("2) Section title", True),
        ("10. Unknown heading", True),
        ("I. SECTION TITLE", True),
        ("II Section title", True),
        ("Title:", True),
        # Known titles are matched with any numeric prefix
        ("2.3 This is a known title", True),
        # Subsections are not title candidates unless they match known titles 
        ("2.3 Subsection not a title", False),  
        # Ends with a period and not a known title
        ("9. Het beroep is ongegrond.", False),  
        (
            "A very long sentence that should not be treated as a title because it has too many words",
            False,
        ),
        ("Look like a title", False),
        ("", False),
        ("   ", False),
    ],
)
def test_is_title_candidate(section_extractor, text, expected):
    """
    Test the _is_title_candidate method with various text inputs to determine if they are considered title candidates.
    """
    assert section_extractor._is_title_candidate(text) is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "para_xml, current_title, initial_sections, expected_title, expected_sections",
    [
        # Title candidate (numeric prefix) -> updates current_title, initializes section
        (
            "<para>1. Title</para>",
            None,
            {},
            "title",
            {"title": []},
        ),
        # Body text -> appended to active section, title unchanged
        (
            "<para>  Body text, not a title. </para>",
            "title",
            {"title": []},
            "title",
            {"title": ["Body text, not a title."]},
        ),
        # Empty para -> no changes
        (
            "<para>   </para>",
            "title",
            {"title": ["Existing line."]},
            "title",
            {"title": ["Existing line."]},
        ),
        # Body text with no active title -> ignored
        (
            "<para>Body without section</para>",
            None,
            {},
            None,
            {},
        ),
    ],
)
def test_read_para_tag(
    section_extractor,
    para_xml,
    current_title,
    initial_sections,
    expected_title,
    expected_sections,
):
    """
    Test that _read_para_tag updates current_title and section_titles_text_lines correctly.
    """
    soup = BeautifulSoup(f"<root>{para_xml}</root>", features="xml")
    para_tag = soup.find("para")
    sections = {k: v[:] for k, v in initial_sections.items()}

    returned_title = section_extractor._read_para_tag(
        para_tag,
        sections,
        current_title,
    )

    assert returned_title == expected_title
    assert sections == expected_sections


@pytest.mark.unit
@pytest.mark.parametrize(
    ("xml", "expected_keys", "expected_values"),
    [
        # Test case with no data, expecting empty sections
        (_test_xml_no_data, set(), []),
        # Short sentences that look like titles but are not, should be treated as body text under the correct section
        (
            _test_xml_short_numbered_sentences,
            {"gronden", "proceskosten"},
            [
                ("gronden", "9. Het beroep is ongegrond."),
                ("proceskosten", "Voor een proceskostenveroordeling bestaat geen aanleiding."),
            ],
        ),
        # Test case with no section titles, but can be extracted using rule-based extraction
        (
            _test_xml_no_section_titles,
            {"de procedure", "context", "beslissing"},
            [
                ("de procedure", "The proceedings. Proceedings continued."),
                ("context", "Context text."),
                ("beslissing", "2.1 The decision is"),
            ],
        ),
    ],
)
def test_extract_full_text_sections_rule_based(
    section_extractor, xml, expected_keys, expected_values
):
    """
    Test the _extract_full_text_sections_rule_based method with different XML inputs.
    """
    uitspraak_children = return_uitspraak_node_children(xml)
    sections = section_extractor._extract_full_text_sections_rule_based(uitspraak_children)

    assert set(sections.keys()) == expected_keys
    for section_name, expected_text in expected_values:
        assert expected_text == sections[section_name]


def test_has_meaningful_sections(section_extractor):
    """
    Test the _has_meaningful_sections method to ensure it correctly identifies whether the extracted sections contain meaningful content.
    """
    # Case 1: No sections
    assert not section_extractor._has_meaningful_sections({})

    # Case 2: Only <uitspraak.info> section
    assert not section_extractor._has_meaningful_sections(
        {"uitspraak.info": "Some info"}
    )

    # Case 3: Meaningful sections present
    assert section_extractor._has_meaningful_sections(
        {"Section 1": "Some text", "Section 2": "More text"}
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "xml, expected_extraction_mode",
    [
        (_test_xml_no_uitspraak_node, "None"),
        (_test_xml_standard_format, "Sections_Standard_Format"),
        (_test_xml_no_section_titles, "Sections_Rule_Based"),
        (_test_xml_no_data, "Full_Text"),
    ],
)
def test_extract_text_sections(
    section_extractor, xml, expected_extraction_mode, caplog
):
    """
    Test the extract_text_sections method with different XML inputs and expected extraction modes.
    """
    # Expected text extracted from standard format or rule-based extraction
    expected_text_standard_format = {
        "uitspraak.info": "uitspraak College van Beroep zaaknummer: 14/803",
        "procesverloop": "Course of proceedings part I. Course of proceedings part II.",
        "beslissing": "1. Decision I. 2. Decision II.",
    }
    expected_text_rule_based = {
        "de procedure": "The proceedings. Proceedings continued.",
        "context": "Context text.",
        "beslissing": "2.1 The decision is",
    }

    # Provide the raw XML file in a parsed form to the SectionExtractor instance
    section_extractor.soup_parsed_xml = BeautifulSoup(xml, features="xml")
    # Extract text by sections and test the logging output for the extraction process
    with caplog.at_level(logging.INFO):
        text_extracted = section_extractor.extract_text_sections()
    # Test if the correct extraction method was used based on the expected extraction mode
    if expected_extraction_mode == "None":
        assert text_extracted == {"full_text": ""}
        assert (
            "No <uitspraak> node found in the XML document. Returning empty text."
            in caplog.text
        )
        return
    elif expected_extraction_mode == "Sections_Standard_Format":
        assert text_extracted == expected_text_standard_format
        assert (
            "Sections extracted from full text using the standard XML structure method."
            in caplog.text
        )
    elif expected_extraction_mode == "Sections_Rule_Based":
        assert text_extracted == expected_text_rule_based
        assert (
            "Sections extracted from full text using the rule-based extraction method."
            in caplog.text
        )
    else:  # expected_extraction_mode == "Full_Text"
        assert text_extracted == {"full_text": "Test data."}
        assert (
            "Sections not found. Returning full text as a single section."
            in caplog.text
        )
