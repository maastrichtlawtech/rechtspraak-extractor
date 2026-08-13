"""
Contains class to extract case text by sections from a parsed XML document.
"""

import logging
import re
from typing import Optional

from bs4 import BeautifulSoup
from bs4.element import Tag

logger = logging.getLogger(__name__)


class SectionExtractor:
    """
    Extracts case text grouped by logical sections from a parsed XML document.

    This class provides two methods for extracting text:
        1. Standard XML structure extraction, which is suitable for documents that follow the standard Rechtspraak XML structure.
        2. Rule-based extraction, which identifies section titles based on specific patterns and rules.
    The extracted text is returned as a dictionary where keys are section titles and values are the corresponding text content.
    """

    def __init__(
        self,
        soup_parsed_xml: BeautifulSoup,
        known_section_titles: Optional[set[str]] = None,
    ) -> None:
        """
        Args:
            soup_parsed_xml: A BeautifulSoup object representing the parsed XML document.
            known_section_titles: Optional set of known section titles used to assist
                section detection.
        """
        self.soup_parsed_xml = soup_parsed_xml
        # Clean the known section titles for improved matching
        # Each title candidate found in the XML is similarly cleaned before matching against this set
        self.known_section_titles = {
            self._clean_title_text(title) for title in known_section_titles or set()
        }

    @staticmethod
    def _normalize_xml_text(text: str) -> str:
        """
        Normalizes XML text by collapsing repeated whitespace.

        Args:
            text: The text to normalize.

        Returns:
            The normalized text.
        """
        return " ".join(text.split())

    def _get_section_title(self, section_tag: Tag) -> str:
        """
        Resolves the title for a <section> tag using fallback rules.

        Priority:
            1. If there is <title> text inside <section> then it is used as the section title
            2. If not then the role attribute on <section>
            3. If not then section is named as "no_section_title_found"

        Args:
            section_tag: The <section> tag to read

        Returns:
            The resolved section title.
        """
        title_element = section_tag.find("title", recursive=False)
        if title_element is not None:
            title_text = self._normalize_xml_text(
                title_element.get_text(" ", strip=True)
            )
            if title_text:
                return title_text

        role_value = section_tag.get("role")
        if role_value:
            role_text = self._normalize_xml_text(str(role_value))
            if role_text:
                return role_text

        return "no_section_title_found"

    def _clean_title_text(self, title_text: str) -> str:
        """
        Cleans a section title by removing leading numbers, Roman numerals, and punctuation and lowercasing the text.

        Args:
            title_text: The title text to clean.

        Returns:
            The cleaned title text.
        """
        cleaned_title = self._normalize_xml_text(title_text)

        # Remove leading section/subsection numbering (e.g. "1", "1.1", "1.2.3", "IV", "IV.I", "II.3", "2)")
        cleaned_title = re.sub(
            r"^\s*(?:(?:\d+|[IVXLCDM]+)(?:\.(?:\d+|[IVXLCDM]+))*[.)]?)\s+",
            "",
            cleaned_title,
            flags=re.IGNORECASE,
        )
        # Remove underscores and non-alphanumeric characters (except whitespace)
        cleaned_title = re.sub(r"_|[^\w\s]", "", cleaned_title)

        return self._normalize_xml_text(cleaned_title).lower()

    def _add_line(
        self,
        section_titles_text_lines: dict[str, list[str]],
        current_title: Optional[str],
        text: str,
    ) -> None:
        """
        Appends non-empty normalized text to the active section.

        Args:
            section_titles_text_lines: Dictionary of section titles to list of text lines.
            current_title: The current active section title.
            text: The text to append.
        """
        if current_title is None:
            return

        cleaned_text = self._normalize_xml_text(text)
        if cleaned_text:
            section_titles_text_lines[current_title].append(cleaned_text)

    def _extract_standard_section(
        self,
        section_tag: Tag,
        section_titles_text_lines: dict[str, list[str]],
    ) -> None:
        """
        Extract one standard section and recursively extract its child sections.
            - Read text from the current <section> tag, excluding any nested <section> tags and the <title> tag.
            - Handles nested sections by recursively calling itself for each child <section> tag.
            - Updates the section_titles_text_lines dictionary with the extracted text lines under their respective section titles.

        Args:
            section_tag: The <section> tag to extract.
            section_titles_text_lines: Dictionary of section titles to list of text lines.
        """
        # Get the title of the current section and initialize its entry in the dictionary
        current_title = self._clean_title_text(self._get_section_title(section_tag))
        section_titles_text_lines.setdefault(current_title, [])
        # Get the section text
        section_text = BeautifulSoup(str(section_tag), features="xml").find(
            "section"
        )

        if section_text is not None:
            # Remove the <title> tag from the section text to avoid duplication in the body text
            copied_title = section_text.find("title", recursive=False)
            if copied_title is not None:
                copied_title.decompose()

            # Child sections are extracted separately and must not be duplicated in
            # the parent section's body.
            for nested_section in section_text.find_all(
                "section", recursive=False
            ):
                nested_section.decompose()

            body_text = self._normalize_xml_text(
                section_text.get_text(" ", strip=True)
            )
            self._add_line(section_titles_text_lines, current_title, body_text)

        # Recursively extract text from any nested <section> tags within the current section
        for nested_section in section_tag.find_all("section", recursive=False):
            self._extract_standard_section(
                nested_section,
                section_titles_text_lines,
            )

    def _extract_full_text_sections_standard_format(
        self, uitspraak_node_children: list[Tag]
    ) -> dict[str, str]:
        """
        Extracts case text grouped by logical XML sections.
            - It is designed for XML documents that follow the standard Rechtspraak XML structure.
            - Most documents follow this structure after the introduction of ECLI in 2013
        (https://www.rechtspraak.nl/binaries/_rts_1768910542320/content/assets/ivo/wi/ivo-wi-technische-documentatie-open-data-van-de-rechtspraak.pdf).

        Args:
            uitspraak_node_children: The children of the <uitspraak> node.

        Returns:
            A dict mapping section names to cleaned text which includes:
                - "uitspraak.info" when that block is present
                - each <section> keyed by its <title> value
            If there is no data or if the <uitspraak> node has no info and sections, returns an empty dictionary.
        """
        # Initialize dictionary of section titles to list of text lines and current active title
        section_titles_text_lines: dict[str, list[str]] = {}
        for child in uitspraak_node_children:
            if child.name == "uitspraak.info":
                # Extract text from <uitspraak.info> and append it to the "uitspraak.info" section in the accumulator
                info_text = self._normalize_xml_text(child.get_text(" ", strip=True))
                if info_text:
                    section_titles_text_lines.setdefault("uitspraak.info", []).append(
                        info_text
                    )
                continue

            if child.name == "section":
                self._extract_standard_section(child, section_titles_text_lines)
                continue

        # After processing all children, join the lines for each section and normalize the text before returning the final dictionary
        return {
            title: self._normalize_xml_text(" ".join(lines))
            for title, lines in section_titles_text_lines.items()
            if lines
        }

    def _match_title_candidate(self, text: str) -> bool:
        """
        Checks whether text matches a known section title.

        Cleaning steps before matching to improve match rate:
            - keep only letters and spaces
            - remove numbers, punctuation, underscores, and symbols

        Args:
            text: The text string to check.

        Returns:
            True if the text matches a known section title, False otherwise.
        """
        # Clean the text by removing numbers, punctuation, and underscores, and converting to lowercase
        cleaned_text = re.sub(r"[\d_]|[^\w\s]", "", text.lower())
        cleaned_text = self._normalize_xml_text(cleaned_text)
        if not cleaned_text:
            return False

        return cleaned_text in self.known_section_titles

    def _is_title_candidate(self, text: str) -> bool:
        """
        Checks whether a text line matches section-title rules.
            1. matches with a known section title
            2. title has <= 10 words
            3. starts with number (e.g. 1, 1., 1))
            4. starts with Roman numeral (e.g. I., II.)
            5. ends with ':'

        Args:
            text: The text string to check.

        Returns:
            True if the text is a section title candidate, False otherwise.
        """
        # Constants for rule based title detection
        # Maximum number of words allowed in a title.
        # In some cases, non-section titles can agree to the rules below but are too long to be a title.
        max_title_words = 10
        # Examples: "1 De procedure", "1. De procedure", "2) De procedure"
        numeric_title_pattern = re.compile(r"^\s*\d+(?:[.)]\s|\s)\S")
        # Examples: "I. ONTSTAAN EN LOOP VAN HET GEDING", "II. MOTIVERING"
        roman_title_pattern = re.compile(r"^\s*[IVXLCDM]+(?:\.\s|\s)\S", re.IGNORECASE)

        normalized_title_text = self._normalize_xml_text(text)
        if not normalized_title_text:
            return False

        if self._match_title_candidate(normalized_title_text):
            return True
        words = normalized_title_text.split()
        if len(words) > max_title_words:
            return False
        if normalized_title_text.endswith(":"):
            return True
        # Short numbered sentences occur frequently in older Rechtspraak XML.
        # Sentence-ending punctuation is a strong signal that the paragraph is
        # body text unless it matched a known title above.
        if normalized_title_text.endswith((".", "!", "?", ";", ",")):
            return False
        if numeric_title_pattern.match(normalized_title_text):
            return True
        return bool(roman_title_pattern.match(normalized_title_text))

    def _read_para_tag(
        self,
        para: Tag,
        section_titles_text_lines: dict[str, list[str]],
        current_title: Optional[str],
    ) -> Optional[str]:
        """
        Extracts title or body from a <para> tag.
        - If the <para> element is a title candidate, it becomes the new current title.
        - If it is not a title candidate, the current title remains unchanged, and the <para> text is added to the current section.

        It updates the section_titles_text_lines dictionary with the extracted text lines under their respective section titles.

        Args:
            para: The <para> tag to process.
            section_titles_text_lines: Dictionary of section titles to list of text lines.
            current_title: The current active section title.

        Returns:
            The updated current active section title.

        """
        # Get the normalized text of the <para> element and skip if it's empty
        para_text = self._normalize_xml_text(para.get_text(" ", strip=True))
        if not para_text:
            return current_title

        # If the <para> text is a title candidate, set it as the current title and initialize its entry in the dictionary
        # Otherwise, add the text to the current section
        if self._is_title_candidate(para_text):
            current_title = self._clean_title_text(para_text)
            section_titles_text_lines.setdefault(current_title, [])
        else:
            self._add_line(section_titles_text_lines, current_title, para_text)

        return current_title

    def _extract_full_text_sections_rule_based(
        self, uitspraak_node_children: list[Tag]
    ) -> dict[str, str]:
        """
        Extracts text by section titles from an XML <uitspraak> node.

        Args:
            uitspraak_node_children: The children of the <uitspraak> node.

        Returns:
            Dict where key is section title and value is text under that title.
        """
        # Initialize an empty dictionary to hold the extracted section text and a variable to track the current section title
        section_titles_text_lines: dict[str, list[str]] = {}
        current_title: Optional[str] = None

        for child in uitspraak_node_children:
            if child.name == "parablock":
                # Process each direct <para> in the <parablock> through the para reader
                para_tags = child.find_all("para", recursive=False)
                for para in para_tags:
                    current_title = self._read_para_tag(
                        para, section_titles_text_lines, current_title
                    )

            elif child.name == "para":
                current_title = self._read_para_tag(
                    child, section_titles_text_lines, current_title
                )

            else:
                # For any other child elements that are not footnotes, treat their text as fallback text
                # Footnotes contain additional information which is not part of the main case text
                if child.name != "footnote":
                    fallback_text = self._normalize_xml_text(
                        child.get_text(" ", strip=True)
                    )
                    self._add_line(section_titles_text_lines, current_title, fallback_text)

        # After processing all children, join the lines for each section and normalize the text before returning the final dictionary
        return {
            title: self._normalize_xml_text(" ".join(lines))
            for title, lines in section_titles_text_lines.items()
            if lines
        }

    def _has_meaningful_sections(self, text_split_by_sections: dict[str, str]) -> bool:
        """
        Checks if the extracted section text contains meaningful sections.
        Text split by sections is considered meaningful if it contains more than just the <uitspraak.info> block.

        Args:
            text_split_by_sections: The dictionary with section title and text.

        Returns:
            True if the section text contains meaningful sections, False otherwise.
        """
        return bool(text_split_by_sections) and set(text_split_by_sections.keys()) != {
            "uitspraak.info"
        }

    def extract_text_sections(self) -> dict[str, str]:
        """
        Extracts case text grouped by logical sections from the parsed XML document.
        It first attempts to extract sections using the standard XML structure method.
        If no sections are found, it falls back to the rule-based extraction method.

        Returns:
            A dictionary where keys are section titles and values are the corresponding text content.
        """
        logger.info(
            "Starting text extraction by sections from the parsed XML document using SectionExtractor."
        )
        # Find the root node in the XML document which contains the case information and case text
        # It is most often <uitspraak> or occasionally <conclusie>
        uitspraak_node = self.soup_parsed_xml.find("uitspraak")
        if uitspraak_node is None:
            uitspraak_node = self.soup_parsed_xml.find("conclusie")
            if uitspraak_node is None:
                logger.warning(
                    "No <uitspraak> or <conclusie> node found in the XML document. Returning empty text."
                )
                return {"full_text": ""}

        # Check the children of the <uitspraak> node to determine if it has a standard structure or if rule-based extraction is needed
        children = [
            child for child in uitspraak_node.children if isinstance(child, Tag)
        ]

        # If the <uitspraak> node has no children, return the full text of the <uitspraak> node as a single section
        if not children:
            full_text = self._normalize_xml_text(
                uitspraak_node.get_text(" ", strip=True)
            )
            logger.info("Sections not found. Returning full text as a single section.")
            return {"full_text": full_text}

        # If there is a section tag then extract sections using the standard XML structure method
        if any(child.name == "section" for child in children):
            text_split_by_sections = self._extract_full_text_sections_standard_format(
                children
            )
            logger.info(
                "Sections extracted from full text using the standard XML structure method."
            )
        # If there is no section tag, use the rule-based extraction method
        elif any(child.name in ["parablock", "para"] for child in children):
            text_split_by_sections = self._extract_full_text_sections_rule_based(children)
            logger.info(
                "Sections extracted from full text using the rule-based extraction method."
            )
        # If none of the above conditions are met, return the full text of the <uitspraak> node as a single section
        else:
            full_text = self._normalize_xml_text(
                uitspraak_node.get_text(" ", strip=True)
            )
            logger.info("Sections not found. Returning full text as a single section.")
            return {"full_text": full_text}

        # If no sections or only the uitspraak.info was extracted using the standard XML structure method or rule-based extraction
        # This happens when no section titles were found (most likely) or the <uitspraak> node only contains the <uitspraak.info> block (unlikely)
        # Return the full text of the <uitspraak> node as a single section
        if not self._has_meaningful_sections(text_split_by_sections):
            full_text = self._normalize_xml_text(
                uitspraak_node.get_text(" ", strip=True)
            )
            logger.info(
                "Sections not found or only <uitspraak.info> extracted. Returning full text as a single section."
            )
            return {"full_text": full_text}

        return text_split_by_sections
