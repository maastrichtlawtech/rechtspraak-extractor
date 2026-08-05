"""
Contains class to extract case text by sections from a parsed XML document.
"""
import logging
import re

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
    def __init__(self, soup_parsed_xml: BeautifulSoup) -> None:
        """
        Args:
            soup_parsed_xml: A BeautifulSoup object representing the parsed XML document.
        """
        self.soup_parsed_xml = soup_parsed_xml

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

    def _extract_text_sections_standard_format(
        self,
        uitspraak_node_children: list[Tag]
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
        # Initialize an empty dictionary to hold the extracted section text
        text_split_by_sections = {}

        for child in uitspraak_node_children:
            if child.name == "uitspraak.info":
                info_text = self._normalize_xml_text(child.get_text(" ", strip=True))
                if info_text:
                    text_split_by_sections["uitspraak.info"] = info_text
                continue

            if child.name == "section":
                title_element = child.find("title", recursive=False) or child.find("title")
                if title_element is None:
                    continue

                title_text = self._normalize_xml_text(title_element.get_text(" ", strip=True))
                if not title_text:
                    continue
                    
                # Remove title from a copy, then use remaining content as body text.
                section_text_copy = BeautifulSoup(str(child), features="xml").find("section")
                if section_text_copy is None:
                    continue
                copied_title = section_text_copy.find("title")
                if copied_title is not None:
                    copied_title.decompose()

                body_text = self._normalize_xml_text(section_text_copy.get_text(" ", strip=True))
                if body_text:
                    text_split_by_sections[title_text] = body_text
                continue

        return text_split_by_sections

    def _is_title_candidate(self, text: str) -> bool:
        """
        Checks whether a text line matches section-title rules.
            1. starts with number (e.g. 1, 1., 1))
            2. starts with Roman numeral (e.g. I., II.)
            3. ends with ':'
            4. title has <= 10 words

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
        numeric_title_pattern = re.compile(r"^\s*\d+(?:[.)]\s+|\s+)\S+")
        # Examples: "I. ONTSTAAN EN LOOP VAN HET GEDING", "II. MOTIVERING"
        roman_title_pattern = re.compile(r"^\s*[IVXLCDM]+(?:\.\s+|\s+)\S+", re.IGNORECASE)

        cleaned_title_text = self._normalize_xml_text(text)
        if not cleaned_title_text:
            return False

        words = cleaned_title_text.split()
        if len(words) > max_title_words:
            return False
        if numeric_title_pattern.match(cleaned_title_text):
            return True
        if roman_title_pattern.match(cleaned_title_text):
            return True
        if cleaned_title_text.endswith(":"):
            return True

        return False

    def _clean_title_text(self, title_text: str) -> str:
        """
        Cleans a section title by removing leading numbers, Roman numerals, and punctuation.
        
        Args:
            title_text: The title text to clean.

        Returns:
            The cleaned title text.
        """
        cleaned_title = self._normalize_xml_text(title_text)

        cleaned_title = re.sub(
            r"^(?:\d+[.)]?|[IVXLCDM]+[.]?)\s+",
            "",
            cleaned_title,
            flags=re.IGNORECASE,
        )
        cleaned_title = re.sub(r"[\d_]|[^\w\s]", "", cleaned_title)

        return self._normalize_xml_text(cleaned_title)

    def _add_line(
        self,
        section_titles_text_lines: dict[str, list[str]], 
        current_title: str | None, 
        text: str
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

    def _read_parablock_tag(
        self,
        parablock_tag: Tag,
        section_titles_text_lines: dict[str, list[str]],
        current_title: str | None,
    ) -> str | None:
        """
        Extracts title/body transitions from a <parablock> tag.
        - If the first <para> element after a <parablock> tag is a title candidate, it becomes the new current title.
        - If it is not a title candidate, the current title remains unchanged, and all <para> texts are added to the current section.
        - If any subsequent <para> element is a title candidate, it becomes the new current title, and subsequent <para> texts are added to that section.

        Args:
            parablock_tag: The <parablock> tag to process.
            section_titles_text_lines: Dictionary of section titles to list of text lines.
            current_title: The current active section title.

        Returns:
            The updated current active section title.
        
        """
        para_texts = [
            self._normalize_xml_text(para.get_text(" ", strip=True))
            for para in parablock_tag.find_all("para", recursive=False)
        ]
        para_texts = [text for text in para_texts if text]
        if not para_texts:
            return current_title

        if self._is_title_candidate(para_texts[0]):
            current_title = self._clean_title_text(para_texts[0])
            section_titles_text_lines.setdefault(current_title, [])
            for line in para_texts[1:]:
                self._add_line(section_titles_text_lines, current_title, line)
            return current_title

        for line in para_texts:
            if self._is_title_candidate(line):
                current_title = self._clean_title_text(line)
                section_titles_text_lines.setdefault(current_title, [])
            else:
                self._add_line(section_titles_text_lines, current_title, line)
        return current_title

    def _extract_text_sections_rule_based(
        self,
        uitspraak_node_children: list[Tag]
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
        current_title: str | None = None

        for child in uitspraak_node_children:
            if child.name == "parablock":
                current_title = self._read_parablock_tag(
                    child,
                    section_titles_text_lines,
                    current_title,
                )
                continue

            if child.name == "para":
                para_text = self._normalize_xml_text(child.get_text(" ", strip=True))
                if not para_text:
                    continue

                if self._is_title_candidate(para_text):
                    current_title = self._clean_title_text(para_text)
                    section_titles_text_lines.setdefault(current_title, [])
                else:
                    self._add_line(section_titles_text_lines, current_title, para_text)
                continue

            fallback_text = self._normalize_xml_text(child.get_text(" ", strip=True))
            self._add_line(section_titles_text_lines, current_title, fallback_text)

        return {
            title: self._normalize_xml_text(" ".join(lines))
            for title, lines in section_titles_text_lines.items()
            if lines
        }

    def _has_meaningful_sections(self, text_split_by_sections: dict[str, str]) -> bool:
        """
        Checks if the extracted section text contains meaningful sections.
        A section is considered meaningful if it contains more than just the <uitspraak.info> block.

        Args:
            text_split_by_sections: The dictionary with section title and text.

        Returns:
            True if the section text contains meaningful sections, False otherwise.
        """
        return bool(text_split_by_sections) and set(text_split_by_sections.keys()) != {"uitspraak.info"}

    def extract_text_sections(self) -> dict[str, str]:
        """
        Extracts case text grouped by logical sections from the parsed XML document.
        It first attempts to extract sections using the standard XML structure method.
        If no sections are found, it falls back to the rule-based extraction method.

        Returns:
            A dictionary where keys are section titles and values are the corresponding text content.
        """
        logger.info("Starting text extraction by sections from the parsed XML document using SectionExtractor.")
        # Find the <uitspraak> node in the XML document which contains the information and case text
        uitspraak_node = self.soup_parsed_xml.find("uitspraak")
        if uitspraak_node is None:
            logger.warning("No <uitspraak> node found in the XML document. Returning empty text.")
            return {'full_text': ""}

        # Check the children of the <uitspraak> node to determine if it has a standard structure or if rule-based extraction is needed
        children = [child for child in uitspraak_node.children if isinstance(child, Tag)]

        # If the <uitspraak> node has no children, return the full text of the <uitspraak> node as a single section
        if not children:
            full_text = self._normalize_xml_text(uitspraak_node.get_text(" ", strip=True))
            logger.info("Sections not found. Returning full text as a single section.")
            return {'full_text': full_text}

        # If there is a section tag then extract sections using the standard XML structure method
        if any(child.name == "section" for child in children):
            text_split_by_sections = self._extract_text_sections_standard_format(children)
            logger.info("Sections extracted from full text using the standard XML structure method.")
        # If there is no section tag, use the rule-based extraction method  
        elif any(child.name in ["parablock", "para"] for child in children):
            text_split_by_sections = self._extract_text_sections_rule_based(children)
            logger.info("Sections extracted from full text using the rule-based extraction method.")
        # If none of the above conditions are met, return the full text of the <uitspraak> node as a single section
        else:
            full_text = self._normalize_xml_text(uitspraak_node.get_text(" ", strip=True))
            logger.info("Sections not found. Returning full text as a single section.")
            return {'full_text': full_text}

        # If no sections or only the uitspraak.info was extracted using the standard XML structure method or rule-based extraction
        # This happens when no section titles were found (most likely) or the <uitspraak> node only contains the <uitspraak.info> block (unlikely)
        # Return the full text of the <uitspraak> node as a single section
        if not self._has_meaningful_sections(text_split_by_sections):
            full_text = self._normalize_xml_text(uitspraak_node.get_text(" ", strip=True))
            logger.info("Sections not found or only <uitspraak.info> extracted. Returning full text as a single section.")
            return {"full_text": full_text}

        return text_split_by_sections