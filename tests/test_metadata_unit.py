"""
Unit tests for rechtspraak_metadata.py.

All tests are isolated — no real network calls, no real filesystem writes
(except tests that use tmp_path for file-write verification).
"""
from __future__ import annotations

import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from rechtspraak_extractor.rechtspraak_metadata import (
    METADATA_COLUMNS,
    ExtractTextbySectionsOption,
    extract_data_from_xml,
    fetch_eclis_via_sqlite,
    get_data_from_api,
    get_rechtspraak_metadata,
    process_metadata_fields,
)


# ---------------------------------------------------------------------------
# extract_data_from_xml
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_extract_data_from_xml_returns_bytes_on_success(sample_ecli_xml):
    mock_response = MagicMock()
    mock_response.read.return_value = sample_ecli_xml
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = extract_data_from_xml("http://fake-url")

    assert result == sample_ecli_xml


@pytest.mark.unit
def test_extract_data_from_xml_retries_and_returns_none():
    call_count = 0

    def fake_urlopen(*a, **kw):
        nonlocal call_count
        call_count += 1
        raise urllib.error.URLError("timeout")

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        with patch("time.sleep"):  # avoid real delay in tests
            result = extract_data_from_xml("http://fake-url")

    assert result is None
    assert call_count == 2  # MAX_RETRIES = 2


@pytest.mark.unit
def test_extract_data_from_xml_returns_none_on_http_error():
    with patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.HTTPError("url", 404, "Not Found", {}, None),
    ):
        with patch("time.sleep"):
            result = extract_data_from_xml("http://fake-url")

    assert result is None


# ---------------------------------------------------------------------------
# process_metadata_fields
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_process_metadata_fields_extracts_known_fields(sample_ecli_xml):
    soup = BeautifulSoup(sample_ecli_xml, features="xml")
    metadata, has_metadata = process_metadata_fields(soup, "ECLI:NL:HR:2020:1")

    assert has_metadata is True
    assert metadata.get("creator") == "Hoge Raad"
    assert metadata.get("date_decision") == "2020-01-15"
    assert metadata.get("language") == "nl"


@pytest.mark.unit
def test_process_metadata_fields_extracts_full_text(sample_ecli_xml):
    soup = BeautifulSoup(sample_ecli_xml, features="xml")

    # Test that full text is extracted correctly when extract_text_by_sections is 'no'
    metadata, has_metadata = process_metadata_fields(
        soup, 
        "ECLI:NL:HR:2020:1",
        extract_text_by_sections=ExtractTextbySectionsOption.NO.value
    )
    assert metadata.get("full_text", "") == "Full decision text here."

    # Test that full text is extracted correctly when extract_text_by_sections is 'yes'
    # As there are no sections in the sample XML, it should return a dictionary with a single key 'full_text'
    metadata, has_metadata = process_metadata_fields(
        soup, 
        "ECLI:NL:HR:2020:1", 
        extract_text_by_sections=ExtractTextbySectionsOption.YES.value
    )
    assert metadata.get("full_text", "") == {'full_text': 'Full decision text here.'}


@pytest.mark.unit
def test_process_metadata_fields_empty_xml_returns_no_metadata():
    soup = BeautifulSoup(b"<open-rechtspraak></open-rechtspraak>", features="xml")
    metadata, has_metadata = process_metadata_fields(soup, "ECLI:NL:HR:2020:99")

    assert has_metadata is False
    assert metadata == {}


# ---------------------------------------------------------------------------
# get_data_from_api
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_get_data_from_api_success(sample_ecli_xml, tmp_path):
    with patch(
        "rechtspraak_extractor.rechtspraak_metadata.extract_data_from_xml",
        return_value=sample_ecli_xml,
    ):
        result = get_data_from_api(
            "ECLI:NL:HR:2020:1",
            METADATA_COLUMNS,
            fake_headers=False,
            data_dir=str(tmp_path),
        )

    assert result is not None
    assert len(result) == len(METADATA_COLUMNS)


@pytest.mark.unit
def test_get_data_from_api_returns_none_on_network_failure(tmp_path):
    with patch(
        "rechtspraak_extractor.rechtspraak_metadata.extract_data_from_xml",
        return_value=None,
    ):
        result = get_data_from_api(
            "ECLI:NL:HR:2020:1",
            METADATA_COLUMNS,
            fake_headers=False,
            data_dir=str(tmp_path),
        )

    assert result is None
    failed_files = list(tmp_path.glob("*failed_eclis*"))
    assert len(failed_files) == 1


# ---------------------------------------------------------------------------
# fetch_eclis_via_sqlite (uses in_memory_sqlite_db fixture from conftest)
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_fetch_eclis_via_sqlite_returns_matching_row(in_memory_sqlite_db):
    result = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:HR:2020:1"],
        sqlite_db_path=in_memory_sqlite_db,
        columns=["ecli", "type", "date_decision"],
    )

    assert len(result) == 1
    assert result.iloc[0]["ecli"] == "ECLI:NL:HR:2020:1"
    assert result.iloc[0]["type"] == "Uitspraak"


@pytest.mark.unit
def test_fetch_eclis_via_sqlite_unknown_ecli_returns_empty(in_memory_sqlite_db):
    result = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:INVALID:9999:0"],
        sqlite_db_path=in_memory_sqlite_db,
        columns=["ecli", "type"],
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


@pytest.mark.unit
def test_fetch_eclis_via_sqlite_nonexistent_db_returns_empty(tmp_path):
    result = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:HR:2020:1"],
        sqlite_db_path=str(tmp_path / "does_not_exist.db"),
        columns=["ecli"],
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "columns",
    [
        ["ecli", "type"],
        ["ecli", "type", "date_decision", "language", "creator"],
    ],
)
def test_fetch_eclis_via_sqlite_various_column_sets(in_memory_sqlite_db, columns):
    result = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:HR:2020:1"],
        sqlite_db_path=in_memory_sqlite_db,
        columns=columns,
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    for col in columns:
        assert col in result.columns


# ---------------------------------------------------------------------------
# get_rechtspraak_metadata — input validation (no network needed)
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_metadata_rejects_both_dataframe_and_filename():
    df = pd.DataFrame({"id": [], "link": []})
    result = get_rechtspraak_metadata(dataframe=df, filename="file.csv", save_file="n")
    assert result is None


@pytest.mark.unit
def test_metadata_rejects_invalid_save_file():
    result = get_rechtspraak_metadata(save_file="maybe")
    assert result is None


@pytest.mark.unit
def test_metadata_rejects_empty_dataframe():
    result = get_rechtspraak_metadata(save_file="n", dataframe=pd.DataFrame())
    assert result is None


@pytest.mark.unit
def test_metadata_rejects_dataframe_with_wrong_columns():
    df = pd.DataFrame({"wrong_col": [1, 2, 3]})
    result = get_rechtspraak_metadata(save_file="n", dataframe=df)
    assert result is None


@pytest.mark.unit
def test_metadata_rejects_no_source_when_save_file_n():
    result = get_rechtspraak_metadata(save_file="n")
    assert result is None


#-----------------------------------------------------------------------------------
# Additional tests for process_metadata_fields with extract_text_by_sections
#-----------------------------------------------------------------------------------
_SAMPLE_ECLI_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<open-rechtspraak>
  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ecli="https://e-justice.europa.eu/ecli" xmlns:tr="http://tuchtrecht.overheid.nl/" xmlns:eu="http://publications.europa.eu/celex/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:bwb="bwb-dl" xmlns:cvdr="http://decentrale.regelgeving.overheid.nl/cvdr/" xmlns:psi="http://psi.rechtspraak.nl/" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
    <rdf:Description>
      <dcterms:identifier>ECLI:NL:CBB:2025:222</dcterms:identifier>
      <dcterms:format>text/xml</dcterms:format>
      <dcterms:accessRights>public</dcterms:accessRights>
      <dcterms:modified>2025-04-01T12:09:11</dcterms:modified>
      <dcterms:issued rdfs:label="Publicatiedatum">2025-03-31</dcterms:issued>
      <dcterms:publisher resourceIdentifier="http://rechtspraak.nl/">Raad voor de Rechtspraak</dcterms:publisher>
      <dcterms:language>nl</dcterms:language>
      <dcterms:creator rdfs:label="Instantie" resourceIdentifier="http://standaarden.overheid.nl/owms/terms/College_van_Beroep_voor_het_bedrijfsleven" scheme="overheid.RechterlijkeMacht">College van Beroep voor het bedrijfsleven</dcterms:creator>
      <dcterms:date rdfs:label="Uitspraakdatum">2025-04-01</dcterms:date>
      <psi:zaaknummer rdfs:label="Zaaknr">23/847</psi:zaaknummer>
      <dcterms:type rdf:language="nl" resourceIdentifier="http://psi.rechtspraak.nl/uitspraak">Uitspraak</dcterms:type>
      <psi:procedure rdf:language="nl" rdfs:label="Procedure" resourceIdentifier="http://psi.rechtspraak.nl/procedure#eersteAanlegEnkelvoudig">Eerste aanleg - enkelvoudig</psi:procedure>
      <psi:procedure rdf:language="nl" rdfs:label="Procedure" resourceIdentifier="http://psi.rechtspraak.nl/procedure#proceskostenveroordeling">Proceskostenveroordeling</psi:procedure>
      <dcterms:coverage>NL</dcterms:coverage>
      <dcterms:spatial rdfs:label="Zittingsplaats">Den Haag</dcterms:spatial>
      <dcterms:subject rdfs:label="Rechtsgebied" resourceIdentifier="http://psi.rechtspraak.nl/rechtsgebied#bestuursrecht">Bestuursrecht</dcterms:subject>
      <dcterms:references rdfs:label="Wetsverwijzing" bwb:resourceIdentifier="jci1.31:c:BWBR0035925&amp;g=2022-08-30">Uitvoeringsregeling rechtstreekse betalingen GLB</dcterms:references>
      <dcterms:references rdfs:label="Wetsverwijzing" bwb:resourceIdentifier="jci1.31:c:BWBR0018989&amp;g=2025-03-01">Uitvoeringsregeling Meststoffenwet</dcterms:references>
      <dcterms:references rdfs:label="Wetsverwijzing" bwb:resourceIdentifier="jci1.31:c:BWBR0009066&amp;g=2023-02-15">Besluit gebruik meststoffen</dcterms:references>
      <dcterms:hasVersion rdfs:label="Vindplaatsen" resourceIdentifier="http://psi.rechtspraak.nl/vindplaats">
        <rdf:list>
          <rdf:li>Rechtspraak.nl</rdf:li>
        </rdf:list>
      </dcterms:hasVersion>
    </rdf:Description>
    <rdf:Description rdf:about="http://deeplink.rechtspraak.nl/uitspraak?id=ECLI:NL:CBB:2025:222">
      <dcterms:identifier>http://deeplink.rechtspraak.nl/uitspraak?id=ECLI:NL:CBB:2025:222</dcterms:identifier>
      <dcterms:format>text/html</dcterms:format>
      <dcterms:accessRights>public</dcterms:accessRights>
      <dcterms:modified>2025-03-31T13:43:06</dcterms:modified>
      <dcterms:issued rdfs:label="Publicatiedatum">2025-04-01</dcterms:issued>
      <dcterms:publisher resourceIdentifier="http://rechtspraak.nl/">Raad voor de Rechtspraak</dcterms:publisher>
      <dcterms:language>nl</dcterms:language>
      <dcterms:title rdf:language="nl">ECLI:NL:CBB:2025:222 College van Beroep voor het bedrijfsleven , 01-04-2025 / 23/847</dcterms:title>
      <dcterms:abstract resourceIdentifier="../../rs:inhoudsindicatie" />
    </rdf:Description>
  </rdf:RDF>
  <inhoudsindicatie id="ECLI:NL:CBB:2025:222:INH" lang="nl" xml:space="preserve" xmlns="http://www.rechtspraak.nl/schema/rechtspraak-1.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
      <parablock>
        <para>GLB subsidiabele hectares; percelen in veenweidegebied met sloten. Grens tussen water en land maakt dat subsidiabele oppervlakte in 2022 kleiner is vastgesteld dan opgegeven en dan in de jaren 2009 tot en met 2021 en 2023.</para>
        <para>Besluit onvoldoende zorgvuldig voorbereid. Beroep gegrond.</para>
      </parablock>
    </inhoudsindicatie>
  <uitspraak id="ECLI:NL:CBB:2025:222:DOC" lang="nl" xml:space="preserve" xmlns="http://www.rechtspraak.nl/schema/rechtspraak-1.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xlink="http://www.w3.org/1999/xlink">
  <uitspraak.info>
    <para>uitspraak 				</para>
    <para>
      <inlinemediaobject>
        <imageobject>
          <imagedata align="center" scale="100" fileref="e1c0bd76-66ac-4011-b33f-7b895ff0bee7" depth="1" width="568" format="image/png" />
        </imageobject>
      </inlinemediaobject>
      <inlinemediaobject>
        <imageobject>
          <imagedata align="center" scale="100" fileref="605dc6a5-5e93-43b2-8d75-6c1587d47328" depth="1" width="13" format="image/png" />
        </imageobject>
      </inlinemediaobject>
    </para>
    <bridgehead role="bold">COLLEGE VAN BEROEP VOOR HET BEDRIJFSLEVEN</bridgehead>
    <para />
    <parablock>
      <para>zaaknummer: 23/847 </para>
    </parablock>
    <para />
    <bridgehead role="bold">uitspraak van de enkelvoudige kamer van 1 april 2025 in de zaak tussen<?linebreak?></bridgehead>
    <bridgehead role="bold">
      [naam 1] te [woonplaats]</bridgehead>
    <para>(gemachtigde: C. Blokland)</para>
    <para />
    <parablock>
      <para>en</para>
    </parablock>
    <para />
    <bridgehead role="bold">de minister van Landbouw, Visserij, Voedselzekerheid en Natuur</bridgehead>
    <para>(gemachtigde: mr. J. van Horsen)</para>
    <para />
    <parablock>
      <para>en</para>
    </parablock>
    <para />
    <bridgehead role="bold">de Staat der Nederlanden (ministerie van Justitie en Veiligheid) (Staat).</bridgehead>
    <para />
    <para />
  </uitspraak.info>
  <section role="procesverloop">
    <title>Procesverloop </title>
    <para />
    <parablock>
      <para>Met een besluit van 12 december 2022 heeft de minister beslist op de aanvraag van [naam 1] voor het jaar 2022 om uitbetaling van de basis- en vergroeningsbetaling op grond van de Uitvoeringsregeling rechtstreekse betalingen GLB (Uitvoeringsregeling).</para>
    </parablock>
    <para />
    <parablock>
      <para>Met een besluit van 15 maart 2023 (bestreden besluit) heeft de minister het daartegen door [naam 1] gemaakte bezwaar gedeeltelijk gegrond verklaard.</para>
    </parablock>
    <para />
    <parablock>
      <para>
        [naam 1] heeft tegen dat besluit beroep ingesteld. </para>
    </parablock>
    <para />
    <parablock>
      <para>De minister heeft een verweerschrift ingediend.</para>
    </parablock>
    <para />
    <parablock>
      <para>
        [naam 1] heeft een nader stuk ingediend en verzocht om schadevergoeding wegens overschrijding van de redelijke termijn. Daarom heeft het College de Staat als partij aangemerkt.</para>
    </parablock>
    <para />
    <parablock>
      <para>De zitting was op 20 februari 2025. Daar waren [naam 1] , de gemachtigden van partijen en voor de minister nog [naam 2] aanwezig.</para>
    </parablock>
    <para />
    <para />
    <para />
  </section>
  <section role="overwegingen">
    <title>Overwegingen </title>
    <para />
    <parablock>
      <para>
        <emphasis role="underline">Inleiding en feiten</emphasis>
      </para>
    </parablock>
    <para />
    <parablock>
      <para>1	[naam 1] heeft voor 2022 voor de uitbetaling van de basis- en vergroeningsbetaling 27 percelen grasland opgegeven met een totale subsidiabele oppervlakte van 39,06 hectare (ha). De percelen liggen in veenweidegebied en zijn van elkaar gescheiden door sloten.</para>
    </parablock>
    <para />
    <paragroup>
      <nr>2.1</nr>
      <para>De minister heeft bij de toekenning van een aantal percelen de subsidiabele oppervlakte kleiner vastgesteld. </para>
      <para />
    </paragroup>
    <paragroup>
      <nr>2.2</nr>
      <parablock>
        <para>Met het bestreden besluit heeft de minister de totale subsidiabele oppervlakte voor 2022 vastgesteld op 38,61 ha. Volgens de minister is soms de perceelgrens in de sloot ingetekend, of is sprake van niet subsidiabele elementen als een pad of bomenrij. Het gaat om de volgende percelen: </para>
        <para>perceel		aangevraagd	geconstateerd</para>
      </parablock>
      <para>1		2,29		2,28</para>
      <para>3		1,23		1,22</para>
      <para>4		0,96		0,94</para>
      <para>5		1,02		1,00</para>
      <para>8		0,78		0,76</para>
      <para>9		0,47		0,46</para>
      <para>13		1,84		1,83</para>
      <para>14		1,42		1,41</para>
      <para>15		2,13		2,12</para>
      <para>16		2,01		2,00</para>
      <para>17		1,89		1,87</para>
      <para>18		2,27		2,26</para>
      <para>19		2,86		2,84</para>
      <para>20		2,80		2,78</para>
      <para>22		1,89		1,88</para>
      <para />
    </paragroup>
    <paragroup>
      <nr>3.1</nr>
      <para>
        [naam 1] heeft aangevoerd dat hij de grens van de sloten juist heeft ingetekend, dat de minister en hij eenzelfde discussie eerder hebben gevoerd en de minister toen van zijn standpunt is teruggekomen. De minister heeft voor 2022 de totale subsidiabele oppervlakte 0,45 ha kleiner vastgesteld dan in zijn besluiten voor 2009 tot en met 2021 en voor 2023. Dat is volgens [naam 1] onterecht. De beteelbare oppervlakte van zijn percelen grasland, die in een veenweidegebied liggen en door sloten zijn omgeven, is in 20 jaar niet gewijzigd en altijd 39,09 ha geweest, ook volgens de minister in zijn besluit van 24 mei 2011 over de bedrijfstoeslag 2009. Daarover is in 2009 een discussie gevoerd die de minister nu over lijkt te willen doen. De minister heeft in de jaren daarna steeds 39,09 ha als subsidiabel vastgesteld, behalve voor 2022.</para>
      <para />
    </paragroup>
    <paragroup>
      <nr>3.2</nr>
      <para>
        [naam 1] erkent dat een bomenrij en een pad niet subsidiabel zijn, maar die beslaan maar een kleine oppervlakte. De slootgrenzen beslaan op zijn percelen dertig kilometer. In het veenweidegebied heeft de waterstand invloed op slootgrens en bij regenval kan de slootrand tijdelijk (omdat het water niet zo snel kan worden bemalen) 15 centimeter landinwaarts liggen, in totaal een oppervlakte van 0,45 hectare. Dat verklaart het verschil tussen de opgegeven en goedgekeurde oppervlakte. De door de minister gebruikte luchtfoto’s zijn op een moment genomen en zijn niet representatief. Van die foto’s kan de minister daarom niet uitgaan. De foto’s uit 2023 laten wel zien hoe het normaal gesproken is.</para>
      <para />
    </paragroup>
    <paragroup>
      <nr>3.3</nr>
      <para>De minister heeft aangevoerd dat hij bij het vaststellen van de subsidiabele oppervlakte van luchtfoto’s (zomer en winter) uit 2022 is uitgegaan en heeft mogen uitgaan. De subsidiabele oppervlakte moet elk jaar opnieuw worden vastgesteld. Mogelijk is in eerdere jaren iets over het hoofd gezien. </para>
      <para />
      <para />
      <parablock>
        <para>
          <emphasis role="bold">Beoordeling</emphasis>
        </para>
      </parablock>
      <para />
      <parablock>
        <para>4	Voor de vaststelling van het bedrag aan basisbetaling en de vergroeningsbetaling is op grond van Verordening 1307/2013<footnote-ref linkend="_94cca51b-df6a-460e-9ce0-bfdc8b9793af" /> de subsidiabele hectares landbouwareaal van belang, waaronder blijvend grasland en blijvend weiland (artikel 4, eerste lid, aanhef en onder e van Verordening 1307/2013). </para>
      </parablock>
      <para />
    </paragroup>
    <paragroup>
      <nr>5.1</nr>
      <para>Partijen zijn het er over eens dat vooral de grens tussen water en land de oorzaak is dat de subsidiabele oppervlakte kleiner is vastgesteld dan opgegeven. </para>
      <para />
    </paragroup>
    <paragroup>
      <nr>5.2</nr>
      <para>De minister heeft terecht aangevoerd dat hij de subsidiabele oppervlakte elk jaar opnieuw moet vaststellen. De minister heeft echter ter zitting verklaard dat hij onderzoek heeft nagelaten of, zoals [naam 1] stelt, de gebruikte luchtfoto’s door toevallige weersomstandigheden een vertekend beeld geven van de (normale) slootgrenzen. De minister baseert zich volledig op die luchtfoto’s en besteedt geen (kenbare) aandacht aan het gegeven dat dezelfde discussie in 2009 ook is gevoerd en dat de minister [naam 1] toen en voor alle jaren daarna, behalve 2022, tegemoet is gekomen. Het verschil in oppervlakte is verder van zeer geringe omvang. Gelet hierop is het College van oordeel dat het bestreden besluit onvoldoende zorgvuldig is voorbereid en niet deugdelijk is gemotiveerd en daardoor in strijd is met de artikelen 3:2 en 7:12 van de Algemene wet bestuursrecht (Awb). </para>
      <para />
    </paragroup>
    <paragroup>
      <nr>5.3</nr>
      <para>Het College ziet aanleiding om het beroep gegrond te verklaren, het bestreden besluit te vernietigen en te bepalen dat de minister de aanvraag voor het jaar 2022 om uitbetaling van de basis- en vergroeningsbetaling, onder herroeping van zijn besluit van 12 december 2022 dient te honoreren op basis van een totale subsidiabele oppervlakte van 39,06 ha. Het College ziet daartoe aanleiding, omdat naar zijn oordeel achteraf niet meer valt vast te stellen dat voor 2022 de subsidiabele oppervlakte kleiner is dan die van eerdere en latere jaren. De minister heeft niet aangevoerd dat de situatie in 2023 anders was dan in 2022. Ter voorlichting van partijen merkt het College nog op dat deze uitspraak de minister de ruimte biedt deze subsidiabele oppervlakte in de toekomst gemotiveerd lager vast te stellen.</para>
      <para />
      <parablock>
        <para>6	Het College zal de minister veroordelen in de door [naam 1] in beroep gemaakte proceskosten in beroep. Deze stelt het College op grond van het Besluit proceskosten bestuursrecht (Bpb) voor de door een derde beroepsmatig verleende rechtsbijstand vast op € 1.814,-  (1 punt voor het beroepschrift en 1 punt voor de zitting met een waarde per punt van € 907,- en een wegingsfactor 1). De door [naam 1] opgegeven verletkosten komen niet voor vergoeding in aanmerking, omdat hij niet aannemelijk heeft gemaakt dat hij die daadwerkelijk heeft gemaakt. De minister moet ook het betaalde griffierecht vergoeden.</para>
        <para>
          <emphasis role="underline">Redelijke termijn</emphasis>
        </para>
      </parablock>
      <para />
    </paragroup>
    <paragroup>
      <nr>7.1</nr>
      <para>
        [naam 1] verzoekt om immateriële schadevergoeding in verband met overschrijding van de redelijke termijn. In deze zaak geldt als uitgangspunt dat de bezwaar- en beroepsfase samen niet langer mogen duren dan twee jaar. Uitgangspunt voor de schadevergoeding is een tarief van € 500,- per half jaar dat de redelijke termijn is overschreden, waarbij het totaal van de overschrijding naar boven wordt afgerond.</para>
      <para />
    </paragroup>
    <paragroup>
      <nr>7.2</nr>
      <para>De termijn is begonnen op 14 december 2022, de datum waarop de minister het bezwaarschrift heeft ontvangen. Dit betekent dat ten tijde van deze uitspraak de redelijke termijn van twee jaar met minder dan een half jaar is overschreden, zodat [naam 1] recht heeft op een schadevergoeding van € 500,-. De overschrijding is volledig toe te rekenen aan het College. Het College zal daarom op de voet van artikel 8:88 van de Awb de Staat veroordelen tot betaling van een bedrag van € 500,- aan [naam 1] .</para>
      <para />
    </paragroup>
    <paragroup>
      <nr>7.3</nr>
      <para>De Staat moet aan [naam 1] de proceskosten vergoeden die hij heeft gemaakt voor het indienen van het verzoek om schadevergoeding. Deze stelt het College op grond van het Bpb vast op € 453,50 (1 punt voor het verzoek, met een waarde van € 907,- en een wegingsfactor 0,5) voor verleende rechtsbijstand.</para>
      <para />
      <para />
    </paragroup>
  </section>
  <section role="beslissing">
    <title>Beslissing </title>
    <para />
    <parablock>
      <para>Het College:  </para>
    </parablock>
    <itemizedlist mark="-">
      <listitem>
        <para>verklaart het beroep gegrond;</para>
      </listitem>
      <listitem>
        <para>vernietigt het bestreden besluit van 15 maart 2023; </para>
      </listitem>
      <listitem>
        <para>bepaalt dat de minister een nieuwe beslissing neemt op het bezwaar met inachtneming van deze uitspraak; </para>
      </listitem>
      <listitem>
        <para>draagt de minister op het griffierecht van € 184,- aan [naam 1] te vergoeden;</para>
      </listitem>
      <listitem>
        <para>veroordeelt de minister in de proceskosten van [naam 1] tot een bedrag van € 1814,-;</para>
      </listitem>
      <listitem>
        <para>veroordeelt de Staat tot betaling van € 500,- aan [naam 1] voor immateriële schade;</para>
      </listitem>
      <listitem>
        <para>veroordeelt de Staat in de proceskosten van [naam 1] tot een bedrag van € 453,50.</para>
      </listitem>
    </itemizedlist>
    <para />
    <para />
    <parablock>
      <para>Deze uitspraak is gedaan door mr. R.C. Stam in aanwezigheid van mr. J.W.E. Pinckaers, griffier. De beslissing is in het openbaar uitgesproken op 1 april 2025.</para>
    </parablock>
    <para />
    <para />
    <para />
    <parablock>
      <para>R.C. Stam 	w.g. J.W.E. Pinckaers</para>
      <para>de voorzitter is verhinderd </para>
      <para>de uitspraak te ondertekenen</para>
    </parablock>
  </section>
  <footnote id="_94cca51b-df6a-460e-9ce0-bfdc8b9793af" label="1">
    <para>Verordening (EU) nr. 1307/2013 van het Europees Parlement en de Raad van 17 december 2013 tot vaststelling van voorschriften voor rechtstreekse betalingen aan landbouwers in het kader van de steunregelingen van het gemeenschappelijk landbouwbeleid</para>
  </footnote>
</uitspraak>
</open-rechtspraak>
"""

@pytest.mark.unit
def test_process_metadata_fields_extracts_full_text_test(caplog):
    soup = BeautifulSoup(_SAMPLE_ECLI_XML, features="xml")

    # Test that full text is extracted correctly when extract_text_by_sections is 'no'
    metadata, has_metadata = process_metadata_fields(
        soup, 
        "ECLI:NL:HR:2020:1",
        extract_text_by_sections=ExtractTextbySectionsOption.NO.value
    )
    # assert metadata.get("full_text", "") == "Full decision text here."

    # Test that full text is extracted correctly when extract_text_by_sections is 'yes'
    # As there are no sections in the sample XML, it should return a dictionary with a single key 'full_text'
    import logging
    with caplog.at_level(logging.INFO):
        metadata, has_metadata = process_metadata_fields(
            soup, 
            "ECLI:NL:HR:2020:1", 
            extract_text_by_sections=ExtractTextbySectionsOption.YES.value
        )
    # assert metadata.get("full_text", "") == {'full_text': 'Full decision text here.'}