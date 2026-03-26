import os
import sqlite3
import subprocess
import logging
from urllib import request
import ssl

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

LIDO_URL = "https://linkeddata.overheid.nl/export/lido-export.ttl.gz"
DATA_DIR = os.path.abspath("data")
TTL_FILE = os.path.join(DATA_DIR, "lido-export.ttl.gz")
SQLITE_DB = os.path.join(DATA_DIR, "lido_metadata.db")

# Serdi and filtering command (based on case-law-explorer Airflow DAG)
# Extracts only predicates relevant for cases to avoid parsing laws/articles
BASH_PIPELINE = f"""
set -o pipefail
gzip -dc '{TTL_FILE}' \\
| perl -pe 's|<([^>]*)>|"<".($1 =~ s/ /%20/gr).">"|ge' \\
| serdi -l -i turtle -o ntriples - \\
| grep "^<http://linkeddata.overheid.nl/terms/jurisprudentie/id/" \\
| fgrep -e "<http://purl.org/dc/terms/identifier>" \\
        -e "<http://purl.org/dc/terms/creator>" \\
        -e "<http://purl.org/dc/terms/date>" \\
        -e "<http://purl.org/dc/terms/issued>" \\
        -e "<http://psi.rechtspraak.nl/zaaknummer>" \\
        -e "<http://purl.org/dc/terms/type>" \\
        -e "<http://purl.org/dc/terms/subject>" \\
        -e "<http://linkeddata.overheid.nl/terms/heeftZaaknummer>" \\
        -e "<http://linkeddata.overheid.nl/terms/refereertAan>" \\
        -e "<http://linkeddata.overheid.nl/terms/linkt>" \\
        -e "<http://purl.org/dc/terms/hasVersion>" \\
        -e "<http://psi.rechtspraak.nl/procedure>" \\
        -e "inhoudsindicatie"
"""

def setup_sqlite():
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    # Create an ECLI oriented table storing property arrays logically
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            ecli TEXT PRIMARY KEY,
            date_publication TEXT,
            language TEXT,
            instance TEXT,
            jurisdiction_city TEXT,
            date_decision TEXT,
            case_number TEXT,
            document_type TEXT,
            procedure_type TEXT,
            domains TEXT,
            referenced_legislation_titles TEXT,
            alternative_publications TEXT,
            title TEXT,
            full_text TEXT,
            summary TEXT,
            citing TEXT,
            cited_by TEXT,
            legislations_cited TEXT,
            predecessor_successor_cases TEXT,
            url_publications TEXT,
            info TEXT,
            source TEXT
        )
    """)
    # Fast insertions setup
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = MEMORY")
    conn.commit()
    return conn

def download_ttl():
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(TTL_FILE):
        logging.info(f"Downloading {LIDO_URL} (this may take a while)...")
        # Ensure we request without checking certs if necessary, or use urllib
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = request.Request(LIDO_URL, headers={'User-Agent': 'Mozilla/5.0'})
        with request.urlopen(req, context=ctx) as response, open(TTL_FILE, 'wb') as out_file:
            out_file.write(response.read())
        logging.info("Download complete.")
    else:
        logging.info(f"File {TTL_FILE} already exists. Skipping download.")

def build_database(conn):
    logging.info("Running serdi pipeline and parsing triples...")
    # Clean the DB before full insert
    conn.execute("DELETE FROM metadata")
    
    process = subprocess.Popen(BASH_PIPELINE, shell=True, executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    current_subject = None
    row_data = {}
    
    def insert_row(data):
        if 'ecli' in data:
            ecli_val = data.get('ecli', '')
            conn.execute("""
                INSERT OR REPLACE INTO metadata (
                    ecli, date_publication, language, instance, jurisdiction_city, 
                    date_decision, case_number, document_type, procedure_type, 
                    domains, referenced_legislation_titles, alternative_publications, 
                    title, full_text, summary, citing, cited_by, legislations_cited, 
                    predecessor_successor_cases, url_publications, info, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                ecli_val,
                data.get('date_publication', ''),
                'nl',  # Default language
                data.get('instance', ''),
                '',  # jurisdiction_city not robustly parsable from URI at this stage
                data.get('date_decision', ''),
                data.get('case_number', ''),
                data.get('document_type', ''),
                data.get('procedure_type', ''),
                data.get('domains', ''),
                '',  # referenced_legislation_titles
                data.get('alternative_publications', ''),
                '',  # title
                '',  # full_text
                data.get('summary', ''),
                data.get('citing', ''),
                '',  # cited_by (can be derived later or left empty based on LIDO limitations)
                data.get('legislations_cited', ''),
                '',  # predecessor_successor_cases
                f"https://uitspraken.rechtspraak.nl/inziendocument?id={ecli_val}",  # url_publications link mapping
                '',  # info
                'Rechtspraak'  # source default
            ))

    # Parse NTriples dynamically
    count = 0
    for line in process.stdout:
        if not line.strip(): continue
        
        parts = line.strip().split(" ", 2)
        if len(parts) < 3: continue
        
        subj, predicate, obj = parts[0], parts[1], parts[2]
        # Clean object wrapping "" . 
        obj = obj.rsplit(" .", 1)[0].strip()
        if obj.startswith('"') and obj.endswith('"'): obj = obj[1:-1]
        elif obj.startswith('<') and obj.endswith('>'): obj = obj[1:-1]
        
        if subj != current_subject:
            if current_subject is not None and row_data:
                insert_row(row_data)
                count += 1
                if count % 10000 == 0:
                    logging.info(f"Inserted {count} ECLIs...")
            current_subject = subj
            row_data = {}

        # Map predicates to dictionary
        if "identifier" in predicate: row_data['ecli'] = obj
        elif "creator" in predicate: row_data['instance'] = obj
        elif "TERMS:date" in predicate or "/date>" in predicate: row_data['date_decision'] = obj
        elif "issued" in predicate: row_data['date_publication'] = obj
        elif "zaaknummer" in predicate: row_data['case_number'] = row_data.get('case_number', '') + "\\n" + obj if 'case_number' in row_data else obj
        elif "TERMS:type" in predicate or "/type>" in predicate: row_data['document_type'] = obj
        elif "subject" in predicate: row_data['domains'] = row_data.get('domains', '') + "\\n" + obj if 'domains' in row_data else obj
        elif "procedure" in predicate: row_data['procedure_type'] = obj
        elif "inhoudsindicatie" in predicate: row_data['summary'] = obj
        elif "hasVersion" in predicate: row_data['alternative_publications'] = obj
        elif "refereertAan" in predicate: row_data['legislations_cited'] = row_data.get('legislations_cited', '') + "\\n" + obj if 'legislations_cited' in row_data else obj
        elif "linkt" in predicate: row_data['citing'] = row_data.get('citing', '') + "\\n" + obj if 'citing' in row_data else obj

    # Final insert
    if current_subject is not None and row_data:
        insert_row(row_data)
        
    conn.commit()
    logging.info(f"Database build complete. Inserted {count} cases into {SQLITE_DB}.")

if __name__ == "__main__":
    download_ttl()
    conn = setup_sqlite()
    build_database(conn)
    conn.close()
