import os
import json
import logging
import subprocess
import time
import requests
from io import BytesIO
import urllib3

# Suppress insecure request warnings for linkeddata.overheid.nl
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Default configuration
GRAPHDB_PORT = 7200
GRAPHDB_IMAGE = "ontotext/graphdb:10.3.0" # Use a valid graphdb free image if required
REPOSITORY_ID = "lido"
MAX_RETRIES = 30
SLEEP_TIME = 2
IMPORT_DIR = os.path.abspath(os.path.join("data", "graphdb_import"))

# Basic repository config script for GraphDB
REPO_CONFIG_TTL = f"""
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rep: <http://www.openrdf.org/config/repository#> .
@prefix sr: <http://www.openrdf.org/config/repository/sail#> .
@prefix sail: <http://www.openrdf.org/config/sail#> .
@prefix owlim: <http://www.ontotext.com/trree/owlim#> .

[] a rep:Repository ;
    rep:repositoryID "{REPOSITORY_ID}" ;
    rdfs:label "LIDO Temporary Repository" ;
    rep:repositoryImpl [
        rep:repositoryType "graphdb:SailRepository" ;
        sr:sailImpl [
            sail:sailType "graphdb:Sail" ;
            owlim:ruleset "empty" ;
        ]
    ] .
"""

def spin_up_graphdb():
    """Start GraphDB in a Docker container."""
    os.makedirs(IMPORT_DIR, exist_ok=True)
    logging.info("Starting temporary GraphDB container...")
    try:
        # Start GraphDB container
        subprocess.run(
            [
                "docker", "run", "-d", "-p", f"{GRAPHDB_PORT}:7200", 
                "-v", f"{IMPORT_DIR}:/opt/graphdb/home/graphdb-import",
                "--name", "temp_graphdb", GRAPHDB_IMAGE
            ],
            check=True,
            capture_output=True
        )
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to start Docker container. Ensure Docker is running. Error: {e.stderr}")
        raise

    # Wait for the service to be healthy
    logging.info("Waiting for GraphDB to initialize...")
    for _ in range(MAX_RETRIES):
        try:
            res = requests.get(f"http://localhost:{GRAPHDB_PORT}/rest/repositories", timeout=2)
            if res.status_code == 200:
                logging.info("GraphDB is up and running!")
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(SLEEP_TIME)
    
    raise Exception("GraphDB failed to start within the timeout period.")

def setup_repository():
    """Creates a temporary repository in GraphDB."""
    logging.info(f"Creating repository '{REPOSITORY_ID}'...")
    url = f"http://localhost:{GRAPHDB_PORT}/rest/repositories"
    
    files = {
        'config': ('config.ttl', REPO_CONFIG_TTL, 'application/x-turtle')
    }
    
    res = requests.post(url, files=files)
    if res.status_code in (201, 204, 200):
        logging.info("Repository created successfully.")
    else:
        logging.error(f"Failed to create repository: {res.text}")
        raise Exception("Repository setup failed.")

def load_actual_data(data_url="https://linkeddata.overheid.nl/export/lido-export.ttl.gz"):
    """Downloads LIDO real data and imports it into the GraphDB repository via Server Import."""
    file_name = "lido-export.ttl.gz"
    file_path = os.path.join(IMPORT_DIR, file_name)
    
    # 1. Download data if it doesn't already exist locally
    if not os.path.exists(file_path):
        logging.info(f"Downloading LIDO data from {data_url}... (This may take a while for large files)")
        try:
            # Setting verify=False because the certificate chain for linkeddata.overheid.nl can sometimes fail locally
            response = requests.get(data_url, stream=True, verify=False)
            response.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logging.info("Download completed successfully.")
        except Exception as e:
            logging.error(f"Failed to download LIDO data: {e}")
            raise
    else:
        logging.info(f"LIDO data already exists at {file_path}. Skipping download.")

    # 2. Trigger Upload in GraphDB using streaming POST to the statements endpoint
    logging.info(f"Uploading '{file_name}' to repository '{REPOSITORY_ID}' directly via API...")
    logging.info("This process might take 15 - 45 minutes as the file contains Gigabytes of RDF data. You can watch the disk space or GraphDB logs.")
    
    # We execute curl inside the docker container to avoid Python pushing a 2.6GB file locally
    # It streams directly from the mounted volume /opt/graphdb/home/graphdb-import
    curl_command = [
        "docker", "exec", "temp_graphdb", "curl", "-X", "POST",
        "-H", "Content-Type: application/x-turtle",
        "-H", "Content-Encoding: gzip",
        "-T", f"/opt/graphdb/home/graphdb-import/{file_name}",
        f"http://localhost:7200/repositories/{REPOSITORY_ID}/statements"
    ]
    
    try:
        process = subprocess.run(curl_command, check=True, capture_output=True, text=True)
        logging.info("--> GraphDB import completed gracefully!")
    except subprocess.CalledProcessError as e:
        logging.error(f"--> GraphDB import failed! Error code: {e.returncode}\\n{e.stderr}")
        raise Exception("Data import trigger failed.")

def teardown_graphdb():
    """Stop and remove the GraphDB container."""
    logging.info("Tearing down GraphDB container...")
    subprocess.run(["docker", "stop", "temp_graphdb"], check=False, capture_output=True)
    subprocess.run(["docker", "rm", "temp_graphdb"], check=False, capture_output=True)
    logging.info("Cleanup complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        # Setup phase
        teardown_graphdb() # Clean state
        spin_up_graphdb()
        setup_repository()
        load_actual_data()
        
        logging.info(f"\\n--> Temporary SPARQL endpoint is ready at: http://localhost:{GRAPHDB_PORT}/repositories/{REPOSITORY_ID}\\n")
        logging.info("Press Ctrl+C to terminate and clean up.")
        
        # Keep alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Shutdown requested.")
        teardown_graphdb()
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        # Not tearing down here so we can debug
