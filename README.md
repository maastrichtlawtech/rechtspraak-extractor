## Rechtspraak extractor
This library contains two functions to get rechtspraak data and metadata from the API.

## Version
Python 3.9+

## Contributors

<!-- readme: contributors,gijsvd -start -->
<table>
<tr>
    <td align="center">
        <a href="https://github.com/pranavnbapat">
            <img src="https://avatars.githubusercontent.com/u/7271334?v=4" width="100;" alt="pranavnbapat"/>
            <br />
            <sub><b>Pranav Bapat</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/running-machin">
            <img src="https://avatars.githubusercontent.com/u/60750154?v=4" width="100;" alt="running-machin"/>
            <br />
            <sub><b>running-machin</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/Cloud956">
            <img src="https://avatars.githubusercontent.com/u/24865274?v=4" width="100;" alt="Cloud956"/>
            <br />
            <sub><b>Piotr Lewandowski</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/shashankmc">
            <img src="https://avatars.githubusercontent.com/u/3445114?v=4" width="100;" alt="shashankmc"/>
            <br />
            <sub><b>shashankmc</b></sub>
        </a>
    </td>
    <td align="center">
        <a href="https://github.com/gijsvd">
            <img src="https://avatars.githubusercontent.com/u/31765316?v=4" width="100;" alt="gijsvd"/>
            <br />
            <sub><b>gijsvd</b></sub>
        </a>
    </td>
</tr>
</table>
<!-- readme: contributors,gijsvd -end -->

## How to install?
<code>pip install rechtspraak_extractor</code>

## What are the functions?
<li><b>Rechtspraak Extractor</b>
<ol>
    <li><code>get_rechtspraak</code></li>
    Gets all the ECLIs and saves them in the CSV file or in-memory.
    <br>It gets, ECLI, title, summary, updated date, link.
    <li><code>get_rechtspraak_metadata</code></li>
    Gets the metadata of the ECLIs created by above function and saves them in the new CSV file or in-memory.
    <br>Link attribute that we get from the above function contains the links of ECLI metadata.
    <br>It gets instantie, datum uitspraak, datum publicatie, zaaknummer, rechtsgebieden, bijzondere kenmerken, 
    inhoudsindicatie, and vindplaatsen.
    <br>Supports two extraction methods: <code>method='api'</code> (default, fetches live from Rechtspraak API)
    and <code>method='sqlite'</code> (fetches from a local pre-built SQLite database — see below).
    <li><code>fetch_eclis_via_sqlite</code></li>
    Low-level function to look up a list of ECLIs directly from a local SQLite database and return a DataFrame.
    Requires the <code>rechtspraak-lido-sqlite</code> package to be installed and its database populated first
    (see <a href="#sqlite-method">SQLite method</a> below).
</ol> </li>

## What are the parameters?
<ol>
    <li><strong>get_rechtspraak(max_ecli=100, sd='2022-05-01', ed='2022-10-01', save_file='y')</strong></li>
    <strong>Parameters:</strong>
    <ul>
        <li><strong>max_ecli: int, optional</strong></li>
        Maximum amount of ECLIs to retrieve
        <br>Default: 100
        <li><strong>sd: date, optional, default '2022-08-01'</strong></li>
        The start publication date (yyyy-mm-dd)
        <li><strong>ed: date, optional, default current date</strong></li>
        The end publication date (yyyy-mm-dd)
        <li><strong>save_file: ['y', 'n'], default 'y'</strong></li>
        y - Save data as a CSV file in data folder
        <br>n - Save data as a dataframe in-memory
    </ul>
    <li><strong>get_rechtspraak_metadata(...)</strong></li>
    <ul>
        <li><strong>save_file: ['y', 'n'], default 'n'</strong></li>
        y - Save data as a CSV file in data folder
        <br>n - Return data as a dataframe in-memory
        <li><strong>dataframe: dataframe, optional</strong></li>
        Dataframe containing ECLIs to retrieve metadata. Cannot be combined with filename
        <li><strong>filename: string, optional</strong></li>
        CSV file containing ECLIs to retrieve metadata. Cannot be combined with dataframe
        <li><strong>method: ['api', 'sqlite'], default 'api'</strong></li>
        api - Fetch metadata live from the Rechtspraak API
        <br>sqlite - Fetch metadata from a local SQLite database (requires <code>rechtspraak-lido-sqlite</code>)
        <li><strong>sqlite_db_path: string, default 'data/lido_metadata.db'</strong></li>
        Path to the SQLite database file. Only used when <code>method='sqlite'</code>
        <li><strong>fallback_to_api: bool, default True</strong></li>
        When using <code>method='sqlite'</code>, fall back to the live API for any ECLIs not found in the database
        <li><strong>multi_threading: bool, default True</strong></li>
        Use multi-threading for API-based metadata extraction. Set to False for single-threaded execution
    </ul>
    <li><strong>fetch_eclis_via_sqlite(ecli_list, sqlite_db_path, columns)</strong></li>
    <ul>
        <li><strong>ecli_list: list[str]</strong></li>
        List of ECLI identifiers to look up
        <li><strong>sqlite_db_path: string</strong></li>
        Path to the SQLite database file produced by <code>rechtspraak-lido-sqlite</code>
        <li><strong>columns: list[str]</strong></li>
        Column names to select from the database
    </ul>
</ol>


## Examples

### Downloading ECLIs
```python
import rechtspraak_extractor as rex

# Get rechtspraak data as a DataFrame (100 ECLIs since 2022-08-01)
df = rex.get_rechtspraak(max_ecli=100, sd="2022-08-01", save_file="n")

# Save rechtspraak data directly to CSV in the data/ folder
rex.get_rechtspraak(max_ecli=100, sd="2022-08-01", save_file="y")
```

### Extracting metadata via the live API (default)
```python
# Get metadata into a DataFrame from an existing DataFrame
df_metadata = rex.get_rechtspraak_metadata(save_file="n", dataframe=df)

# Get metadata into a DataFrame from a CSV produced by get_rechtspraak
df_metadata = rex.get_rechtspraak_metadata(save_file="n", filename="rechtspraak.csv")

# Produce metadata CSV from an in-memory DataFrame
rex.get_rechtspraak_metadata(save_file="y", dataframe=df)

# Produce metadata CSV from files already in data/ (processes all files)
rex.get_rechtspraak_metadata(save_file="y")
```

- `filename` refers to a file in the `data/` folder created by `get_rechtspraak`.
- `df` is the DataFrame returned by `get_rechtspraak`.

---

### <a name="sqlite-method"></a>Extracting metadata via SQLite (offline, faster)

The SQLite method fetches metadata from a local pre-built database instead of making live API calls.
This is significantly faster for large batches and works offline.

**Prerequisite:** The [`rechtspraak-lido-sqlite`](https://github.com/maastrichtlawtech/rechtspraak-lido-sqlite) package must be installed and its database must be built locally before using this method.

```bash
pip install rechtspraak-lido-sqlite
```

After installing, follow the `rechtspraak-lido-sqlite` instructions to build the local database (typically produces a file at `data/lido.db` or a path you configure).

#### Using `get_rechtspraak_metadata` with `method='sqlite'`
```python
import rechtspraak_extractor as rex

df = rex.get_rechtspraak(max_ecli=500, sd="2025-01-01", save_file="n")

# Fetch metadata from local SQLite database
df_metadata = rex.get_rechtspraak_metadata(
    save_file="n",
    dataframe=df,
    method="sqlite",
    sqlite_db_path="data/lido.db",   # path to the database built by rechtspraak-lido-sqlite
    fallback_to_api=True,            # fall back to live API for ECLIs not found in the DB
)
```

#### Using `fetch_eclis_via_sqlite` directly
```python
from rechtspraak_extractor.rechtspraak_metadata import fetch_eclis_via_sqlite

eclis = ["ECLI:NL:HR:2023:1", "ECLI:NL:HR:2023:2"]

columns = ["ecli", "document_type", "date_decision", "instance", "full_text"]

df = fetch_eclis_via_sqlite(
    ecli_list=eclis,
    sqlite_db_path="data/lido.db",
    columns=columns,
)
```

> **Note:** If the database file does not exist or an ECLI is not found in it, `fetch_eclis_via_sqlite` returns an empty DataFrame rather than raising an error. Use `fallback_to_api=True` in `get_rechtspraak_metadata` to automatically cover missing ECLIs via the live API.


## License
[![License: Apache 2.0](https://img.shields.io/github/license/maastrichtlawtech/extraction_libraries)](https://opensource.org/licenses/Apache-2.0)

Previously under the [MIT License](https://opensource.org/licenses/MIT), as of 28/10/2022 this work is licensed under a [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0).
```
Apache License, Version 2.0

Copyright (c) 2022 Maastricht Law & Tech Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
