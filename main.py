import requests
import re
import sqlite3

# Function to extract the year from an ELI string
def eli_regex(x):
    pattern = r'/(\d+)/'
    regex_pattern = re.search(pattern, x)
    regex_year = regex_pattern.group(1) if regex_pattern else 'Unknown Year'
    return regex_year

# Function to extract data for a specific ID
def extract_data(id):
    url = f'https://data.bka.gv.at/ris/api/v2.6/Bundesrecht?Applikation=BrKons&Gesetzesnummer={id}'
    try:
        print(f"Fetching data for ID {id}...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        hits = int(data.get('OgdSearchResult', {}).get('OgdDocumentResults', {}).get('Hits', {}).get('#text', 0))
        if hits == 0:
            return None
        extracted_data = []
        for doc in data.get('OgdSearchResult', {}).get('OgdDocumentResults', {}).get('OgdDocumentReference', []):
            try:
                metadata = doc.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {})
                bgbl_kons = doc.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {}).get('BrKons', {})
            except:
                data_new = data.get('OgdSearchResult', {}).get('OgdDocumentResults', {}).get('OgdDocumentReference', {})
                bgbl_kons = data_new.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {}).get('BrKons', {})
                metadata = data_new.get('Data', {}).get('Metadaten', {}).get('Bundesrecht', {})
            kurztitel = metadata.get('Kurztitel', 'Kein Kurztitel gefunden')
            titel = metadata.get('Titel', 'Kein vollständiger Titel gefunden')
            eli_year_match = eli_regex(metadata.get('Eli', ''))
            abkuerzung = bgbl_kons.get('Abkuerzung', 'Keine Abkürzung gefunden')
            typ = bgbl_kons.get('Typ','Keinen Dokumententyp gefunden')
            gesetzesnummer = bgbl_kons.get('Gesetzesnummer', 'Keine Gesetzesnummer gefunden')
            extracted_data = {'ID': id, 'Kurztitel': kurztitel, 'Titel': titel, 'Eli_year': eli_year_match, 'Abkuerzung': abkuerzung, 'Typ': typ, 'Gesetzesnummer': gesetzesnummer}
            return extracted_data
        return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data for ID {id}: {e}")
        return None

# Function to initialize the database
def init_db():
    conn = sqlite3.connect('gesetze.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS gesetze (
                        ID INTEGER PRIMARY KEY,
                        Kurztitel TEXT,
                        Titel TEXT,
                        Eli_year TEXT,
                        Abkuerzung TEXT,
                        Typ TEXT,
                        Gesetzesnummer TEXT)''')
    conn.commit()
    conn.close()

# Function to get the last processed ID from the database
def get_last_processed_id():
    conn = sqlite3.connect('gesetze.db')
    cursor = conn.cursor()
    cursor.execute('SELECT MAX(ID) FROM gesetze')
    last_id = cursor.fetchone()[0]
    conn.close()
    return last_id if last_id else 10000000  # Default start ID if no data is found

# Function to save data to the database
def save_to_db(data):
    conn = sqlite3.connect('gesetze.db')
    cursor = conn.cursor()
    cursor.executemany('''INSERT OR REPLACE INTO gesetze (ID, Kurztitel, Titel, Eli_year, Abkuerzung, Typ, Gesetzesnummer)
                          VALUES (:ID, :Kurztitel, :Titel, :Eli_year, :Abkuerzung, :Typ, :Gesetzesnummer)''', data)
    conn.commit()
    conn.close()

# Initialize the database
init_db()

#Beim ersten Ausführen muss die Variable start_id auf 10000000 gesetzt werden

# Get the last processed ID and set the new start ID
last_processed_id = get_last_processed_id()
#start_id = last_processed_id + 1
start_id = 20000000
if last_processed_id > start_id:
    start_id = last_processed_id
# Process the IDs starting from the last processed ID
consecutive_empty = 0
extracted_data = []

while consecutive_empty < 200:
    data = extract_data(start_id)
    if data:
        extracted_data.append(data)
        consecutive_empty = 0
    else:
        consecutive_empty += 1
    start_id += 1

if extracted_data:
    save_to_db(extracted_data)
    print(f"Processing completed successfully. Total records: {len(extracted_data)}")
else:
    print("No data extracted.")
