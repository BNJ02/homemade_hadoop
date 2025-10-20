from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import re
import time
import json

# --- Configuration navigateur headless ---
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# --- Lancer Chrome avec Selenium ---
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# --- Ouvrir la page ---
url = "https://tp.telecom-paris.fr/"
driver.get(url)

# --- Attendre un peu que le JS charge le tableau ---
time.sleep(5)

# --- Récupérer le texte complet visible ---
text = driver.page_source

# --- Extraire les noms de machines tp-... ---
all_hosts = sorted(set(re.findall(r'\btp-[\w-]+', text)))

# --- Filtrer et ordonner : tp-4b01-xx au début ---
priority_hosts = []
other_hosts = []

for host in all_hosts:
    # Vérifier si c'est tp-4b01-01 à tp-4b01-44
    match = re.match(r'tp-4b01-(\d+)', host)
    if match:
        num = int(match.group(1))
        if 1 <= num <= 44:
            priority_hosts.append(host)
        else:
            other_hosts.append(host)
    else:
        other_hosts.append(host)

# Trier les machines prioritaires par numéro
priority_hosts.sort(key=lambda x: int(re.search(r'tp-4b01-(\d+)', x).group(1)))

# Liste finale : prioritaires d'abord, puis les autres
hosts = priority_hosts + other_hosts

print(f"\nTotal : {len(hosts)} hôtes trouvés.")
print(f"Machines tp-4b01-xx prioritaires : {len(priority_hosts)}")

# --- Sauvegarder en JSON ---
with open('hosts.json', 'w') as f:
    json.dump(hosts, f, indent=2)

print("Liste sauvegardée dans hosts.json")

driver.quit()
