# Guide d'utilisation `benchmark_warc.py` & `amdahl_analysis.ipynb`

Ce document explique comment exécuter des campagnes d'expériences avec `benchmark_warc.py` et comment analyser les résultats dans le notebook `amdahl_analysis.ipynb` pour illustrer la loi d'Amdahl.

---

## 0. Contexte réseau

Ce guide suppose l'accès au réseau pédagogique de Télécom Paris. Les scripts communiquent directement avec les machines de l'école (dans `hosts.json`, mais modifiable pour tout autre parc de machines) assurez-vous d'être connecté au réseau interne de l'école ou via le VPN institutionnel avant de lancer les commandes.

---

## 1. Pré-requis

- Accès SSH aux machines du cluster (clé déjà installée pour l'utilisateur, ici `blepourt-25`).
- `client.py` et `serveur.py` présents à la racine du projet (le script les synchronise automatiquement).
- Python 3.x et les dépendances standards (`pandas`, `numpy`, `matplotlib`, `nbformat` si besoin pour le notebook).
- Un fichier CSV généré par `benchmark_warc.py` (non nécessaire pour l'exécution, uniquement pour le notebook).

---

## 2. Commande de base

```bash
python benchmark_warc.py \
  --ssh-user blepourt-25 \
  --machine-counts 1,3,5,10 \
  --map-max-lines 50000 \
  --timeout 600 \
  --results-csv warc_speedup
```

Cette commande exécute 4 campagnes (1, 3, 5 et 10 machines) avec 10 splits WARC par défaut, limite chaque worker à 50 000 lignes, et dépose les résultats dans `warc_speedup_DATE.csv`.

---

## 3. Options disponibles

| Option | Description | Défaut |
|--------|-------------|--------|
| `--master HOST` | Machine master (`serveur.py`). | `tp-4b01-10` |
| `--host-pool HOSTS` | Liste des workers séparés par des virgules. | `tp-4b01-11` … `tp-4b01-20` |
| `--machine-counts N1,N2,...` | Nombre de machines à activer par campagne. | `1,3,5,10` |
| `--total-workers N` | Nombre total de workers/splits. Idéal pour 30 splits : `--total-workers 30`. | `10` |
| `--warc-dir DIR` & `--warc-template PATTERN` | Répertoire & motif des WARC. | `/cal/commoncrawl` & `CC-MAIN-20230320083513-20230320113513-{index:05d}.warc.wet` |
| `--warc-offset K` | Décalage d’index (0 ⇒ `00000`, 1 ⇒ `00001`, etc.). | `0` |
| `--map-max-lines L` | Nombre maximum de lignes lues par worker pendant la phase map (`client.py`). Permet de dimensionner la charge. | `None` |
| `--map-lines-lists SPEC` | Quand `--map-max-lines` est omis, permet de définir des listes de lignes par `machine_count` (ex. `1:10000,20000;10:50000`). | `None` |
| `--map-lines-all LIST` | Limites communes appliquées à tous les `machine_count` si `--map-max-lines` est omis et qu’il n’existe pas d’entrée dans `--map-lines-lists`. | `None` |
| `--runs-per-count R` | Répéter chaque configuration (machine count) `R` fois pour moyenner les résultats. | `1` |
| `--control-port`, `--shuffle-port-base` | Ports utilisés par master/clients. | `5374`, `6200` |
| `--remote-python BIN` | Interpréteur python sur les machines distantes. | `python3` |
| `--remote-root PATH` | Répertoire où copier/chercher `serveur.py` & `client.py`. | `~` |
| `--ssh-user USER` | Nom d’utilisateur SSH. | `$SSH_USER` ou `$USER` |
| `--ssh-key PATH` | Clé privée pour SSH. | S/O |
| `--ssh-args "args"` | Options supplémentaires SSH (s’ajoutent à `-o BatchMode=yes …`). | S/O |
| `--skip-sync` | Ne pas synchroniser `client.py`/`serveur.py` avant la campagne. | `False` |
| `--dry-run` | Affiche les commandes sans les exécuter. | `False` |
| `--verbose` | Log détaillé des commandes SSH/SCP. | `False` |
| `--sleep-after-master S` | Pause après lancement du master (s). | `2.0` |
| `--timeout T` | Temps max par campagne (s). | `900` |
| `--results-csv FILE` | Fichier CSV append pour consigner les mesures. | `None` |

> Priorité des limites de lignes : `--map-max-lines` > `--map-lines-lists` > `--map-lines-all`.

---

## 4. Contrôle automatique des hôtes

Avant chaque campagne, `benchmark_warc.py` vérifie la disponibilité du master et des workers listés :

- un `ping` (1,5 s de délai par hôte) est envoyé à chaque machine
- les hôtes qui ne répondent pas sont automatiquement retirés du `host-pool` et signalés dans la console
- le script continue tant qu’il reste suffisamment de machines pour couvrir les `machine_count` demandés **sinon** il échoue explicitement
- si l’outil `ping` est absent, aucun filtrage n’est effectué (les hôtes sont alors utilisés “tels quels”)

⚠️ Pensez à ajuster votre `--host-pool` si le filtrage retire des machines essentielles, ou à vérifier la connectivité réseau avant de lancer une campagne longue.

---

## 5. Exemples complets

### 5.1 30 splits, 7 campagnes, 3 répétitions

```bash
python benchmark_warc.py \
  --ssh-user blepourt-25 \
  --total-workers 30 \
  --machine-counts 1,3,5,10,15,20,30 \
  --map-max-lines 1000000 \
  --runs-per-count 3 \
  --timeout 900 \
  --results-csv warc_speedup_DATE
```

### 5.2 Balayer plusieurs tailles de splits

```bash
python benchmark_warc.py \
  --ssh-user blepourt-25 \
  --total-workers 10 \
  --machine-counts 1,3,5,10 \
  --map-lines-lists "1:5000,20000;5:5000,20000;10:20000" \
  --runs-per-count 2 \
  --timeout 900 \
  --results-csv warc_speedup_multiscale
```

> Ici, chaque `machine_count` est exécuté pour plusieurs limites de lignes, ce qui permet d’analyser l’impact de la quantité de données.
> Détail de la syntaxe `--map-lines-lists` :
>
>- 1:5000,20000 ⇒ pour 1 machine, lancer deux essais avec 5 000 et 20 000 lignes max par worker.
>- 5:5000 ⇒ pour 5 machines, un essai à 5 000 lignes max.
>- 10:20000 ⇒ pour 10 machines, un essai à 20 000 lignes max.

### 5.3 Limites communes à toutes les configurations

```bash
python benchmark_warc.py \
  --ssh-user blepourt-25 \
  --total-workers 10 \
  --machine-counts 1,3,5,10 \
  --map-lines-all 5000,20000 \
  --runs-per-count 2 \
  --timeout 900 \
  --results-csv warc_speedup_all
```

Tous les `machine_count` sont testés avec 5 000 puis 20 000 lignes (utile pour dégager une 3ᵉ dimension uniforme).

### 5.4 Simulation (dry-run)

```bash
python benchmark_warc.py \
  --ssh-user blepourt-25 \
  --total-workers 5 \
  --machine-counts 1 \
  --dry-run --verbose
```

> Affiche l’intégralité des commandes sans lancer le cluster.

---

## 6. Structure du CSV de sortie

Chaque ligne correspond à une campagne (n machines) et comporte :

- `machine_count` : nombre de machines distinctes utilisées.
- `run_iteration` : numéro d'essai pour ce `machine_count` (1..R).
- `map_max_lines` : limite de lignes utilisée (vide si aucune limitation).
- `elapsed_seconds` : durée totale.
- `speedup` : ratio temps_1_machine / temps_N (si calculé).
- `serial_fraction` : estimation `f` à partir de la loi d’Amdahl (pour N > 1).
- `status` : `ok` ou `failed`.
- `notes` : commentaire (inclut par défaut la prédiction d’Amdahl).

Les fichiers sont appendés : plusieurs campagnes peuvent cohabiter dans le même CSV.

---

## 7. Exploitation dans `amdahl_analysis.ipynb`

1. **Définir le fichier CSV** : dans la première cellule, mettre à jour `CSV_PATH` (ex. `Path('warc_speedup_DATE.csv')`).
2. **Exécuter toutes les cellules** (`Run All`). Les étapes :
   - Chargement des data (`pd.read_csv` + nettoyage de la colonne `notes`).
   - Filtrage des campagnes réussies (`status == "ok"`).
   - Calcul du speedup observé et régression sur la fraction sérielle `f`.
   - Graphiques : speedup observé vs prédiction d’Amdahl + temps total.
   - Interprétation (markdown) rappelant pourquoi les gains plafonnent.
3. **Exporter si besoin** : `File > Download > HTML` pour partager la figure et l’analyse.

---

## 8. Conseils / bonnes pratiques

- **Toujours synchroniser** (`--skip-sync` absent) après modification de `client.py`/`serveur.py`.
- **Surveiller les logs** : `~/mapreduce_master.log` et `~/mapreduce_worker_X.log` pour diagnostiquer un `status=failed`.
- **Ajuster la charge** : `--total-workers`, `--map-max-lines` ou `--warc-offset` permettent d’obtenir des jobs plus longs et significatifs pour l’étude d’Amdahl.
- **Contrôle des hôtes** : les machines injoignables (ping KO) sont automatiquement retirées du host-pool. Vérifiez que la liste restante couvre vos machine_count.
- **Multipliez les campagnes** pour lisser les variations réseau : relancez la commande plusieurs fois et consolidez les CSV.
- **Nettoyage** : en cas d’arrêt manuel, exécuter `pkill -f "python3.*serveur.py"` sur le master et `pkill -f "python3.*client.py"` sur les workers.

Bon benchmark !
