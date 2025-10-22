# Guide rapide – script `map_reduce.sh`

Ce guide décrit comment lancer et superviser le job MapReduce de base à l’aide du script `map_reduce.sh`.

---

## 1. Vue d’ensemble

- **Script** : `map_reduce.sh`
- **Langage** : Bash
- **Principe** : lancer `serveur.py` sur un master puis `client.py` sur une liste de workers via SSH.
- **Logs** : `~/mapreduce_master.log` côté master, `~/mapreduce_worker_X.log` côté workers.

Le script suppose que :

1. `serveur.py` et `client.py` sont présents dans `~` sur chaque machine.
2. Les splits (texte) sont accessibles sous `~/split_X.txt` pour chaque worker.
3. L’authentification SSH sans mot de passe est déjà configurée pour l’utilisateur courant.

---

## 2. Contenu du script (résumé)

```bash
USER=blepourt-25
MASTER=tp-4b01-10
WORKERS=(tp-4b01-11 … tp-4b01-20)
CONTROL_PORT=5374
SHUFFLE_BASE=6200

ssh "$MASTER" "nohup python3 ~/serveur.py --num-workers ${#WORKERS[@]} … &"

for worker_id, host in enumerate(WORKERS):
  ssh "$host" "nohup python3 ~/client.py ${worker_id} ${WORKERS[@]} --master-host $MASTER … &"
done
```

Points clés :

- `--num-workers` indique au master combien de clients attendre.
- Les workers reçoivent la liste complète d’hôtes afin d’établir les connexions shuffle.
- Les processus sont lancés en arrière-plan via `nohup`.

---

## 3. Exécution

### 3.1 Lancer une campagne

```bash
./map_reduce.sh
```

Le script :

1. lance le master sur `MASTER`,
2. attend 1 seconde,
3. lance les clients sur chaque worker,
4. affiche un rappel pour consulter les logs.

### 3.2 Vérifier que tout part bien

```bash
# Master (connexion + suivi du log)
ssh blepourt-25@tp-4b01-10 'tail -f ~/mapreduce_master.log'

# Worker 3
ssh blepourt-25@tp-4b01-13 'tail -f ~/mapreduce_worker_3.log'
```

---

## 4. Adaptations courantes

### 4.1 Changer la topologie

Modifier directement les variables en haut du script :

```bash
MASTER="tp-4b01-05"
WORKERS=(tp-4b01-06 tp-4b01-07 tp-4b01-08)
```

Le nombre de workers (`${#WORKERS[@]}`) s’ajuste automatiquement.

### 4.2 Ajouter/retirer des machines

Il suffit d’ajouter ou d’enlever des éléments dans le tableau `WORKERS`. Les identifiants `(worker_id)` suivront l’ordre du tableau.

### 4.3 Modifier les ports

Si un port est déjà utilisé :

```bash
CONTROL_PORT=6000      # maître ⇄ workers
SHUFFLE_BASE=7000      # base pour (7000 + worker_id - 1)
```

Assurez-vous que les pare-feu autorisent ces ports.

### 4.4 Exploiter plusieurs cœurs sur un worker

Sur les splits volumineux, vous pouvez paralléliser la lecture locale en
ajoutant `--map-workers N` (par exemple `--map-workers 4`) au lancement de
`client.py`. Chaque worker découpe alors son fichier en segments équilibrés et
les traite via un pool de processus.

Contraintes :

- laisser `--max-lines` désactivé ;
- disposer d'un fichier suffisamment gros pour générer plusieurs segments,
  sinon le client revient automatiquement au chemin séquentiel et affiche un
  message.

Pour observer la montée en charge, connectez-vous sur le worker pendant la
phase map et lancez `htop` : plusieurs processus `python3` doivent consommer du
CPU simultanément.

---

## 5. Surveillance & diagnostic

### 5.1 Processus

```bash
# Master
ssh blepourt-25@tp-4b01-10 'ps aux | grep serveur.py'

# Worker 4
ssh blepourt-25@tp-4b01-14 'ps aux | grep client.py'
```

### 5.2 Ports

```bash
ssh blepourt-25@tp-4b01-10 'ss -tlnp | grep 5374'   # port contrôle master
ssh blepourt-25@tp-4b01-11 'ss -tlnp | grep 6200'  # port shuffle worker 1
```

### 5.3 Logs finaux

```bash
ssh blepourt-25@tp-4b01-10 'tail -n 100 ~/mapreduce_master.log'
```

Vous devriez voir les phases `register`, `start_map`, `map_finished`, `start_reduce`, puis `Final wordcount`.

---

## 6. Nettoyage

Lorsque vous souhaitez stopper les jobs :

```bash
ssh blepourt-25@tp-4b01-10 'pkill -f "python3.*serveur.py"'

for host in tp-4b01-11 tp-4b01-12 … tp-4b01-20; do
  ssh blepourt-25@"$host" 'pkill -f "python3.*client.py"'
done
```

Vous pouvez encapsuler ce bloc dans un petit script `cleanup.sh`.

---

## 7. Bonnes pratiques

- **Tester en dry-run** : exécuter manuellement chaque commande `ssh` pour vérifier les droits et chemins.
- **Logs** : surveiller `~/mapreduce_master.log` pour confirmer la terminaison complète.
- **Synchroniser les scripts** si vous les modifiez (ex. `scp serveur.py tp-4b01-10:~/`).
- **Préparer les splits** (`split_X.txt`) sur chaque worker avant lancement.

Cette base vous permet ensuite de passer à des orchestrateurs plus évolués (multi-campagnes, mesure de speedup, etc.) tout en conservant la maîtrise du flux minimal. Bon run !
