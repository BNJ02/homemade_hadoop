# Guide Rapide - Cluster MapReduce tp-4b01-10 à tp-4b01-20

## ✅ Configuration actuelle

- **Master:** tp-4b01-10
- **Workers:** tp-4b01-11 à tp-4b01-20 (10 workers)
- **Fichiers splits:** Déjà présents sur les machines (split_1.txt à split_10.txt)
- **Scripts Python:** Déjà présents sur les machines

---

## 🚀 Lancement du Job MapReduce

### Méthode 1 : Depuis Windows (PowerShell)

```powershell
.\launch_remote.ps1
```

### Méthode 2 : Depuis une machine Linux du cluster (simple)

```bash
# Se connecter au master
ssh blepourt-25@tp-4b01-10

# Lancer le job une fois
./map_reduce.sh
```

### Méthode 3 : Depuis Linux avec benchmark

```bash
# Lancer avec attente et affichage des résultats
./run_cluster.sh --wait

# Lancer 10 fois et collecter les statistiques
./run_cluster.sh --runs 10

# Sauvegarder la sortie dans un fichier
./run_cluster.sh --wait --output resultat.txt

# Avec horodatage dans le nom de fichier
./run_cluster.sh --wait --output "resultat_$(date +%Y%m%d_%H%M%S).txt"

# Spécifier les workers personnalisés (machine i → split_i.txt)
./run_cluster.sh --wait --workers tp-4b01-11,tp-4b01-12,tp-4b01-13

# Utiliser seulement 5 workers (workers 1-5 → splits 1-5)
./run_cluster.sh --wait --workers tp-4b01-15,tp-4b01-16,tp-4b01-17,tp-4b01-18,tp-4b01-19

# Changer le master et les workers
./run_cluster.sh --wait --master tp-4b01-05 --workers tp-4b01-00,tp-4b01-01,tp-4b01-02
```

---

## 📊 Suivre l'exécution

### Voir le log du master en temps réel

```powershell
ssh blepourt-25@tp-4b01-10 'tail -f ~/mapreduce_master.log'
```

### Voir le log d'un worker

```powershell
ssh blepourt-25@tp-4b01-11 'tail -f ~/mapreduce_worker_1.log'
```

### Voir le résultat final

```powershell
ssh blepourt-25@tp-4b01-10 'tail -n 100 ~/mapreduce_master.log'
```

---

## 🧹 Nettoyer le cluster

Si vous devez arrêter les processus :

```powershell
# Arrêter le master
ssh blepourt-25@tp-4b01-10 'pkill -f serveur.py'

# Arrêter tous les workers
for ($i=11; $i -le 20; $i++) {
    ssh "blepourt-25@tp-4b01-$i" 'pkill -f client.py'
}
```

Ou créer un script de nettoyage :

```bash
# Sur tp-4b01-10, créer cleanup.sh
#!/bin/bash
pkill -f serveur.py
for i in {11..20}; do
    ssh tp-4b01-$i 'pkill -f client.py'
done
```

---

## 🔍 Vérifier l'état

### Processus en cours
```powershell
# Sur le master
ssh blepourt-25@tp-4b01-10 'ps aux | grep serveur.py'

# Sur un worker
ssh blepourt-25@tp-4b01-11 'ps aux | grep client.py'
```

### Ports ouverts
```powershell
# Master (port de contrôle 5374)
ssh blepourt-25@tp-4b01-10 'netstat -tlnp | grep 5374'

# Worker (port shuffle 6200)
ssh blepourt-25@tp-4b01-11 'netstat -tlnp | grep 6200'
```

---

## 📋 Architecture

```
Master: tp-4b01-10
  - Port de contrôle: 5374
  - Fichiers: serveur.py, split_1.txt à split_10.txt

Workers:
  - tp-4b01-11 (worker 1, port shuffle 6200) -> lit split_1.txt
  - tp-4b01-12 (worker 2, port shuffle 6201) -> lit split_2.txt
  - tp-4b01-13 (worker 3, port shuffle 6202) -> lit split_3.txt
  - tp-4b01-14 (worker 4, port shuffle 6203) -> lit split_4.txt
  - tp-4b01-15 (worker 5, port shuffle 6204) -> lit split_5.txt
  - tp-4b01-16 (worker 6, port shuffle 6205) -> lit split_6.txt
  - tp-4b01-17 (worker 7, port shuffle 6206) -> lit split_7.txt
  - tp-4b01-18 (worker 8, port shuffle 6207) -> lit split_8.txt
  - tp-4b01-19 (worker 9, port shuffle 6208) -> lit split_9.txt
  - tp-4b01-20 (worker 10, port shuffle 6209) -> lit split_10.txt
```

---

## ✨ Résultat attendu

Le log du master affichera :

```
Worker 1 registered from ('x.x.x.x', xxxx)
Worker 2 registered from ('x.x.x.x', xxxx)
...
Worker 10 registered from ('x.x.x.x', xxxx)

All workers registered. start_map sent.

Map from worker 1: ok
Map from worker 2: ok
...
Map from worker 10: ok

All map_finished received. start_reduce sent.

Reduce from worker 1: ok (XXX keys)
Reduce from worker 2: ok (XXX keys)
...
Reduce from worker 10: ok (XXX keys)

Final wordcount:
le: 12543
de: 9876
et: 8765
...
```

---

## ⚠️ Notes importantes

- Les fichiers Python et les splits sont déjà sur les machines
- Le script `map_reduce.sh` est déjà configuré pour ce cluster
- Pas besoin de déployer quoi que ce soit, tout est prêt !
- Le job s'exécute en arrière-plan sur chaque machine
