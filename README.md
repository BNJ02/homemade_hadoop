# MapReduce Wordcount Demo

Ce dépôt montre une version simplifiée d'un traitement MapReduce de type
« wordcount » réparti sur plusieurs machines virtuelles d'un TP.

## Architecture

- `serveur.py` (master) : ouvre un canal de contrôle TCP, attend
  l'inscription de `N` workers, diffuse les ordres `start_map` puis
  `start_reduce`, agrège les résultats et publie le comptage global.
- `client.py` (worker) : chaque instance se connecte au master, expose un
  écouteur "shuffle" dédié et exécute la phase map puis la réduction pour
  les clés qui lui reviennent.
- `Bonjour_a_3_machines.sh` / `map_reduce.sh` : scripts d'orchestration qui
  démarrent le master (tp-1a207-37) puis les trois workers
  (tp-1a207-34..36) en tâche de fond.
- `split_<id>.txt` : fichiers d'entrée déjà partitionnés côté workers. Le
  worker `i` (1-based) lit `split_i.txt`.

## Déroulement du job

1. Les workers démarrent leur listener shuffle **avant** de s'inscrire
   auprès du master. Les enregistrements sont parallèles.
2. Dès que le master a reçu `N` enregistrements, il émet `start_map`.
   Chaque worker lit son split local et pour chaque mot calcule `MD5(word)
   % N` afin d'identifier le worker propriétaire. L'envoi se fait au fil de
   l'eau via des sockets persistants : les mappers sont donc réellement
   parallèles.
3. Pendant que chaque mapper continue de lire son fichier, son thread
   d'écoute accepte des connexions entrantes et incrémente le compteur
   local dès qu'un mot arrive. Les réceptions s'exécutent donc en parallèle
   de l'émission.
4. Une fois son split terminé, chaque worker notifie `map_finished`. Le
   master attend les `N` notifications avant de broadcast `start_reduce`.
5. La phase reduce consiste simplement à trier et renvoyer les comptes
   accumulés. Tous les reducers travaillent simultanément puisque le master
   attend seulement les messages `reduce_finished`.
6. Le master fusionne les dictionnaires reçus, affiche les totaux globaux,
   puis envoie `shutdown` pour terminer les workers proprement.

## Lancer un job

1. Copier `serveur.py` sur `tp-1a207-37` et `client.py` ainsi que
   `split_i.txt` sur chaque worker. Vérifier la présence de Python 3.
2. Depuis la machine de pilotage, exécuter `./Bonjour_a_3_machines.sh` (ou
   `map_reduce.sh` si vous préférez). Le script ouvre le master en tâche de
   fond puis chaque worker avec l'identifiant adéquat (1..N).
3. Suivre la progression via `ssh tp-1a207-37 'tail -f ~/mapreduce_master.log'`
   et `ssh tp-1a207-3x 'tail -f ~/mapreduce_worker_i.log'`.
4. À la fin, le master affiche « Final wordcount » avec le total consolidé.

## Personnalisation

- Modifier `WORKERS` dans le script shell pour ajuster la taille du cluster.
- L'option `--split-id` de `client.py` permet de cibler un fichier différent
  si l'identifiant du worker ne correspond pas au suffixe du split.
- Les ports contrôles et shuffle sont configurables via `--control-port` et
  `--shuffle-port-base`.

## Validation parallèle

Le parallélisme provient de deux éléments principaux :

- le master n'envoie `start_map` qu'une fois tous les workers prêts, ce qui
  enclenche les lectures simultanées des fichiers ;
- chaque worker maintient un thread d'écoute qui traite les flux en même
  temps que la lecture du split, régulée par le hash. Les logs montrent que
  les notifications `map_finished` et `reduce_finished` arrivent dans des
  ordres interdépendants, preuve que les phases se chevauchent.

Pour déboguer, activer des prints supplémentaires dans `client.py` ou
inspecter les fichiers `mapreduce_worker_*.log`.
