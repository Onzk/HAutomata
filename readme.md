# Documentation du Projet

## Introduction

Ce projet est un script Python conçu pour ingérer, transformer et charger des données provenant de fichiers `.parquet` relatifs aux trajets de taxi à New York. Les fichiers sont téléchargés depuis une URL spécifique, traités pour en extraire des dimensions temporelles et de trajets, puis chargés dans un Data Warehouse HDFS via Hive.

## Fonctionnalités

1. **Téléchargement des Fichiers** : Le script télécharge les fichiers `.parquet` à partir d'une URL spécifiée, en se basant sur les années définies dans la configuration.
2. **Extraction des Nouveaux Fichiers** : Le script recherche les nouveaux fichiers à traiter dans un répertoire local.
3. **Transformation des Données** : Les fichiers `.parquet` sont transformés en fichiers CSV pour les dimensions "mois" et "trajet", ainsi que pour un Data Mart contenant le nombre de trajets.
4. **Chargement des Données** : Les fichiers transformés sont chargés dans un Data Warehouse HDFS via Hive.

## Prérequis

- Python 3.x
- Packages Python :
  - `pandas`
  - `requests`
  - `beautifulsoup4`
  - `pyhive`
  - `hdfs`
- Serveur HDFS
- Serveur Hive

## Configuration

Avant d'exécuter le script, assurez-vous que les variables de configuration suivantes sont correctement définies :

- `dataset_url`: URL du site contenant les fichiers de données.
- `dataset_of_years`: Liste des années à traiter (utilisez `'*'` pour toutes les années).
- `data_folder_path`: Chemin vers le dossier contenant les fichiers à télécharger.
- `logs_folder_path`: Chemin vers le dossier pour les fichiers de log.
- `hdfs_url`, `hdfs_user`: URL et utilisateur du serveur HDFS.
- `hive_host`, `hive_port`, `hive_user`: Adresse et port du serveur Hive.
- `datawarehouse_name`: Nom du Data Warehouse dans Hive.
- `max_init_retries`, `init_delay`: Nombre maximal de tentatives d'initialisation et délai entre les tentatives.

## Structure des Répertoires

- `./data`: Contient les fichiers `.parquet` téléchargés.
- `./logs`: Contient le fichier de log `handled.txt`.
- `./database/month`: Contient les fichiers CSV pour la dimension "mois".
- `./database/trip`: Contient les fichiers CSV pour la dimension "trajet".
- `./database/datamart`: Contient les fichiers CSV pour le Data Mart "fait nombre trajets".

## Fonctions Principales

1. **`ingester()`** : Télécharge les fichiers depuis l'URL spécifiée.
2. **`init()`** : Initialise les connexions HDFS et Hive, récupère les fichiers traités précédemment, et démarre le processus de téléchargement.
3. **`extract()`** : Extrait les nouveaux fichiers à partir du répertoire des données.
4. **`transform(files: list) -> list`** : Transforme les fichiers extraits en fichiers CSV pour les dimensions et le Data Mart.
5. **`load(files: list) -> list`** : Charge les fichiers transformés dans le Data Warehouse HDFS.
6. **`main()`** : Fonction principale qui orchestre le processus d'extraction, transformation et chargement de manière continue.

## Exécution

Pour exécuter le script, utilisez la commande suivante :

```bash
python <nom_du_script>.py
```

Le script s'exécutera en boucle continue, traitant les fichiers dès qu'ils sont disponibles.

## Logs

Les logs des fichiers traités sont stockés dans `./logs/handled.txt`.

## Gestion des Erreurs

Les erreurs sont affichées dans la sortie standard et dans les logs. Les tentatives de reconnexion sont gérées lors de l'initialisation des connexions aux serveurs HDFS et Hive.

