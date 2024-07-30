
# Projet d'Automatisation de Traitement de Données NYC Taxi

## Description

Ce projet automatise le téléchargement, la transformation et le chargement des données sur les trajets en taxi à New York. Il utilise Hadoop HDFS et Hive pour gérer les données, et est conçu pour fonctionner dans un environnement Docker avec plusieurs services interconnectés.

## Structure du Projet

- **`automata/`** : Code principal pour le traitement des données.
- **`docker-compose.yml`** : Définition des services Docker nécessaires pour l'exécution du projet.
- **`data/`** : Dossier pour stocker les fichiers de données téléchargés.
- **`logs/`** : Dossier pour les fichiers de logs.
- **`hdfs/`** : Dossier pour les fichiers transformés et les données du Data Mart.
- **`metastore-postgresql/`** : Dossier pour les fichiers PostgreSQL du Hive Metastore.

## Dépendances

- Python 3.x
- Pandas
- Requests
- BeautifulSoup4
- PyHive
- HDFS
- Docker
- Docker Compose

## Installation

1. **Clonez le dépôt :**
   ```bash
   git clone https://github.com/your-repo.git
   cd your-repo
   ```

2. **Créez un environnement virtuel et installez les dépendances :**
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configurez vos variables d'environnement :**
   Créez un fichier `.env` avec les variables nécessaires (voir la section des services Docker pour plus de détails).

## Configuration des Services Docker

Le projet utilise Docker Compose pour gérer les différents services nécessaires au traitement des données. Voici les services définis dans le fichier `docker-compose.yml` :

### Services

#### `namenode`
- **Image** : `bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8`
- **Description** : Conteneur Hadoop NameNode qui gère le namespace HDFS et les métadonnées des fichiers.
- **Volumes** : 
  - `./hdfs/namenode:/hadoop/dfs/name`
- **Ports** : 
  - `50070:50070` (Port web UI du NameNode)
- **Environnement** :
  - `CLUSTER_NAME=hive`

#### `datanode`
- **Image** : `bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8`
- **Description** : Conteneur Hadoop DataNode qui stocke les données HDFS.
- **Volumes** :
  - `./hdfs/datanode:/hadoop/dfs/data`
  - `./dataset:/data`
- **Ports** :
  - `50075:50075` (Port web UI du DataNode)
- **Environnement** :
  - `SERVICE_PRECONDITION: "namenode:50070"`

#### `hive-server`
- **Image** : `bde2020/hive:2.3.2-postgresql-metastore`
- **Description** : Serveur Hive pour les requêtes SQL.
- **Volumes** :
  - `./sql:/sql`
- **Ports** :
  - `10000:10000` (Port pour les connexions Hive)
- **Environnement** :
  - `HIVE_CORE_CONF_javax_jdo_option_ConnectionURL: "jdbc:postgresql://hive-metastore/metastore"`
  - `SERVICE_PRECONDITION: "hive-metastore:9083"`

#### `hive-metastore`
- **Image** : `bde2020/hive:2.3.2-postgresql-metastore`
- **Description** : Service Hive Metastore qui stocke les métadonnées Hive.
- **Command** :
  - `/opt/hive/bin/hive --service metastore`
- **Ports** :
  - `9083:9083` (Port pour le service metastore)
- **Environnement** :
  - `SERVICE_PRECONDITION: "namenode:50070 datanode:50075 hive-metastore-postgresql:5432"`

#### `hive-metastore-postgresql`
- **Image** : `bde2020/hive-metastore-postgresql:2.3.0`
- **Description** : Base de données PostgreSQL pour Hive Metastore.
- **Volumes** :
  - `./metastore-postgresql/postgresql/data:/var/lib/postgresql/data`

#### `automata`
- **Build** : `automata`
- **Description** : Conteneur principal exécutant le code d'automatisation.
- **Volumes** :
  - `./data:/app/data`
- **Depends_on** :
  - `hive-server`
  - `datanode`
  - `namenode`

## Utilisation

1. **Lancez les services Docker :**
   ```bash
   docker-compose up -d
   ```

2. **Exécutez le traitement des données :**
   Le script principal `main` est exécuté automatiquement lorsque le conteneur `automata` est démarré. Il surveille en continu le dossier des données pour de nouveaux fichiers, les transforme et les charge dans HDFS et Hive.

3. **Arrêtez les services Docker :**
   ```bash
   docker-compose down
   ```

## Fonctionnalités

- **Ingestion** : Télécharge les fichiers de données depuis le site web.
- **Extraction** : Identifie les nouveaux fichiers à traiter.
- **Transformation** : Convertit les fichiers en formats adaptés pour HDFS et Hive.
- **Chargement** : Envoie les données transformées vers HDFS et Hive.

## Gestion des Logs

Les logs du processus d'automatisation sont stockés dans le fichier `./logs/handled.txt`. Assurez-vous de vérifier ce fichier pour le suivi des fichiers traités et pour toute erreur éventuelle.

## Avertissements

- Assurez-vous que les chemins spécifiés dans les volumes Docker existent sur votre machine locale.
- Les services doivent être accessibles et fonctionnels pour que le traitement des données soit effectué correctement.

