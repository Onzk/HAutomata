import os
import pandas as pd
import warnings
from hdfs import InsecureClient
from pyhive import hive
from time import sleep
from utils import printer


# Chemin vers le dossier
# contenant les fichiers à charger
data_folder_path = "./data"

# Chemin vers les fichiers logs
logs_folder_path = "./logs"

# Nom du fichier log
log_file = "handled.txt"

# Chemin complet vers le fichier log
log_path = f"{logs_folder_path}/{log_file}"

# Chemin vers le dossier contenant les
# fichiers transformés pour la dimension "mois"
month_dim_folder_path = "./database/month"

# Chemin vers le dossier contenant les
# fichiers transformés pour la dimension "trajet"
trip_dim_folder_path = "./database/trip"

# Chemin vers le dossier contenant les
# fichiers transformés pour le Data Mart "fait nombre trajets"
data_mart_dim_folder_path = "./database/datamart"

# Fonction pour garder uniquement les fichiers
# de type : .parquet
pfilter = lambda fs: [f for f in fs if f.lower().endswith(".parquet")]

# Indique le chemin d'accès au serveur hdfs
hdfs_url = "http://host.docker.internal:50070"

# Indique l'utilisateur du serveur hdfs
hdfs_user = "root"

# Indique le Data Warehouse
datawarehouse_name = "DWHNYCTAXI"

# Indique le chemin vers le Data Warehouse
datawarehouse_path = f"/user/hive/warehouse/{datawarehouse_name}"

# Indique l'adresse ou le nom d'hôte
# du serveur hive
hive_host = "host.docker.internal"

# Indique le port du serveur hive
hive_port = 10000

# Indique le nom d'utilisateur du serveur hive
hive_user = "hive"

# Indique le nombre maximal de tentatives
# d'initialisation
max_init_retries = 10

# Indique le délai d'attente entre chaque
# tentative d'initialisation en secondes
init_delay = 15

# Représente la liste des id actuellement dans
# la dimension mois
current_months = []

# Récupère le dernier identifiant dans
# dimension trajet
last_id_dim_trip = 0

# Récupère le dernier identifiant dans
# le Data Mart fait_nombre_trajet
last_id_data_mart = 0


# Fonction pour extraire les identifiants
# ou seulement le dernier dans une table
def get_current_ids(conn, table: str, only_last=True, id="id") -> list | int:
    # Ignorer tous les avertissements
    warnings.filterwarnings("ignore")
    # Si on le veut que le dernier identifiant
    if only_last:
        # Crée la requête pour récupérer le dernier identifiant
        query = f"SELECT MAX({id}) FROM {datawarehouse_name}.{table}"
    # Sinon
    else:
        # Crée la requête pour tout récupérer
        query = f"SELECT {id} FROM {datawarehouse_name}.{table}"
    # Initialise le Data Frame des résultats
    df = pd.read_sql(query, conn)
    # Crée la liste des identifiants
    ids = df.iloc[:, 0].tolist()
    # Renvoie le dernier identifiant ou
    # les données sous forme de liste
    return ([0] + ids)[-1] if only_last else ids


# Fonction pour initialiser le service
def init():
    # Indique que la séquence d'initialisation
    # vient de commencer
    printer.bold("### Initialization sequence engaged ###")
    # Initialise la connection à l'HDFS et à Hive
    hdfs_client, hive_client = None, None
    # Réinitialise les logs
    logs = open(log_path, "w+")
    # Déclare les variables globales qu'on veut utiliser
    global current_months, last_id_dim_trip, last_id_data_mart
    for i in range(0, max_init_retries):
        # Tente l'initialisation
        try:
            # Tente une connection vers l'HDFS si elle
            # n'est pas encore faite
            printer.simple(f"Waiting to connect with HDFS Server")
            printer.simple(f"  + {hdfs_url} ")
            while hdfs_client is None:
                try:
                    # Crée une connection vers l'HDFS
                    hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)
                    printer.simple(f"  + [established]")
                except BaseException as ex:
                    # Attends un certain temps avant de retenter
                    sleep(init_delay)
            # Tente une connection vers Hive si elle
            # n'est pas encore faite
            printer.simple(f"Waiting to connect with Hive Server")
            printer.simple(f"  + {hive_host}:{hive_port}?user={hive_user} ")
            while hive_client is None:
                try:
                    # Crée une connection vers Hive
                    hive_client = hive.Connection(
                        host=hive_host, port=hive_port, username=hive_user
                    )
                    printer.simple(f"  + [established]")
                except BaseException as ex:
                    # Attends un certain temps avant de retenter
                    sleep(init_delay)
            # Récupère la liste des fichiers déjà
            # traités auparavant
            printer.simple("Loading configs")
            handled_files = hdfs_client.list(
                f"{datawarehouse_path}.db/fait_nombre_trajet"
            )
            # Stocke la liste des fichiers déjà traités dans les logs
            logs.writelines([l + "\n" for l in handled_files])
            # Ferme le fichier des logs
            logs.close()
            printer.simple(f"  + LOG_FILE [ok]")
            # Initialise la liste des id actuellement dans
            # la dimension mois
            current_months = get_current_ids(
                hive_client, "dimension_mois", False, "id_dim_mois"
            )
            printer.simple(f"  + CURRENT_MONTHS [ok]")
            # Récupère le dernier identifiant dans
            # dimension trajet
            last_id_dim_trip = get_current_ids(
                hive_client, "dimension_trajet", id="id_dim_trajet"
            )
            printer.simple(f"  + LAST_ID_DIM_TRAJET [ok]")
            # Récupère le dernier identifiant dans
            # le Data Mart fait nombre trajet
            last_id_data_mart = get_current_ids(
                hive_client, "fait_nombre_trajet", id="id_fait_nombre_trajet"
            )
            printer.simple(f"  + LAST_ID_FAIT_NOMBRE_TRAJET [ok]")
            # Ferme la connection au Serveur Hive
            hive_client = hive_client.close()
            # Affiche que tout s'est bien passé
            printer.simple("Initialization sequence completed successfully")
            printer.bold("")
            printer.bold(
                "###################################################################"
            )
            printer.bold(
                "#### H #### A #### U #### T #### O #### M #### A #### T #### A ####"
            )
            printer.bold(
                "###################################################################"
            )
            printer.bold("")
            # Arrête l'initialisation si tout s'est bien passé
            return
        except BaseException as ex:
            # Affiche une erreur s'il y en a
            printer.red(f"  - Unable to init automata")
            # Affiche la description de l'erreur
            printer.red(f"  - {type(ex).__name__} : {ex}")
            # S'il ne s'agit pas du dernier essai
            if i != max_init_retries - 1:
                printer.yellow(f"  + Retry in {init_delay}s")
                # Attends un certain temps avant de réessayer
                sleep(init_delay)
    printer.red(f"")
    printer.red(f"Unable to init automata [error]")
    printer.red(f"")
    # Arrête le programme
    exit()


# Fonction pour extraire les nouveaux fichiers
# dans le dossier des données
def extract() -> list:
    printer.bold(f"### Extraction ###")
    # Représente la liste des fichiers à gérer
    to_handle = []
    # Tant qu'il n'y a rien à traiter,
    # on recherche de nouveaux fichiers
    printer.simple("Searching for new files . . .")
    while len(to_handle) == 0:
        # Récupère la liste des fichiers dans
        # le dossier des données
        files = os.listdir(data_folder_path)
        # Crée le fichier de log, s'il n'existe pas.
        open(log_path, "a+").close()
        # Ouvre le fichier de log
        handled = open(log_path, "r")
        # Récupère la liste des fichiers déjà
        # traités auparavant
        handled_files = [f.strip() for f in handled.readlines()]
        # Ferme le fichier de log
        handled.close()
        # Récupère les nouveaux fichiers
        to_handle = [file for file in files if file + ".csv" not in handled_files]
        # Crée la liste des fichiers de type .parquets
        # non traités
        to_handle = pfilter(to_handle)
        # S'il n'y a rien à traiter
        if len(to_handle) == 0:
            # Attends 5 secondes
            sleep(5)
            # Continue la recherche
            continue
        # Affiche que le fichier a été extrait avec succès
        [printer.blue(file + "\t[extracted]") for file in to_handle]
        # Affiche le nombre de fichiers à traiter
        printer.blue(f"Extraction finished : {len(to_handle)} file(s) found")
        # Retourne la liste des fichiers à traiter
    return to_handle


# Fonction pour transformer les nouveaux fichiers
def transform(files: list) -> list:
    printer.bold(f"### Transformation ###")
    # Déclare les variables globales qu'on veut utiliser
    global current_months, last_id_dim_trip, last_id_data_mart
    # Crée la liste des fichiers traités avec succès
    transformed_files = []
    # Parcourt la liste des fichiers à transformer
    for file in files:
        printer.purple(f"Transforming '{file}'")
        # Pour chaque fichier à traiter
        try:
            # Charge le fichier dans un DataFrame
            df = pd.read_parquet(data_folder_path + "/" + file)
            # Récupère la liste des mois où les trajets ont commencé
            all_months = df[df.columns[1]].dt.month.tolist()
            # Récupère la liste des mois où les trajets ont commencé
            # de façon unique
            months = list(set(all_months))
            # Garde uniquement les mois absents de la dimension mois
            months = list(set(months) - set(current_months))
            # Met à jour la liste des mois courrants
            current_months = list(set(all_months + current_months))
            # Crée un DataFrame à partir de ces mois
            monthdf = pd.DataFrame({"id": months, "month": months})
            # Définit l'identifiant du DataFrame des mois
            monthdf.set_index("id", inplace=True)
            # Charge le DataFrame des mois dans un fichier CSV
            monthdf.to_csv(month_dim_folder_path + "/" + file + ".csv", header=False)
            printer.purple(f"  + Dimension Mois [ok]")
            # Calcule le dernier identifiant après chargement
            old, last_id_dim_trip = (
                last_id_dim_trip,
                last_id_dim_trip + df.__len__() + 1,
            )
            # Génère les nouveaux identifiants des trajets
            trip_ids = list(range(old + 1, last_id_dim_trip))
            # Crée le dictionnaire des trajets avec la date de début
            # et de fin des trajets avec des identifiants
            trips = {
                "id": trip_ids,
                "pickup_datetime": pd.to_datetime(df[df.columns[1]]).tolist(),
                "dropoff_datetime": pd.to_datetime(df[df.columns[2]]).tolist(),
            }
            # Crée le DataFrame des trajets
            tripdf = pd.DataFrame(trips)
            # Définit l'identifiant de chaque trajet
            tripdf.set_index("id", inplace=True)
            # Charge le DataFrame des trajets dans un fichier CSV
            tripdf.to_csv(trip_dim_folder_path + "/" + file + ".csv", header=False)
            printer.purple(f"  + Dimension Trajet [ok]")
            # Calcule dernier identifiant après chargement
            old, last_id_data_mart = (
                last_id_data_mart,
                last_id_data_mart + df.__len__() + 1,
            )
            # Crée le DataFrame du Data Mart
            data_martdf = pd.DataFrame(
                {
                    "id": list(range(old + 1, last_id_data_mart)),
                    "id_dim_mois": all_months,
                    "id_dim_trajet": trip_ids,
                }
            )
            # Définit l'identifiant dans la table du Data Mart
            data_martdf.set_index("id", inplace=True)
            # Charge le DataFrame du Data Mart dans un fichier CSV
            data_martdf.to_csv(
                data_mart_dim_folder_path + "/" + file + ".csv", header=False
            )
            printer.purple(f"  + Fait Nombre Trajet [ok]")
            # Ajoute le fichier courrant à la liste des fichiers traités
            transformed_files.append(file)
            # Affiche que le fichier a été transformé avec succès
            printer.purple(f"  + [transformed]")
        except BaseException as ex:
            # Affiche une erreur s'il y en a
            printer.red(f"   - Unable to transform file : {file}")
            # Affiche la description de l'erreur
            printer.red(f"   - {type(ex).__name__} : {ex}")
    # Affiche le nombre de fichiers transformés
    printer.purple(
        f"Transformation finished : {len(transformed_files)} file(s) transformed"
    )
    # Retourne la liste des fichiers traités
    return transformed_files


# Fonction pour charger les fichiers transformés
# dans le Data Warehouse
def load(files: list) -> list:
    printer.bold("### Loading ###")
    # Stocke les fichiers chargés avec succès
    loaded_files = []
    # Crée une connection vers hdfs
    hdfs_client = InsecureClient(hdfs_url, user=hdfs_user)
    # Parcourt la liste des fichiers traités
    for file in files:
        printer.green(f"Loading '{file}'")
        # Pour chaque fichier
        try:
            # Charge les données dans la dimension mois
            hdfs_client.upload(
                datawarehouse_path + ".db/dimension_mois",
                f"./database/month/{file}.csv",
            )
            printer.green(f"  + Dimenstion Mois [ok]")
            # Supprime le fichier chargé sur le disque local
            os.unlink(f"./database/month/{file}.csv")
            # Charge les données dans la dimension trajet
            hdfs_client.upload(
                datawarehouse_path + ".db/dimension_trajets",
                f"./database/trip/{file}.csv",
            )
            printer.green(f"  + Dimension Trajet [ok]")
            # Supprime le fichier chargé sur le disque local
            # os.unlink(f"./database/trip/{file}.csv")
            # Charge les données dans de data mart fait_nombre_trajet
            hdfs_client.upload(
                datawarehouse_path + ".db/fait_nombre_trajet",
                f"./database/datamart/{file}.csv",
            )
            printer.green(f"  + Fait Nombre Trajet [ok]")
            # Supprime le fichier chargé sur le disque local
            os.unlink(f"./database/datamart/{file}.csv")
            # Ajoute le fichier à la liste des fichiers chargés
            loaded_files.append(file)
            # Affiche que le fichier a été transformé avec succès
            printer.green(f"  + [loaded]")
        except BaseException as ex:
            # Affiche une erreur s'il y en a
            printer.red(f"   - Unable to load file : {file}")
            # Affiche la description de l'erreur
            printer.red(f"   - {type(ex).__name__} : {ex}")
    # Si aucun fichier n'a été chargé
    if len(loaded_files) == 0:
        # Affiche qu'aucun fichier n'a été chargé
        printer.yellow("no file loaded")
    else:
        # Affiche le nombre de fichiers à traités
        printer.green(f"Loading finished : {len(loaded_files)} file(s) loaded")
    # Retourne la liste des fichiers chargés
    return loaded_files


# Fonction qui lance le traitement des fichiers
def main():
    # Initialise le programme
    init()
    # Exécute à l'infini
    while True:
        # Récupère le résultat des opérations
        result = load(transform(extract()))
        # Si rien n'a été fait
        if len(result) == 0:
            # Attends 5 secondes
            sleep(5)
            # Relance le traitement
            continue
        # Sinon, ouvre le fichier de log
        with open(log_path, "a+") as log:
            # Enregistre les fichiers traités
            # dans le fichier des logs
            log.writelines(["\n" + r + ".csv" for r in result])
        # Attends 1 seconde avant de recommencer
        sleep(1)


if __name__ == "__main__":
    # Lance le programme principal
    main()
