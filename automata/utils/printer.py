from datetime import datetime
from time import sleep

# Fonction pour obtenir la date courante
now = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Fonction pour afficher les logs du programme
show = lambda text, code: (sleep(0.8), print(f"{now()}\33[{code}m  |  {text}\033[0m"))

# Fonction pour afficher un simple log du programme
simple = lambda text: (sleep(0.8), print(f"{now()}  |  {text}"))

# Fonction pour afficher les logs du programme en rouge
red = lambda text: show(text, "31")

# Fonction pour afficher les logs du programme en vert
green = lambda text: show(text, "32")

# Fonction pour afficher les logs du programme en jaune
yellow = lambda text: show(text, "33")

# Fonction pour afficher les logs du programme en bleu
blue = lambda text: show(text, "34")

# Fonction pour afficher les logs du programme en violet
purple = lambda text: show(text, "35")

# Fonction pour afficher les logs du programme en gras
bold = lambda text: show(text, "1")
