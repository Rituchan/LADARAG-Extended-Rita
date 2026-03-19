import yaml
import glob
import os

# Creiamo un Dumper personalizzato che impedisce la scrittura di alias/ancore
class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True

# Trova tutti i file yaml nella cartella delle API
cartella_apis = os.path.join("MOCKS-smart-city", "apis", "*.yaml")
file_trovati = glob.glob(cartella_apis)

for filepath in file_trovati:
    # 1. Legge il file (PyYAML risolve automaticamente * e & in memoria)
    with open(filepath, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    
    # 2. Sovrascrive il file scrivendo l'oggetto JSON per esteso (senza alias)
    with open(filepath, 'w', encoding='utf-8') as f:
        yaml.dump(data, f, Dumper=NoAliasDumper, default_flow_style=False, sort_keys=False)
        
    print(f"Risolto: {filepath}")

print("Completato! Tutte le ancore YAML sono state espanse.")