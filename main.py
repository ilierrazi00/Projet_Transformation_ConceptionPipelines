import pandas as pd              # Pour la manipulation des données
import numpy as np               # Pour les calculs mathématiques
from datetime import datetime    # Pour la gestion des dates
import logging                   # Pour la gestion des erreurs (journalisation)

# === Étape 1 : Chargement des données ===
df = pd.read_csv("data/ventes.csv")  # Lecture du fichier CSV

print("Aperçu des 5 premières lignes :")
print(df.head())

print("\nInfos sur les colonnes :")
print(df.info())

print("\nStatistiques :")
print(df.describe(include='all'))

# === Étape 2 : Nettoyage des données ===
print("\n--- Nettoyage des données ---")

df = df[df['Date_vente'] != 'invalid_date']  # Suppression des lignes avec des dates invalides
df = df.dropna(subset=['Nom_produit'])       # Suppression des lignes sans nom de produit

# Remplacement des valeurs manquantes par la moyenne
moy_quantite = df['Quantite_vendue'].mean()
df['Quantite_vendue'] = df['Quantite_vendue'].fillna(moy_quantite)

moy_prix = df['Prix_unitaire'].mean()
df['Prix_unitaire'] = df['Prix_unitaire'].fillna(moy_prix)

# Suppression des lignes avec des quantités nulles ou négatives
df = df[df['Quantite_vendue'] > 0]

# Suppression des doublons et réinitialisation de l'index
df.drop_duplicates(inplace=True)
df.reset_index(drop=True, inplace=True)

print("\nDonnées nettoyées (5 premières lignes) :")
print(df.head())

print("\nValeurs manquantes restantes :")
print(df.isnull().sum())

# === Étape 3 : Validation des données ===
print("\n--- Validation des données ---")

try:
    df['Date_vente'] = pd.to_datetime(df['Date_vente'], format='%Y-%m-%d')
    print("✅ Dates converties avec succès")
except Exception as e:
    print("❌ Erreur lors de la conversion des dates :", e)

# Vérification des prix et quantités valides
df = df[(df['Prix_unitaire'] > 0) & (df['Quantite_vendue'] > 0)]
print("✅ Prix et quantités validés")

# === Étape 4 : Transformations ===
print("\n--- Transformation des données ---")

# Calcul du montant total par ligne
df['Montant_total'] = df['Prix_unitaire'] * df['Quantite_vendue']
print("✅ Colonne Montant_total ajoutée")

# Normalisation Min-Max
min_montant = df['Montant_total'].min()
max_montant = df['Montant_total'].max()
df['Montant_normalise'] = (df['Montant_total'] - min_montant) / (max_montant - min_montant)
print("✅ Colonne Montant_normalise ajoutée (normalisation Min-Max)")

print("\nAperçu final avec transformations :")
print(df[['Nom_produit', 'Quantite_vendue', 'Prix_unitaire', 'Montant_total', 'Montant_normalise']].head())

# === Étape 5 : Enrichissement (segmentation) ===
print("\n--- Enrichissement des données : segmentation ---")

# Fonction de segmentation basée sur le montant total
def segmenter(montant):
    if montant >= 500:
        return "Premium"
    elif montant >= 200:
        return "Standard"
    else:
        return "Basique"

df['Segment'] = df['Montant_total'].apply(segmenter)
print("✅ Colonne Segment ajoutée")

print("\nAperçu avec le segment :")
print(df[['Nom_produit', 'Montant_total', 'Segment']].head())

# === Étape 6 : Gestion des erreurs avec journalisation ===
print("\n--- Gestion des erreurs ---")

# Configuration du fichier de log
logging.basicConfig(
    filename="logs/erreurs.log",        # Fichier où les erreurs seront enregistrées
    level=logging.ERROR,                # Niveau : erreur uniquement
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Fonction simulant une opération risquée (division par 0 volontaire)
def traitement_critique(row):
    try:
        total = row['Montant_total']
        division = total / (row['Quantite_vendue'] - row['Quantite_vendue'])  # provoque une ZeroDivisionError
        return division
    except Exception as e:
        logging.error(f"Erreur sur la ligne ID {row['ID_produit']} : {e}")
        return np.nan  # On retourne NaN en cas d'erreur

# Application du traitement critique à chaque ligne
df['Resultat_test'] = df.apply(traitement_critique, axis=1)

print("✅ Erreurs capturées et enregistrées dans logs/erreurs.log")

# === Étape 7 : Export des données transformées ===
print("\n--- Export des données transformées ---")
df.to_csv("data/ventes_transformees.csv", index=False)
print("✅ Données exportées dans data/ventes_transformees.csv")

