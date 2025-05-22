# Projet Transformation – Conception de Pipelines
Projet réaliser par : Issmail KHOUYI - Ilias ERRAZI – Melek JENDOUBI.

Ce projet est un pipeline ETL (Extract, Transform, Load) développé en Python.  
Il a été réalisé dans le cadre d’un TP de l’unité "Conception de pipelines de données" à l’EPSI.

---

## Étapes du projet

1. **Chargement des données** depuis `ventes.csv`
2. **Nettoyage** :
   - Suppression des dates invalides et valeurs manquantes
   - Remplissage des quantités/prix manquants
   - Suppression des doublons
3. **Validation** :
   - Conversion du format de date
   - Vérification des prix et quantités positifs
4. **Transformation** :
   - Calcul du `Montant_total` (prix × quantité)
   - Normalisation entre 0 et 1 (`Montant_normalise`)
5. **Enrichissement** :
   - Ajout du segment client (`Basique`, `Standard`, `Premium`)
6. **Gestion des erreurs** :
   - `try/except` appliqué à un traitement risqué (division volontaire)
   - Enregistrement dans le fichier `logs/erreurs.log`
7. **Export final** :
   - Données exportées dans `data/ventes_transformees.csv`

---

## Exécution du pipeline

```bash
python main.py
```
---

## Historisation des données (L pour Load)

Une nouvelle fonctionnalité d’historisation a été ajoutée au pipeline pour répondre à la logique "Load".

À chaque exécution du script `main.py`, les données transformées sont sauvegardées dans un fichier CSV avec un **horodatage automatique** dans le dossier `historique_load/`.

### Exemple de fichier généré : 
historique_load/ventes_transformees_20250522_144636.csv

Cela permet de :
- Conserver une trace de chaque traitement effectué
- Comparer les données entre différentes dates
- Retrouver facilement une version précédente

Les fichiers ne remplacent pas l’ancien export, mais s’ajoutent à l’historique.
