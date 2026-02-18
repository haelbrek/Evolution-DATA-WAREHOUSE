# Rapport E5 - Mise en place d'un Entrepôt de Données

## Structure du dossier

```
E5_Entrepot_Donnees/
├── rapport_E5.tex      # Rapport principal LaTeX
├── README.md           # Ce fichier
├── figures/            # Dossier pour les images/diagrammes
└── output/             # Dossier pour le PDF généré
```

## Compilation du rapport

### Prérequis
- Distribution LaTeX (MiKTeX, TeX Live, ou MacTeX)
- Packages requis : babel, geometry, graphicx, booktabs, tikz, hyperref, listings

### Commandes de compilation

**Avec pdflatex :**
```bash
pdflatex rapport_E5.tex
pdflatex rapport_E5.tex  # Exécuter 2 fois pour la table des matières
```

**Avec latexmk (recommandé) :**
```bash
latexmk -pdf rapport_E5.tex
```

### Compilation en ligne
Vous pouvez aussi utiliser [Overleaf](https://www.overleaf.com/) en uploadant le fichier .tex

## Sections du rapport

| Section | Statut | Description |
|---------|--------|-------------|
| Introduction | Complète | Contexte et objectifs |
| Inventaire des données | À compléter | Liste des sources et analyses |
| Modélisation | À compléter | Schémas logiques et physiques |
| Architecture | À compléter | Configuration technique |
| ETL | À compléter | Pipelines de données |
| Tests | À compléter | Procédures de validation |
| Documentation | À compléter | Guides techniques |
| Retour d'expérience | À compléter | Bilan du projet |

## Mise à jour du rapport

Les sections marquées `% TODO` doivent être complétées au fur et à mesure de l'avancement du projet.
