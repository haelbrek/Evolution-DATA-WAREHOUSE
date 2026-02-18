# E6 - Methodologie de Maintenance (ITIL)
## Projet Data Engineering - Region Hauts-de-France

---

## Table des matieres

1. [Introduction a ITIL](#1-introduction-a-itil)
2. [Gestion des incidents](#2-gestion-des-incidents)
3. [Gestion des changements](#3-gestion-des-changements)
4. [Gestion des problemes](#4-gestion-des-problemes)
5. [Matrice de priorisation](#5-matrice-de-priorisation)
6. [Outillage de ticketing](#6-outillage-de-ticketing)
7. [Journalisation et monitoring](#7-journalisation-et-monitoring)
8. [Procedures de backup](#8-procedures-de-backup)
9. [KPIs de suivi](#9-kpis-de-suivi)

---

## 1. Introduction a ITIL

**ITIL** (Information Technology Infrastructure Library) est un ensemble de bonnes pratiques pour la gestion des services informatiques. Dans le contexte de notre entrepot de donnees, ITIL structure trois processus cles :

- **Gestion des incidents** : Comment reagir quand un ETL echoue ou qu'une donnee est corrompue
- **Gestion des changements** : Comment integrer une nouvelle source sans casser l'existant
- **Gestion des problemes** : Comment identifier et corriger les causes racines d'erreurs recurrentes

---

## 2. Gestion des incidents

### Definition

Un **incident** est tout evenement non planifie qui interrompt ou degrade la qualite d'un service. Exemples :
- Echec d'un pipeline ETL
- Base de donnees inaccessible
- Donnees manquantes dans un datamart
- Temps de reponse degrade sur les requetes analytiques

### Processus de gestion

```
Detection --> Enregistrement --> Classification --> Investigation --> Resolution --> Cloture
```

| Etape | Description | Responsable |
|-------|-------------|-------------|
| **Detection** | Alerte automatique (logs ETL, monitoring) ou signalement utilisateur | Systeme / Analyste |
| **Enregistrement** | Creation du ticket GitHub Issue avec date, description, impact | Data Engineer |
| **Classification** | Priorisation selon la matrice Impact x Urgence | Data Engineer |
| **Investigation** | Consultation des logs (`dwh.log_etl`, `dwh.log_erreurs`), identification du composant | Data Engineer |
| **Resolution** | Correctif : relance ETL, correction de donnees, ajustement de config | Data Engineer |
| **Cloture** | Validation du service retabli, mise a jour du ticket, documentation | Data Engineer |

---

## 3. Gestion des changements

### Definition

Un **changement** est toute modification apportee a l'infrastructure ou aux services. L'objectif est de minimiser les risques d'interruption.

### Exemples de changements

| Type de changement | Exemple | Impact |
|--------------------|---------|--------|
| Nouvelle source | Ajout d'un fichier CSV emploi | Nouveau pipeline ETL |
| Schema | Ajout de colonnes SCD a une dimension | ALTER TABLE + mise a jour ETL |
| Datamart | Nouveau datamart pour un besoin d'analyse | CREATE VIEW + permissions |
| ETL | Modification suite a un changement de format source | Mise a jour du script Python |
| Infrastructure | Changement de SKU Azure SQL (S0 -> S3) | terraform apply |

### Processus de changement

1. **Demande** : Ticket GitHub Issue (label `changement`)
2. **Evaluation d'impact** : Quelles tables, ETL, datamarts sont concernes ?
3. **Approbation** : Validation par le responsable technique
4. **Implementation** : Deploiement en respectant les scripts SQL numerotes (001, 002, ...)
5. **Test** : Execution des tests (`test_dwh.py`)
6. **Validation** : Verification post-deploiement
7. **Documentation** : Mise a jour du guide et du rapport

---

## 4. Gestion des problemes

### Difference Incident vs Probleme

- **Incident** : Un symptome ponctuel (ex: le chargement echoue le 15 fevrier)
- **Probleme** : La cause racine d'incidents recurrents (ex: le fichier source a change de format)

### Exemple concret

```
Incident 1 : Le chargement de fait_population echoue le 15/02/2026
Incident 2 : Le meme chargement echoue le 01/03/2026
    |
    v
Probleme identifie : Le fichier source INSEE a ajoute une nouvelle colonne
    |
    v
Solution : Adapter l'ETL pour accepter dynamiquement les colonnes
```

---

## 5. Matrice de priorisation

La priorite d'un incident est determinee par le croisement de l'**impact** et de l'**urgence** :

|  | **Urgence Haute** | **Urgence Moyenne** | **Urgence Basse** |
|--|-------------------|--------------------|--------------------|
| **Impact Eleve** (tous les utilisateurs) | P1 - Critique | P2 - Haute | P3 - Moyenne |
| **Impact Moyen** (un service/equipe) | P2 - Haute | P3 - Moyenne | P4 - Basse |
| **Impact Faible** (un utilisateur) | P3 - Moyenne | P4 - Basse | P4 - Basse |

### Temps de resolution cible

| Priorite | Description | Delai |
|----------|-------------|-------|
| **P1 - Critique** | Base inaccessible, perte de donnees | < 1h |
| **P2 - Haute** | ETL en echec, datamart non rafraichi | < 8h (dans la journee) |
| **P3 - Moyenne** | Performance degradee, acces manquant | < 48h |
| **P4 - Basse** | Demande d'evolution, amelioration | Sprint suivant |

---

## 6. Outillage de ticketing

Nous utilisons **GitHub Issues** pour le suivi des taches de maintenance.

### Labels configures

| Categorie | Labels |
|-----------|--------|
| **Type** | `incident`, `changement`, `probleme`, `maintenance` |
| **Priorite** | `P1-critique`, `P2-haute`, `P3-moyenne`, `P4-basse` |
| **Composant** | `etl`, `sql`, `infrastructure`, `securite`, `datamart` |

### Template de ticket incident

```markdown
## Description de l'incident
[Description claire du probleme observe]

## Impact
- [ ] Base inaccessible
- [ ] ETL en echec
- [ ] Donnees manquantes
- [ ] Performance degradee

## Etapes pour reproduire
1. ...

## Logs associes
[Copier les logs pertinents depuis dwh.log_etl ou dwh.log_erreurs]

## Resolution
[A completer lors de la resolution]
```

---

## 7. Journalisation et monitoring

### Architecture de journalisation

Notre systeme de journalisation fonctionne a **deux niveaux** :

```
Scripts ETL (Python)                    Base de donnees (SQL)
      |                                        |
      v                                        v
etl_pipeline.log                        dwh.log_etl
(logging.FileHandler)                   dwh.log_erreurs
      |                                        |
      v                                        v
Diagnostic local                        Vues de monitoring BI
(consultation manuelle)                 analytics.v_monitoring_alertes
                                        analytics.v_erreurs_ouvertes
```

### Niveaux de severite

| Niveau | Usage | Exemple |
|--------|-------|---------|
| **DEBUG** | Developpement uniquement | "Connexion a la base etablie en 0.3s" |
| **INFO** | Fonctionnement normal | "Chargement dim_temps : 14 lignes inserees" |
| **WARNING** | Anormal mais non bloquant | "Table stg_emploi vide, etape ignoree" |
| **ERROR** | Echec d'une fonctionnalite | "Echec chargement fait_revenus : cle etrangere invalide" |
| **CRITICAL** | Systeme en peril | "Connexion a la base impossible, pipeline interrompu" |

### Tables de journalisation SQL

- **`dwh.log_etl`** : Enregistre chaque etape ETL (debut, succes, erreur, warning, duree, nb lignes)
- **`dwh.log_erreurs`** : Erreurs detaillees avec stack trace et suivi de resolution
- **`analytics.v_monitoring_alertes`** : Vue aggregee par jour/etape/statut pour tableaux de bord BI
- **`analytics.v_erreurs_ouvertes`** : Erreurs non resolues avec duree depuis l'erreur

### Consulter les logs

```sql
-- Derniers evenements ETL
SELECT TOP 20 * FROM dwh.log_etl ORDER BY date_execution DESC;

-- Erreurs non resolues
SELECT * FROM analytics.v_erreurs_ouvertes ORDER BY heures_depuis_erreur DESC;

-- Resume du monitoring par jour
SELECT * FROM analytics.v_monitoring_alertes WHERE jour >= DATEADD(DAY, -7, GETDATE());
```

---

## 8. Procedures de backup

### Strategie de backup

| Type | Frequence | Retention | Outil |
|------|-----------|-----------|-------|
| **Backup complet automatique** | Hebdomadaire | 14 jours (court terme) | Azure SQL Automated |
| **Backup differentiel automatique** | Toutes les 12h | 14 jours | Azure SQL Automated |
| **Backup log transactions** | Toutes les 5-10 min | 14 jours | Azure SQL Automated |
| **Retention long terme** | Hebdo/Mensuel/Annuel | 4 sem / 12 mois / 3 ans | Azure LTR |
| **Backup complet a la demande** | Sur demande | Manuel | `dwh.sp_backup_complet` |
| **Backup partiel a la demande** | Sur demande | Manuel | `dwh.sp_backup_partiel` |

### Objectifs RPO / RTO

- **RPO (Recovery Point Objective)** = 24h : Les donnees sont chargees quotidiennement
- **RTO (Recovery Time Objective)** = 4h : Les analystes tolerent une demi-journee d'indisponibilite

### Commandes de backup

```sql
-- Backup complet
EXEC dwh.sp_backup_complet;

-- Backup partiel du schema dwh
EXEC dwh.sp_backup_partiel @schema = 'dwh';

-- Restauration depuis un fichier
EXEC dwh.sp_restaurer_backup @chemin_fichier = 'dwh_full_20260214_120000.bak';

-- Consulter l'historique des backups
SELECT * FROM analytics.v_historique_backups ORDER BY date_execution DESC;
```

### Test de restauration

Il est recommande de tester la restauration **au moins une fois par trimestre** sur un environnement de test pour verifier l'integrite des sauvegardes et mesurer le RTO effectif.

---

## 9. KPIs de suivi

| KPI | Description | Objectif | Source |
|-----|-------------|----------|--------|
| Taux de disponibilite | % du temps ou l'entrepot est accessible | >= 99,5% | Azure Monitor |
| MTTR | Temps moyen de resolution d'un incident | < 4h | GitHub Issues |
| Taux de succes ETL | % d'executions ETL sans erreur sur le mois | >= 95% | `dwh.log_etl` |
| Nb incidents/mois | Nombre total d'incidents enregistres | Tendance a la baisse | GitHub Issues |
| Fraicheur des donnees | Delai entre source disponible et integration DWH | < 24h | `dwh.log_etl` |

### Requete de calcul du taux de succes ETL

```sql
SELECT
    FORMAT(date_execution, 'yyyy-MM') AS mois,
    COUNT(*) AS total_executions,
    SUM(CASE WHEN statut = 'SUCCES' THEN 1 ELSE 0 END) AS nb_succes,
    CAST(SUM(CASE WHEN statut = 'SUCCES' THEN 1.0 ELSE 0 END) / COUNT(*) * 100 AS DECIMAL(5,2)) AS taux_succes_pct
FROM dwh.log_etl
WHERE etape LIKE 'load_%'
GROUP BY FORMAT(date_execution, 'yyyy-MM')
ORDER BY mois DESC;
```
