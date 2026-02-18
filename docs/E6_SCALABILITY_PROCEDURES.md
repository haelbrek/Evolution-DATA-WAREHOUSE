# E6 - Procedures d'Evolution et de Scalabilite
## Projet Data Engineering - Region Hauts-de-France

---

## Table des matieres

1. [Qu'est-ce que la scalabilite ?](#1-quest-ce-que-la-scalabilite)
2. [Procedure : Creer un nouvel acces utilisateur](#2-procedure--creer-un-nouvel-acces-utilisateur)
3. [Procedure : Ajouter un nouveau datamart](#3-procedure--ajouter-un-nouveau-datamart)
4. [Procedure : Augmenter l'espace de stockage](#4-procedure--augmenter-lespace-de-stockage)
5. [Procedure : Ajouter une nouvelle source de donnees](#5-procedure--ajouter-une-nouvelle-source-de-donnees)
6. [Procedure : Appliquer un changement SCD](#6-procedure--appliquer-un-changement-scd)
7. [Roles RBAC existants](#7-roles-rbac-existants)

---

## 1. Qu'est-ce que la scalabilite ?

La **scalabilite** est la capacite d'un systeme a s'adapter a une augmentation de charge sans degradation des performances.

| Type | Description | Exemple Azure SQL |
|------|-------------|-------------------|
| **Scale-up (vertical)** | Augmenter les ressources d'une machine | Changer le SKU : S0 -> S3 -> P1 |
| **Scale-out (horizontal)** | Ajouter des machines pour repartir la charge | Repliques en lecture seule, sharding |

### SKUs Azure SQL disponibles

| SKU | DTU | Stockage max | Cas d'usage |
|-----|-----|--------------|-------------|
| **S0** (actuel) | 10 | 250 Go | Developpement, petit volume |
| S1 | 20 | 250 Go | Volume modere |
| S2 | 50 | 250 Go | Volume moyen |
| S3 | 100 | 250 Go | Volume important |
| **P1** | 125 | 500 Go | Production, gros volume |
| P2 | 250 | 500 Go | Production intensive |

---

## 2. Procedure : Creer un nouvel acces utilisateur

### Prerequis
- Ticket GitHub Issue cree (label `changement`, priorite `P4-basse`)
- Profil utilisateur identifie (analyste, BI, ETL, admin)

### Etapes

1. **Identifier le role** selon le profil :

| Profil utilisateur | Role a attribuer |
|--------------------|------------------|
| Compte de service ETL | `role_etl_process` |
| Data Analyst | `role_analyst` |
| Power BI / Tableau | `role_bi_reader` |
| Administrateur DBA | `role_dwh_admin` |

2. **Executer la procedure** :

```sql
EXEC dwh.sp_creer_acces
    @login = 'prenom.nom@entreprise.com',
    @role  = 'role_analyst';
```

3. **Verifier** les permissions avec un test de connexion
4. **Documenter** dans le ticket : login cree, role attribue, date
5. **Notifier** l'utilisateur de ses identifiants (canal securise)

---

## 3. Procedure : Ajouter un nouveau datamart

### Prerequis
- Besoin d'analyse identifie et valide
- Ticket GitHub Issue (label `changement`)

### Etapes

1. **Identifier** les tables de faits et dimensions impliquees

2. **Creer la vue** dans le schema `dm` :

```sql
-- Exemple : datamart emploi par departement
CREATE VIEW dm.vm_emploi_departement AS
SELECT
    t.annee,
    g.departement_code,
    g.departement_nom,
    SUM(f.nb_actifs) AS total_actifs,
    SUM(f.nb_chomeurs) AS total_chomeurs,
    CASE
        WHEN SUM(f.nb_actifs) > 0
        THEN CAST(SUM(f.nb_chomeurs) AS FLOAT) / SUM(f.nb_actifs) * 100
        ELSE 0
    END AS taux_chomage_pct
FROM dwh.fait_emploi f
JOIN dwh.dim_temps t ON f.temps_id = t.temps_id
JOIN dwh.dim_geographie g ON f.geo_id = g.geo_id
WHERE g.est_actif = 1  -- SCD Type 2 : version courante uniquement
GROUP BY t.annee, g.departement_code, g.departement_nom;
```

3. **Attribuer les permissions** :

```sql
GRANT SELECT ON dm.vm_emploi_departement TO role_analyst;
GRANT SELECT ON dm.vm_emploi_departement TO role_bi_reader;
```

4. **Tester** avec les analystes
5. **Documenter** : mettre a jour le guide et le modele logique

---

## 4. Procedure : Augmenter l'espace de stockage

### Prerequis
- Alerte de stockage ou besoin anticipe
- Ticket GitHub Issue (label `infrastructure`)

### Etapes

1. **Verifier l'utilisation actuelle** :

```sql
SELECT * FROM analytics.v_monitoring_tables ORDER BY size_mb DESC;
```

2. **Modifier le SKU dans Terraform** :

```hcl
# Terraform/variables.tf
variable "sql_database_sku_name" {
  default = "S3"  # Ancien: "S0" -> Nouveau: "S3" (100 DTU)
}
```

3. **Appliquer le changement** :

```bash
cd Terraform
terraform plan    # Verifier les changements prevus
terraform apply   # Appliquer (sans interruption de service)
```

4. **Verifier** que la base repond correctement apres le changement
5. **Journaliser** le changement dans un ticket

> **Note** : Azure gere le basculement de SKU de maniere transparente, sans interruption de service.

---

## 5. Procedure : Ajouter une nouvelle source de donnees

### Prerequis
- Fichier source disponible (CSV, Parquet, API)
- Format et colonnes documentes

### Etapes detaillees

#### 5.1 Analyse de la source

| Question | Reponse a documenter |
|----------|---------------------|
| Format du fichier ? | CSV, Parquet, API REST... |
| Nombre de colonnes ? | Lister les colonnes et types |
| Volume (nb lignes) ? | Estimation |
| Frequence de mise a jour ? | Quotidien, mensuel, annuel |
| Qualite des donnees ? | Valeurs manquantes, doublons ? |

#### 5.2 Creer la table staging

```sql
-- Exemple pour une nouvelle source "transport"
CREATE TABLE stg.stg_transport (
    departement_code NVARCHAR(3),
    annee INT,
    nb_lignes_bus INT,
    nb_gares INT,
    -- ... autres colonnes
    date_chargement DATETIME DEFAULT GETDATE()
);
```

#### 5.3 Mapping vers les dimensions existantes

Identifier les cles de jointure avec les dimensions :

| Colonne source | Dimension cible | Cle de jointure |
|----------------|-----------------|-----------------|
| departement_code | `dwh.dim_geographie` | `departement_code` (avec `est_actif = 1`) |
| annee | `dwh.dim_temps` | `annee` |

#### 5.4 Creer la table de faits

```sql
CREATE TABLE dwh.fait_transport (
    transport_id INT IDENTITY(1,1) PRIMARY KEY,
    temps_id INT FOREIGN KEY REFERENCES dwh.dim_temps(temps_id),
    geo_id INT FOREIGN KEY REFERENCES dwh.dim_geographie(geo_id),
    nb_lignes_bus INT,
    nb_gares INT,
    date_creation DATETIME2 DEFAULT GETDATE()
);
```

#### 5.5 Developper l'ETL

Ajouter une fonction dans `analytics/etl/load_facts.py` :

```python
def load_fait_transport(engine, logger):
    logger.info("Chargement de fait_transport...")
    # 1. Lire depuis staging
    # 2. Mapper les cles etrangeres
    # 3. Inserer dans la table de faits
    # 4. Journaliser le resultat
```

#### 5.6 Ajouter les tests

```python
# Dans analytics/tests/test_dwh.py
def test_fait_transport_exists(self):
    result = self.engine.execute("SELECT COUNT(*) FROM dwh.fait_transport")
    self.assertGreaterEqual(result.scalar(), 0)
```

#### 5.7 Mettre a jour le deploiement

Ajouter le nouveau script SQL dans `Terraform/sql/deploy_dwh.py`.

---

## 6. Procedure : Appliquer un changement SCD

### SCD Type 1 (Ecrasement) - dim_activite

**Quand** : Correction d'erreur (faute d'orthographe, code incorrect)

```sql
-- Corriger un libelle NAF
EXEC dwh.sp_scd_type1_activite
    @naf_section_code = 'C',
    @nouveau_libelle = 'Industrie manufacturiere';
```

L'ancien libelle est journalise dans `dwh.log_etl` pour audit.

### SCD Type 2 (Historisation) - dim_geographie

**Quand** : Changement reel qu'on veut historiser (fusion de communes, renommage)

```sql
-- Une commune change de nom (fusion)
EXEC dwh.sp_scd_type2_geographie
    @commune_code = '62617',
    @nouveau_nom  = 'Henin-Beaumont (fusion)',
    @nouveau_code = '62427';
```

L'ancienne version est conservee avec `est_actif = 0` et `date_fin_validite` renseignee.

### SCD Type 3 (Colonne precedente) - dim_demographie

**Quand** : Renommage d'une categorie qu'on veut comparer avant/apres

```sql
-- Une categorie PCS est renommee
EXEC dwh.sp_scd_type3_demographie
    @pcs_code = '3',
    @nouveau_libelle = 'Cadres et professions intellectuelles superieures';
```

L'ancien libelle est conserve dans `ancien_pcs_libelle`.

### Consulter l'historique des changements

```sql
-- Historique des versions de communes
SELECT * FROM analytics.v_historique_geographie
WHERE commune_code = '62617' ORDER BY version;

-- Changements PCS (avant/apres)
SELECT * FROM analytics.v_changements_pcs
WHERE statut_changement = 'Modifie';

-- Resume global des operations SCD
SELECT * FROM analytics.v_resume_scd;
```

---

## 7. Roles RBAC existants

| Role | Permissions | Utilisateurs types |
|------|-------------|-------------------|
| `role_etl_process` | Lecture/ecriture sur `stg` et `dwh`, lecture sur `dm` et `analytics`, execution des procedures SCD | Comptes de service ETL |
| `role_analyst` | Lecture sur `dwh` (dimensions), lecture complete sur `dm` et `analytics` | Data Analysts |
| `role_bi_reader` | Lecture seule sur `dm` et `analytics`, dimensions cles | Power BI, Tableau |
| `role_dwh_admin` | Controle total, backup, restauration, gestion des acces | Administrateurs DBA |

### Creer un nouvel acces

```sql
EXEC dwh.sp_creer_acces
    @login = 'nouveau.utilisateur@entreprise.com',
    @role  = 'role_analyst';
```

### Verifier les permissions d'un utilisateur

```sql
-- Voir les roles d'un utilisateur
SELECT dp.name AS user_name, dr.name AS role_name
FROM sys.database_role_members rm
JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
JOIN sys.database_principals dr ON rm.role_principal_id = dr.principal_id
WHERE dp.name = 'prenom.nom@entreprise.com';
```
