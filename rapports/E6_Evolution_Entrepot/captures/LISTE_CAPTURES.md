# Liste des captures Azure à prendre
# Projet E6 - Evolution Entrepot de Données
# Hamza Elbrek - Fevrier 2026

Toutes les captures sont à placer dans ce dossier (captures/)
avec exactement le nom de fichier indiqué.

---

## 1. azure_log_etl.png

**Où :** Azure SQL Database → Query editor (portail)
**Quoi :** Lance cette requête et prends la capture du résultat :
    SELECT TOP 20 * FROM dwh.log_etl ORDER BY date_execution DESC

---

## 2. azure_backup_config.png

**Où :** Azure SQL Database → menu gauche → Backups
**Quoi :** L'onglet qui montre la configuration PITR (14 jours)
           et la rétention long terme

---

## 3. azure_datalake_bacpac.png

**Où :** Storage Account → Containers → raw → dossier backups/
**Quoi :** La liste des fichiers .bacpac avec leur nom horodaté

---

## 4. azure_nouvelles_tables.png

**Où :** Azure SQL Database → Query editor
**Quoi :** Lance cette requête et prends la capture du résultat :
    SELECT TOP 5 * FROM dwh.fait_emploi

---

## 5. azure_rbac_roles.png

**Où :** Azure SQL Database → Query editor
**Quoi :** Lance cette requête et prends la capture du résultat :
    SELECT name FROM sys.database_principals WHERE type = 'R'

---

## 6. azure_scd_dim_geographie.png

**Où :** Azure SQL Database → Query editor
**Quoi :** Lance cette requête et prends la capture du résultat :
    SELECT TOP 5 geo_id, nom_commune, est_actif, version,
                 date_debut_validite
    FROM dwh.dim_geographie

---

## 7. tests_49_succes.png

**Où :** Ton terminal local (dans le projet)
**Quoi :** Le résultat de la commande :
    python test_dwh.py
    → doit afficher : 49 tests, 0 failures

---

## 8. azure_resource_group.png

**Où :** Portail Azure → Resource groups → ton resource group du projet
**Quoi :** La vue d'ensemble avec toutes les ressources
           (SQL Server, Storage Account, etc.)

---

## 9. azure_datawarehouse_schema.png

**Où :** Azure SQL Database → Query editor
**Quoi :** Lance cette requête et prends la capture du résultat :
    SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN ('stg','dwh','dm','analytics')
    ORDER BY TABLE_SCHEMA, TABLE_NAME

---

## 10. azure_log_erreurs.png

**Où :** Azure SQL Database → Query editor
**Quoi :** Lance cette requête et prends la capture du résultat :
    SELECT TOP 20 * FROM dwh.log_erreurs ORDER BY date_erreur DESC

---

Une fois les 10 fichiers placés ici, dis-le à Claude
pour qu'il vérifie l'intégration dans le rapport.
