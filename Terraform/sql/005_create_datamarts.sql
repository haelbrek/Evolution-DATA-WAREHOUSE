-- ============================================================
-- E5 - ENTREPOT DE DONNEES : Datamarts et Vues Analytiques
-- Projet Data Engineering - Region Hauts-de-France
-- ============================================================

-- ============================================================
-- DATAMART DEMOGRAPHIE
-- Vue agregee pour les analyses demographiques
-- ============================================================
IF OBJECT_ID('dm.vm_demographie_departement', 'V') IS NOT NULL
    DROP VIEW dm.vm_demographie_departement;
GO

CREATE VIEW dm.vm_demographie_departement AS
SELECT
    t.annee,
    g.departement_code,
    g.departement_nom,

    -- Population
    SUM(p.population) AS population_totale,

    -- Evenements
    SUM(e.naissances) AS naissances,
    SUM(e.deces) AS deces,
    SUM(e.solde_naturel) AS solde_naturel,

    -- Taux
    CASE WHEN SUM(p.population) > 0
        THEN CAST(SUM(e.naissances) AS FLOAT) / SUM(p.population) * 1000
        ELSE NULL END AS taux_natalite,
    CASE WHEN SUM(p.population) > 0
        THEN CAST(SUM(e.deces) AS FLOAT) / SUM(p.population) * 1000
        ELSE NULL END AS taux_mortalite

FROM dwh.fait_population p
INNER JOIN dwh.dim_temps t ON p.temps_id = t.temps_id
INNER JOIN dwh.dim_geographie g ON p.geo_id = g.geo_id
LEFT JOIN dwh.fait_evenements_demo e
    ON e.temps_id = t.temps_id AND e.geo_id = g.geo_id
WHERE g.niveau_geo = 'DEPARTEMENT'
GROUP BY t.annee, g.departement_code, g.departement_nom;
GO

PRINT 'Vue dm.vm_demographie_departement creee';
GO

-- ============================================================
-- DATAMART ECONOMIE
-- Vue agregee pour les analyses economiques
-- ============================================================
IF OBJECT_ID('dm.vm_entreprises_departement', 'V') IS NOT NULL
    DROP VIEW dm.vm_entreprises_departement;
GO

CREATE VIEW dm.vm_entreprises_departement AS
SELECT
    t.annee,
    g.departement_code,
    g.departement_nom,
    a.secteur_activite,
    a.naf_section_code,
    a.naf_section_libelle,

    -- Creations
    SUM(e.nb_creations_entreprises) AS nb_creations,
    SUM(e.nb_creations_micro) AS nb_creations_micro,
    SUM(e.nb_creations_ei) AS nb_creations_ei,

    -- Par profil
    SUM(e.nb_creations_hommes) AS creations_hommes,
    SUM(e.nb_creations_femmes) AS creations_femmes,
    SUM(e.nb_creations_moins_30ans) AS creations_jeunes

FROM dwh.fait_entreprises e
INNER JOIN dwh.dim_temps t ON e.temps_id = t.temps_id
INNER JOIN dwh.dim_geographie g ON e.geo_id = g.geo_id
INNER JOIN dwh.dim_activite a ON e.activite_id = a.activite_id
WHERE g.niveau_geo = 'DEPARTEMENT'
GROUP BY t.annee, g.departement_code, g.departement_nom,
         a.secteur_activite, a.naf_section_code, a.naf_section_libelle;
GO

PRINT 'Vue dm.vm_entreprises_departement creee';
GO

-- ============================================================
-- DATAMART SOCIAL
-- Vue agregee pour les analyses sociales (revenus, pauvrete)
-- ============================================================
IF OBJECT_ID('dm.vm_revenus_departement', 'V') IS NOT NULL
    DROP VIEW dm.vm_revenus_departement;
GO

CREATE VIEW dm.vm_revenus_departement AS
SELECT
    t.annee,
    g.departement_code,
    g.departement_nom,

    -- Revenus
    AVG(r.revenu_median) AS revenu_median_moyen,
    MIN(r.revenu_d1) AS revenu_d1_min,
    MAX(r.revenu_d9) AS revenu_d9_max,
    AVG(r.rapport_interdecile) AS rapport_interdecile_moyen,

    -- Pauvrete
    AVG(r.taux_pauvrete) AS taux_pauvrete_moyen,

    -- Structure revenus
    AVG(r.part_revenus_activite) AS part_revenus_activite,
    AVG(r.part_pensions_retraites) AS part_pensions_retraites,
    AVG(r.part_prestations_sociales) AS part_prestations_sociales,

    -- Volumes
    SUM(r.nb_menages) AS nb_menages_total,
    SUM(r.nb_personnes) AS nb_personnes_total

FROM dwh.fait_revenus r
INNER JOIN dwh.dim_temps t ON r.temps_id = t.temps_id
INNER JOIN dwh.dim_geographie g ON r.geo_id = g.geo_id
WHERE g.niveau_geo = 'DEPARTEMENT'
GROUP BY t.annee, g.departement_code, g.departement_nom;
GO

PRINT 'Vue dm.vm_revenus_departement creee';
GO

-- ============================================================
-- DATAMART EMPLOI
-- Vue agregee pour les analyses emploi/chomage
-- ============================================================
IF OBJECT_ID('dm.vm_emploi_departement', 'V') IS NOT NULL
    DROP VIEW dm.vm_emploi_departement;
GO

CREATE VIEW dm.vm_emploi_departement AS
SELECT
    t.annee,
    g.departement_code,
    g.departement_nom,
    d.pcs_code,
    d.pcs_libelle,

    -- Population active
    SUM(e.population_active) AS population_active,
    SUM(e.population_en_emploi) AS population_en_emploi,
    SUM(e.population_chomeurs) AS population_chomeurs,

    -- Taux
    CASE WHEN SUM(e.population_active) > 0
        THEN SUM(e.population_chomeurs) / SUM(e.population_active) * 100
        ELSE NULL END AS taux_chomage_calc

FROM dwh.fait_emploi e
INNER JOIN dwh.dim_temps t ON e.temps_id = t.temps_id
INNER JOIN dwh.dim_geographie g ON e.geo_id = g.geo_id
INNER JOIN dwh.dim_demographie d ON e.demo_id = d.demo_id
WHERE g.niveau_geo = 'DEPARTEMENT'
GROUP BY t.annee, g.departement_code, g.departement_nom, d.pcs_code, d.pcs_libelle;
GO

PRINT 'Vue dm.vm_emploi_departement creee';
GO

-- ============================================================
-- DATAMART LOGEMENT
-- Vue agregee pour les analyses logement
-- ============================================================
IF OBJECT_ID('dm.vm_logement_departement', 'V') IS NOT NULL
    DROP VIEW dm.vm_logement_departement;
GO

CREATE VIEW dm.vm_logement_departement AS
SELECT
    t.annee,
    g.departement_code,
    g.departement_nom,

    -- Stock logements
    SUM(l.nb_residences_principales) AS nb_residences_principales,
    SUM(l.nb_logements_surpeuples) AS nb_logements_surpeuples,

    -- Taux surpeuplement
    CASE WHEN SUM(l.nb_residences_principales) > 0
        THEN SUM(l.nb_logements_surpeuples) / SUM(l.nb_residences_principales) * 100
        ELSE NULL END AS taux_surpeuplement

FROM dwh.fait_logement l
INNER JOIN dwh.dim_temps t ON l.temps_id = t.temps_id
INNER JOIN dwh.dim_geographie g ON l.geo_id = g.geo_id
WHERE g.niveau_geo = 'DEPARTEMENT'
GROUP BY t.annee, g.departement_code, g.departement_nom;
GO

PRINT 'Vue dm.vm_logement_departement creee';
GO

-- ============================================================
-- VUE SYNTHETIQUE TABLEAU DE BORD
-- Indicateurs cles par departement et annee
-- ============================================================
IF OBJECT_ID('analytics.v_tableau_bord_territorial', 'V') IS NOT NULL
    DROP VIEW analytics.v_tableau_bord_territorial;
GO

CREATE VIEW analytics.v_tableau_bord_territorial AS
SELECT
    g.departement_code,
    g.departement_nom,
    t.annee,

    -- Demographie
    demo.population_totale,
    demo.naissances,
    demo.deces,
    demo.solde_naturel,
    demo.taux_natalite,
    demo.taux_mortalite,

    -- Economie
    ent.nb_creations AS creations_entreprises,

    -- Social
    rev.revenu_median_moyen,
    rev.taux_pauvrete_moyen,
    rev.rapport_interdecile_moyen,

    -- Emploi
    emp.taux_chomage_calc AS taux_chomage,

    -- Logement
    log.taux_surpeuplement

FROM dwh.dim_geographie g
CROSS JOIN dwh.dim_temps t
LEFT JOIN dm.vm_demographie_departement demo
    ON demo.departement_code = g.departement_code AND demo.annee = t.annee
LEFT JOIN (
    SELECT annee, departement_code, SUM(nb_creations) AS nb_creations
    FROM dm.vm_entreprises_departement
    GROUP BY annee, departement_code
) ent ON ent.departement_code = g.departement_code AND ent.annee = t.annee
LEFT JOIN dm.vm_revenus_departement rev
    ON rev.departement_code = g.departement_code AND rev.annee = t.annee
LEFT JOIN (
    SELECT annee, departement_code,
           SUM(population_chomeurs)/NULLIF(SUM(population_active),0)*100 AS taux_chomage_calc
    FROM dm.vm_emploi_departement
    GROUP BY annee, departement_code
) emp ON emp.departement_code = g.departement_code AND emp.annee = t.annee
LEFT JOIN dm.vm_logement_departement log
    ON log.departement_code = g.departement_code AND log.annee = t.annee
WHERE g.niveau_geo = 'DEPARTEMENT';
GO

PRINT 'Vue analytics.v_tableau_bord_territorial creee';
GO

PRINT '========================================';
PRINT 'DATAMARTS ET VUES ANALYTIQUES CREES';
PRINT '========================================';
