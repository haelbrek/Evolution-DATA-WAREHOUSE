-- ============================================================
-- E5 - ENTREPOT DE DONNEES : Alimentation des Dimensions
-- Projet Data Engineering - Region Hauts-de-France
-- Donnees de reference (referentiels)
-- ============================================================

-- ============================================================
-- DIMENSION TEMPS : Annees de reference
-- ============================================================
TRUNCATE TABLE dwh.dim_temps;
GO

INSERT INTO dwh.dim_temps (annee, trimestre, mois, libelle_periode, est_annee_recensement)
VALUES
    -- Annees de recensement
    (2010, NULL, NULL, 'Annee 2010', 1),
    (2015, NULL, NULL, 'Annee 2015', 1),
    (2021, NULL, NULL, 'Annee 2021', 1),

    -- Annees intermediaires (naissances/deces, entreprises)
    (2012, NULL, NULL, 'Annee 2012', 0),
    (2013, NULL, NULL, 'Annee 2013', 0),
    (2014, NULL, NULL, 'Annee 2014', 0),
    (2016, NULL, NULL, 'Annee 2016', 0),
    (2017, NULL, NULL, 'Annee 2017', 0),
    (2018, NULL, NULL, 'Annee 2018', 0),
    (2019, NULL, NULL, 'Annee 2019', 0),
    (2020, NULL, NULL, 'Annee 2020', 0),
    (2022, NULL, NULL, 'Annee 2022', 0),
    (2023, NULL, NULL, 'Annee 2023', 0),
    (2024, NULL, NULL, 'Annee 2024', 0);
GO

PRINT 'Dimension TEMPS alimentee : ' + CAST(@@ROWCOUNT AS VARCHAR) + ' lignes';
GO

-- ============================================================
-- DIMENSION GEOGRAPHIE : Departements Hauts-de-France
-- ============================================================
-- Insertion des departements (niveau principal des donnees sources)
INSERT INTO dwh.dim_geographie
    (departement_code, departement_nom, region_code, region_nom, niveau_geo, geo_code_source)
VALUES
    ('02', 'Aisne', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-02'),
    ('59', 'Nord', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-59'),
    ('60', 'Oise', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-60'),
    ('62', 'Pas-de-Calais', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-62'),
    ('80', 'Somme', '32', 'Hauts-de-France', 'DEPARTEMENT', '2024-DEP-80');
GO

PRINT 'Dimension GEOGRAPHIE alimentee : 5 departements';
GO

-- ============================================================
-- DIMENSION DEMOGRAPHIE : Codes de reference
-- ============================================================

-- Sexe
INSERT INTO dwh.dim_demographie (sexe_code, sexe_libelle, age_code, pcs_code)
VALUES
    ('M', 'Masculin', '_T', '_T'),
    ('F', 'Feminin', '_T', '_T'),
    ('_T', 'Total', '_T', '_T');
GO

-- PCS (Professions et Categories Socioprofessionnelles)
INSERT INTO dwh.dim_demographie (sexe_code, sexe_libelle, age_code, pcs_code, pcs_libelle, pcs_niveau)
VALUES
    ('_T', 'Total', '_T', '1', 'Agriculteurs exploitants', 1),
    ('_T', 'Total', '_T', '2', 'Artisans, commercants, chefs d''entreprise', 1),
    ('_T', 'Total', '_T', '3', 'Cadres et professions intellectuelles superieures', 1),
    ('_T', 'Total', '_T', '4', 'Professions intermediaires', 1),
    ('_T', 'Total', '_T', '5', 'Employes', 1),
    ('_T', 'Total', '_T', '6', 'Ouvriers', 1),
    ('_T', 'Total', '_T', '7', 'Retraites', 1),
    ('_T', 'Total', '_T', '9', 'Autres personnes sans activite professionnelle', 1);
GO

-- Tranches d'age
INSERT INTO dwh.dim_demographie (sexe_code, age_code, age_libelle, age_min, age_max, pcs_code)
VALUES
    ('_T', 'Y15T24', '15-24 ans', 15, 24, '_T'),
    ('_T', 'Y25T54', '25-54 ans', 25, 54, '_T'),
    ('_T', 'Y_GE55', '55 ans et plus', 55, 999, '_T'),
    ('_T', 'Y_GE15', '15 ans et plus', 15, 999, '_T'),
    ('_T', 'Y15T64', '15-64 ans (actifs)', 15, 64, '_T'),
    ('_T', 'Y_LT30', 'Moins de 30 ans', 0, 29, '_T'),
    ('_T', 'Y30T39', '30-39 ans', 30, 39, '_T'),
    ('_T', 'Y40T49', '40-49 ans', 40, 49, '_T'),
    ('_T', 'Y50T59', '50-59 ans', 50, 59, '_T'),
    ('_T', 'Y_GE60', '60 ans et plus', 60, 999, '_T'),
    ('_T', 'Y60T74', '60-74 ans', 60, 74, '_T'),
    ('_T', 'Y_GE75', '75 ans et plus', 75, 999, '_T');
GO

PRINT 'Dimension DEMOGRAPHIE alimentee';
GO

-- ============================================================
-- DIMENSION ACTIVITE : Sections NAF et formes juridiques
-- ============================================================

-- Sections NAF (Nomenclature d'Activite Francaise)
INSERT INTO dwh.dim_activite (naf_section_code, naf_section_libelle, secteur_activite, forme_juridique_code)
VALUES
    ('A', 'Agriculture, sylviculture et peche', 'Primaire', '_T'),
    ('B', 'Industries extractives', 'Secondaire', '_T'),
    ('C', 'Industrie manufacturiere', 'Secondaire', '_T'),
    ('D', 'Production et distribution d''electricite, gaz', 'Secondaire', '_T'),
    ('E', 'Production et distribution d''eau, assainissement', 'Secondaire', '_T'),
    ('F', 'Construction', 'Secondaire', '_T'),
    ('G', 'Commerce, reparation d''automobiles', 'Tertiaire', '_T'),
    ('H', 'Transports et entreposage', 'Tertiaire', '_T'),
    ('I', 'Hebergement et restauration', 'Tertiaire', '_T'),
    ('J', 'Information et communication', 'Tertiaire', '_T'),
    ('K', 'Activites financieres et d''assurance', 'Tertiaire', '_T'),
    ('L', 'Activites immobilieres', 'Tertiaire', '_T'),
    ('M', 'Activites specialisees, scientifiques et techniques', 'Tertiaire', '_T'),
    ('N', 'Activites de services administratifs et de soutien', 'Tertiaire', '_T'),
    ('O', 'Administration publique', 'Tertiaire', '_T'),
    ('P', 'Enseignement', 'Tertiaire', '_T'),
    ('Q', 'Sante humaine et action sociale', 'Tertiaire', '_T'),
    ('R', 'Arts, spectacles et activites recreatives', 'Tertiaire', '_T'),
    ('S', 'Autres activites de services', 'Tertiaire', '_T'),
    ('_T', 'Toutes activites', 'Total', '_T');
GO

-- Formes juridiques
INSERT INTO dwh.dim_activite (naf_section_code, forme_juridique_code, forme_juridique_libelle, type_entreprise)
VALUES
    ('_T', '10', 'Entrepreneur individuel', 'Micro'),
    ('_T', '54', 'SARL', 'PME'),
    ('_T', '57', 'SAS', 'PME'),
    ('_T', 'MICRO', 'Micro-entrepreneur', 'Micro'),
    ('_T', 'ENTIND_X_MICRO', 'Entrepreneur individuel hors micro', 'TPE'),
    ('_T', 'OTH_SIDE', 'Autres formes juridiques', 'Autres');
GO

PRINT 'Dimension ACTIVITE alimentee';
GO

-- ============================================================
-- DIMENSION INDICATEUR : Codes FILOSOFI et autres
-- ============================================================

INSERT INTO dwh.dim_indicateur (indicateur_code, indicateur_libelle, unite_mesure, domaine, source_donnees)
VALUES
    -- Indicateurs de revenus
    ('MED_SL', 'Niveau de vie median', 'EUR/an', 'REVENUS', 'FILOSOFI'),
    ('D1_SL', 'Premier decile de niveau de vie', 'EUR/an', 'REVENUS', 'FILOSOFI'),
    ('D9_SL', 'Neuvieme decile de niveau de vie', 'EUR/an', 'REVENUS', 'FILOSOFI'),
    ('IR_D9_D1_SL', 'Rapport interdecile D9/D1', 'Ratio', 'REVENUS', 'FILOSOFI'),

    -- Indicateurs de pauvrete
    ('PR_MD60', 'Taux de pauvrete (seuil 60% mediane)', '%', 'PAUVRETE', 'FILOSOFI'),

    -- Structure des revenus
    ('S_EI_DI', 'Part des revenus d''activite', '%', 'REVENUS', 'FILOSOFI'),
    ('S_EI_DI_SAL', 'Part des salaires', '%', 'REVENUS', 'FILOSOFI'),
    ('S_EI_DI_N_SAL', 'Part des revenus non salaries', '%', 'REVENUS', 'FILOSOFI'),
    ('S_RET_PEN_DI', 'Part des pensions et retraites', '%', 'REVENUS', 'FILOSOFI'),
    ('S_SOC_BEN_DI', 'Part des prestations sociales', '%', 'REVENUS', 'FILOSOFI'),
    ('S_SOC_BEN_DI_FAM_BEN', 'Part des prestations familiales', '%', 'REVENUS', 'FILOSOFI'),
    ('S_SOC_BEN_DI_HOU_BEN', 'Part des allocations logement', '%', 'REVENUS', 'FILOSOFI'),
    ('S_SOC_BEN_DI_MIN_SOC', 'Part des minima sociaux', '%', 'REVENUS', 'FILOSOFI'),
    ('S_INC_ASS_DI', 'Part des indemnites chomage', '%', 'REVENUS', 'FILOSOFI'),

    -- Comptages
    ('NUM_HH', 'Nombre de menages fiscaux', 'Nombre', 'POPULATION', 'FILOSOFI'),
    ('NUM_CU', 'Nombre d''unites de consommation', 'Nombre', 'POPULATION', 'FILOSOFI'),
    ('NUM_PER', 'Nombre de personnes', 'Nombre', 'POPULATION', 'FILOSOFI'),

    -- Evenements demographiques
    ('LVB', 'Naissances vivantes', 'Nombre', 'DEMOGRAPHIE', 'INSEE'),
    ('DTH', 'Deces', 'Nombre', 'DEMOGRAPHIE', 'INSEE'),

    -- Emploi
    ('POP', 'Population', 'Nombre', 'POPULATION', 'INSEE RP'),
    ('DWELLINGS', 'Nombre de logements', 'Nombre', 'LOGEMENT', 'INSEE RP'),
    ('DWELLINGS_POPSIZE', 'Population des menages', 'Nombre', 'LOGEMENT', 'INSEE RP');
GO

PRINT 'Dimension INDICATEUR alimentee';
GO

-- ============================================================
-- DIMENSION LOGEMENT : Types et caracteristiques
-- ============================================================

INSERT INTO dwh.dim_logement (occupation_code, occupation_libelle, surpeuplement_code, surpeuplement_libelle)
VALUES
    ('DW_MAIN', 'Residence principale', '0', 'Non surpeuple'),
    ('DW_MAIN', 'Residence principale', '1', 'Surpeuple'),
    ('DW_MAIN', 'Residence principale', '_T', 'Total');
GO

-- Types de menages
INSERT INTO dwh.dim_logement (type_menage_code, type_menage_libelle, taille_menage_code, taille_menage_libelle)
VALUES
    ('110', 'Personne seule homme', '1', '1 personne'),
    ('111', 'Personne seule femme', '1', '1 personne'),
    ('11', 'Personne seule', '1', '1 personne'),
    ('12', 'Autres menages sans famille', '_T', 'Variable'),
    ('MF21', 'Couple sans enfant', '2', '2 personnes'),
    ('MF221', 'Couple avec 1 enfant', '3', '3 personnes'),
    ('MF222', 'Couple avec 2 enfants ou plus', '4+', '4 personnes ou plus'),
    ('220', 'Famille monoparentale homme', '_T', 'Variable'),
    ('223', 'Famille monoparentale femme', '_T', 'Variable'),
    ('_T', 'Total tous types', '_T', 'Total');
GO

PRINT 'Dimension LOGEMENT alimentee';
GO

PRINT '========================================';
PRINT 'TOUTES LES DIMENSIONS ONT ETE ALIMENTEES';
PRINT '========================================';
