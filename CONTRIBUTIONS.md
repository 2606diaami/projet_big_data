# Contributions — Projet EcommerceAnalytics

## Tableau des tâches

| Tâche | Responsable (officiel) | Réalisé par | Heures passées (approx.) | Statut |
|---|---|---|---|---|
| Q1.1-1.3 — Structure projet, README, config.sbt | Racky Sall | Racky Sall | 3h | ✅ |
| Q2.1 — Ingestion des données | Racky Sall | Racky Sall | 6h | ✅ |
| Q2.2 — Validation des données | Racky Sall | Racky Sall | 4h | ✅ |
| Q2.3 — Gestion des erreurs et des rejets | Racky Sall | Racky Sall | 3h | ✅ |
| Q2.4 — Rapport de qualité | Racky Sall | Racky Sall | 3h | ✅ |
| Q2.5 (bonus) — Intégrité référentielle | Racky Sall | Racky Sall | 1h30 | ✅ |
| Q7.1 — Configuration (application.conf) | Racky Sall | Racky Sall | 1h | ✅ |
| Configuration de l'environnement Windows/Hadoop | — | Racky Sall | 8h | ✅ |
| Q3.1-3.4 — Transformation, UDF, fenêtres, transactions suspectes | Chérif Sow | Chérif Sow | 6h | ✅ |
| Q4.1 — KPI marchands | Aminata Dia | Aminata Dia | 1h30 | ✅ |
| Q4.2 — Cohortes et rétention | Aminata Dia | Aminata Dia | 1h30 | ✅ |
| Q4.3 (bonus) — Segmentation RFM | — | Aminata Dia | 1h30 | ✅ |
| Q4.4 (bonus) — Analyse produits/catégories | — | Aminata Dia | 1h | ✅ |
| Q5.1 — Cache | Aminata Dia | Aminata Dia | 1h | ✅ |
| Q5.2 — Broadcast join | Aminata Dia | Aminata Dia | 1h | ✅ |
| Q5.3 (bonus) — Mesure du gain | — | Aminata Dia | 30min | ✅ |
| Q6.1 — Orchestration (MainApp) | Aminata Dia | Aminata Dia | 1h | ✅ |
| Fusion Git des 3 parties + résolution erreurs d'environnement | — | Aminata Dia | 3h | ✅ |

**Remarque (Partie A)** : l'implémentation de la Partie A intégrée au dépôt principal a été réalisée par Aminata Dia. Racky Sall a développé sa propre implémentation de cette partie (dépôt `rackysall33-code/scala`), l'a utilisée pour valider les résultats du rapport de qualité, et a corrigé le traitement des valeurs nulles dans la validation des transactions.

**Total estimé : ~30h pour Racky Sall, ~20h pour Aminata Dia, ~6h pour Chérif Sow**

## Difficultés rencontrées

- **Configuration Windows/Hadoop** : erreur `HADOOP_HOME not set` au premier lancement de Spark sous Windows. Résolu en installant `winutils.exe` et `hadoop.dll` (Hadoop 3.3.5) dans `C:\hadoop\bin`, en ajoutant ce chemin au PATH système, et en ajoutant `System.setProperty("hadoop.home.dir", "C:\\hadoop")` au démarrage de l'application.
- **Cast de type JSON** : erreur `CANNOT_UP_CAST_DATATYPE` sur la colonne `age` du dataset `users` (Spark infère un `BIGINT` depuis le JSON). Résolu en déclarant `age: Long` dans la case class `User` plutôt que `Int`.
- **Limite des tuples Scala** : impossible de créer une ligne anonyme à plus de 22 champs pour les données de test. Résolu en utilisant une case class nommée plutôt qu'un tuple.
- **Fusion du travail en équipe via Git** : coordination nécessaire pour récupérer le code de la Partie B depuis le dépôt du camarade (ajout d'un remote secondaire, `git fetch`, puis `git checkout` ciblé sur les deux fichiers concernés, sans écraser le reste du projet).
- **Données réelles avec valeurs manquantes** : certains marchands et produits référencés dans les transactions n'existent pas dans les tables de référence (orphelins détectés en Q2.5 : 400 user_id, 300 product_id, 250 merchant_id). Géré par des jointures `left` qui conservent la transaction avec des colonnes `NULL` plutôt que de la perdre.
- **Valeurs nulles en validation** : en Spark, `null <= 0` vaut `null`, donc une ligne dont le montant est manquant échappe à la règle de rejet. Détecté en comparant deux implémentations (137 transactions de différence), corrigé en testant `isNull` avant la comparaison.
- **Format des données** : `users.json` est au format une-ligne-par-objet et non un tableau JSON ; ajustement des options de lecture et des noms de colonnes (snake_case → camelCase) pour que le mapping vers les case classes fonctionne.

## Décisions techniques

- Utilisation de `Dataset[T]` typé (via case classes) plutôt que `DataFrame` brut pour l'ingestion, afin de détecter les erreurs de schéma plus tôt.
- Double écriture systématique des résultats en CSV (pour lecture humaine) et Parquet (pour performance et réutilisation).
- Toutes les analyses bonus (Q2.5, Q4.3, Q4.4, Q5.3) ont été réalisées sur les **vraies données** du projet (après ingestion et validation), et non sur des données simulées, afin de garantir des résultats représentatifs du jeu de données réel.