# EcommerceAnalytics

Projet Spark & Scala - Système d'analyse de données e-commerce distribué.
Projet final du module Data Engineer - Spark & Scala, groupe 3.

## Prérequis

Pour compiler et exécuter ce projet, il faut avoir installé :

- **Java JDK 11** (ou une version compatible avec Spark 3.5.0)
- **Scala 2.12.18**
- **SBT 1.9.7** (ou une version récente de SBT)
- **Apache Spark 3.5.0** (les dépendances sont gérées automatiquement par SBT, pas besoin d'installer Spark séparément si on exécute avec `sbt run`)

Sous Windows, pour pouvoir écrire des fichiers (CSV/Parquet) avec Spark, il faut aussi :
- `winutils.exe` et `hadoop.dll` (version hadoop-3.3.5), à placer dans un dossier `C:\hadoop\bin`
- Ajouter `C:\hadoop\bin` à la variable d'environnement `PATH` de Windows

## Compilation

Dans un terminal, à la racine du projet :

```
sbt compile
```

Pour générer le JAR exécutable (avec le plugin sbt-assembly) :

```
sbt assembly
```

Le JAR est généré dans :

```
target/scala-2.12/EcommerceAnalytics-assembly-1.0.jar
```

## Exécution locale

Avec SBT, directement :

```
sbt run
```

Si plusieurs objets `main` existent dans le projet (comme les fichiers de test `TestQ21`, `TestQ22`...), SBT va demander de choisir lequel exécuter. Il faut choisir `com.ecommerce.analytics.MainApp` pour lancer le pipeline complet.

On peut aussi lancer directement `MainApp.scala` depuis IntelliJ IDEA (clic droit sur le fichier → Run 'MainApp').

Les chemins des fichiers de données et le dossier de sortie sont définis dans `src/main/resources/application.conf`, rien n'est codé en dur dans le code Scala.

## Déploiement (spark-submit)

Pour exécuter le JAR sur un cluster Spark :

```
spark-submit --class com.ecommerce.analytics.MainApp --master <url-du-cluster> target/scala-2.12/EcommerceAnalytics-assembly-1.0.jar
```

En local, avec le JAR généré :

```
spark-submit --class com.ecommerce.analytics.MainApp --master local[*] target/scala-2.12/EcommerceAnalytics-assembly-1.0.jar
```

## Structure du projet

```
EcommerceAnalytics/
├── build.sbt
├── README.md
├── EQUIPE.md
├── CONTRIBUTIONS.md
├── src/
│   ├── main/
│   │   ├── scala/com/ecommerce/
│   │   │   ├── analytics/
│   │   │   │   ├── DataIngestion.scala
│   │   │   │   ├── DataValidation.scala
│   │   │   │   ├── DataQualityReport.scala
│   │   │   │   ├── DataTransformation.scala
│   │   │   │   ├── TimeFeatures.scala
│   │   │   │   ├── Analytics.scala
│   │   │   │   ├── RFMSegmentation.scala
│   │   │   │   ├── ProductAnalytics.scala
│   │   │   │   └── MainApp.scala
│   │   │   ├── models/       (case classes Transaction, User, Product, Merchant)
│   │   │   └── utils/        (ConfigLoader, DataFrameWriterUtils, SparkOptimizations)
│   │   └── resources/
│   │       ├── application.conf
│   │       └── data/         (transactions.csv, users.json, products.parquet, merchants.csv)
└── output/                   (résultats générés : CSV + Parquet)
```

## Questions bonus réalisées

En plus du tronc commun, les bonus suivants ont été implémentés et testés sur les vraies données du projet :

- **Q2.5** — Vérification de l'intégrité référentielle entre transactions et tables de référence (users, products, merchants), via jointures `left_anti`
- **Q3.4** — Détection des transactions suspectes (écart au panier moyen, horaire nocturne, délai court entre achats, paiement en crypto-monnaie)
- **Q4.3** — Segmentation RFM (Récence/Fréquence/Montant) des clients, avec croisement par segment client existant
- **Q4.4** — Analyse produits et catégories (top 10 produits, chiffre d'affaires par catégorie/région, chiffre d'affaires par mode de paiement/période)
- **Q5.3** — Mesure du gain apporté par les optimisations (cache)

## Q5.3 — Mesure du gain des optimisations (bonus)

Test effectué en exécutant 3 agrégations successives (`groupBy` sur category, payment_method, location) sur le même DataFrame de transactions validées, avec et sans mise en cache.

| Mesure | Résultat |
|---|---|
| Durée sans cache | 5,55 secondes |
| Durée avec cache | 3,24 secondes |
| Gain | 41,66 % |

Le cache évite à Spark de relire et retraiter les données depuis le disque à chaque nouvelle action, ce qui explique le gain de temps observé dès la 2ème opération.