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
│   │   │   ├── analytics/    (DataIngestion, DataValidation, Analytics, MainApp...)
│   │   │   ├── models/       (case classes Transaction, User, Product, Merchant)
│   │   │   └── utils/        (ConfigLoader, DataFrameWriterUtils)
│   │   └── resources/
│   │       ├── application.conf
│   │       └── data/         (transactions.csv, users.json, products.parquet, merchants.csv)
└── output/                   (résultats générés : CSV + Parquet)
```