package org.hicham;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        System.setProperty("spark.driver.extraJavaOptions",
                "--add-opens=java.base/javax.security.auth=ALL-UNNAMED " +
                        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
                        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED");

        // Disable Hadoop's security to avoid the Subject error
        System.setProperty("hadoop.home.dir", "/");

        SparkSession spark = SparkSession.builder()
                .appName("TotalVentesParVille")
                .config("spark.master", "local[*]")
                .getOrCreate();

        // Lecture du fichier texte
        Dataset<Row> ventesDF = spark.read()
                .option("header", "false")
                .option("inferSchema", "true")
                .csv("ventes.txt")
                .toDF("date", "ville", "produit", "prix");

        // Calcul du total des ventes par ville
        Dataset<Row> totalParVille = ventesDF.groupBy("ville")
                .agg(sum("prix").alias("total_ventes"))
                .orderBy("ville");

        // Affichage des résultats
        totalParVille.show();

        // Sauvegarde des résultats
        totalParVille.write().csv("resultats_ventes_par_ville");

        // Extraction de l'année à partir de la date
        Dataset<Row> ventesAvecAnnee = ventesDF
                .withColumn("annee", year(to_date(col("date"), "yyyy-MM-dd")));

        // Calcul du total par ville et année
        Dataset<Row> totalParVilleAnnee = ventesAvecAnnee
                .groupBy("ville", "annee")
                .agg(sum("prix").alias("total_ventes"))
                .orderBy("ville", "annee");

        // Affichage des résultats
        totalParVilleAnnee.show();

        // Sauvegarde des résultats
        totalParVilleAnnee.write().csv("resultats_ventes_par_ville_annee");
        spark.stop();

        }
    }