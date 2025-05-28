# === 0. Importation des modules ===
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime
import os
import traceback

try:
    # === 1. Création de la session Spark ===
    spark = SparkSession.builder \
        .appName("StreamingVentes") \
        .getOrCreate()

    # Réduction du niveau de log
    spark.sparkContext.setLogLevel("WARN")

    # === 2. Définition du schéma des données CSV ===
    schema = StructType() \
        .add("id", IntegerType()) \
        .add("produit", StringType()) \
        .add("quantite", IntegerType()) \
        .add("prix", DoubleType()) \
        .add("date_vente", TimestampType())

    # === 3. Lecture en streaming du dossier d’entrée ===
    df_stream = spark.readStream \
        .option("sep", ",") \
        .option("header", "true") \
        .schema(schema) \
        .csv("C:/spark_streaming/stream_input")  # Dossier surveillé

    # === 4. Transformation des données : ajout du chiffre d'affaires ===
    df_transforme = df_stream.withColumn("chiffre_affaire", col("quantite") * col("prix"))

    # === 5. Affichage des données en streaming dans la console ===
    query_console = df_transforme.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # === 6. Export des données vers un dossier horodaté ===
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"stream_output/ventes_{timestamp}"
    checkpoint_path = f"{output_path}/checkpoint"

    query_export = df_transforme.writeStream \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .format("csv") \
        .start()

    # === 7. Maintien du programme actif ===
    query_console.awaitTermination()
    query_export.awaitTermination()

except Exception as e:
    # === 8. Gestion d’erreur : écriture dans un fichier log ===
    error_message = f"[{datetime.now()}] Erreur : {str(e)}\n{traceback.format_exc()}\n"
    print(error_message)  # Affichage dans le terminal

    # Sauvegarde de l'erreur dans un fichier texte
    with open("log_erreurs.txt", "a", encoding="utf-8") as log_file:
        log_file.write(error_message)
