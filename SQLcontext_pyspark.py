%pyspark
yeardoi = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").load("s3://papers-archive/dblp/yeardoi").withColumnRenamed("_c0", "year").withColumnRenamed("_c1", "doi").cache()
plaintext = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").load("s3://papers-archive/batch2-txt-cs").withColumnRenamed("_c0", "doi").withColumnRenamed("_c1", "plaintext")
new_dataframe = plaintext.join(yeardoi, plaintext.doi == yeardoi.doi)
keywords_list = ['SQL', 'RDF', 'XML', 'NoSQL', 'Machine Learning', 'Hadoop', 'Internet of Things', 'Data Science', 'Blockchain']
keywords = [key.lower() for key in keywords_list]
from pyspark.sql.functions import lower, col
import matplotlib.pyplot as plt

results ={}
for key in keywords:
    count = new_dataframe.filter(lower(col("plaintext")).like("%" + key + "%")).sort(col("year").asc()).groupby('year').count().toPandas().astype('int64')
    results[key] = count

for key in keywords:
    plt.plot(results[key]["year"],results[key]["count"], label = str(key))
    plt.legend()
plt.xlim(1985,2017)
plt.title("Number of Articles containing Keywords", fontsize=20)
plt.xlabel("Year", fontsize=18)
plt.ylabel("Number of Articles", fontsize=16)

plt.show()

'''
SQL sqlContext

SELECT tag, doi FROM plaintext JOIN tags ON LOWER(plaintext.plaintext) LIKE CONCAT('%', LOWER(tags.tag), '%')
SELECT PART_ONE.tag, DBLP.year, DBLP.authors, DBLP.title FROM DBLP INNER JOIN PART_ONE ON DBLP.doi = PART_ONE.doi)
SELECT MIN(year) AS year, tag FROM PART_TWO GROUP BY tag ORDER BY year ASC LIMIT 100
SELECT PART_TWO.tag, PART_TWO.year, authors, title FROM PART_TWO JOIN PART_THREE ON PART_TWO.year = PART_THREE.year AND PART_THREE.tag = PART_THREE.tag
'''


%pyspark
from pyspark.sql.functions import lit, col, lower

#load all files
tags = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").load("s3://bigdatacourse2019/dblp_tags.txt").withColumnRenamed("_c0", "tag").cache()
yeardoi = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").load("s3://papers-archive/dblp/yeardoi").withColumnRenamed("_c0", "year").withColumnRenamed("_c1", "doi")
plaintext = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter", ",").load("s3://papers-archive/batch2-txt-cs").withColumnRenamed("_c0", "doi").withColumnRenamed("_c1", "plaintext")
DBLP = sqlContext.read.json("s3://bigdatacourse2019/dblp-2019-02-01").cache()

#register temp tables
tags.registerTempTable("temp_tags")
yeardoi.registerTempTable("temp_years")
plaintext.registerTempTable("temp_text")
DBLP.registerTempTable("temp_DBLP")


#super query
output = sqlContext.sql("SELECT filter_on_doi.tag, filter_on_doi.year, authors, title FROM (SELECT filter_on_tag.tag, temp_DBLP.year, temp_DBLP.authors, temp_DBLP.title FROM temp_DBLP INNER JOIN (SELECT tag, doi FROM temp_text JOIN temp_tags ON LOWER(temp_text.plaintext) LIKE CONCAT('%', LOWER (temp_tags.tag), '%')) filter_on_tag ON temp_DBLP.doi = filter_on_tag.doi) filter_on_doi JOIN (SELECT MIN(year) AS year, tag FROM (SELECT filter_on_tag.tag, temp_DBLP.year, temp_DBLP.authors, temp_DBLP.title FROM temp_DBLP INNER JOIN (SELECT tag, doi FROM temp_text JOIN temp_tags ON LOWER(temp_text.plaintext) LIKE CONCAT('%', LOWER(temp_tags.tag), '%')) filter_on_tag ON temp_DBLP.doi = filter_on_tag.doi)filter_on_doi GROUP BY tag ORDER BY year ASC LIMIT 100) filter_on_year ON filter_on_doi.year = filter_on_year.year AND filter_on_doi.tag =filter_on_year.tag")

#save query result
output.toPandas().coalesce(1).to_csv("s3a://group19bucket/Lab_03/output3/output.csv", mode="overwrite", header=True)
