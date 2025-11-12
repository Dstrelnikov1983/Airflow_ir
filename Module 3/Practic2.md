https://yandex.cloud/ru/docs/cli/quickstart#yandex-account_1
yc compute ssh --id epda1u4ici4psoi1o6mu

### **Загрузка данных в кластер**

#### **Способ 1: Через Object Storage (рекомендуется)**

bash

```bash
# На локальной машине - загружаем файл в S3
yc storage s3api put-object \
  --bucket my-dataproc-bucket \
  --key data/input.txt \
  --body ./local_file.txt

# На мастер-ноде - копируем из S3 в HDFS
hadoop fs -cp s3a://my-dataproc-bucket/data/input.txt /user/ubuntu/input.txt

# Проверяем
hdfs dfs -ls /user/ubuntu/
```


### **1. MapReduce задача**

bash

```bash
# На мастер-ноде

# Пример: WordCount
hadoop jar \
  /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar \
  wordcount \
  /user/ubuntu/input.txt \
  /user/ubuntu/output

# Просмотр результата
hdfs dfs -cat /user/ubuntu/output/part-r-00000
```



### **3. Hive запрос**

bash

```bash
# На мастер-ноде
hive

# В Hive Shell
hive> CREATE TABLE sales (
        product STRING,
        amount INT
      )
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      STORED AS TEXTFILE
      LOCATION 's3a://dagstore/input';

hive> SELECT product, SUM(amount) 
      FROM sales 
      GROUP BY product;

hive> quit;
```
