# ЛАБОРАТОРНАЯ

РАБОТА №1

## Разработка

MapReduce приложений

Модуль
3: Apache Hadoop

**Длительность: ** 45 минут

**Формат: ** Индивидуальная
работа

**Технологии: ** Python
(mrjob), Apache Hive

## Цели лабораторной работы

По завершении вы сможете:

Разрабатывать MapReduce приложения на Python

Обрабатывать большие объемы данных распределенным
способом

Использовать Apache Hive для SQL-запросов

Оптимизировать MapReduce задачи

## Требования

●      ✓
Выполненная практическая работа №1

●      ✓
Python 3.x установлен на кластере

●      ✓
Библиотека mrjob

●
✓ Доступ к Hive

# ЗАДАЧА 1: WORDCOUNT - КЛАССИЧЕСКАЯ ЗАДАЧА

### Описание задачи

Создайте MapReduce программу
для подсчета частоты слов в текстовых документах.

### Шаг 1.1: Подготовка данных

Создайте тестовый файл с
текстом:

cat > books.txt << 'EOF' The quick brown fox jumps over the
lazy dog The dog was really very lazy The fox was quick and brown Hadoop
processes big data efficiently MapReduce is a programming model for big data
EOF

Загрузите файл в HDFS:

hdfs dfs -put books.txt /user/$USER/lab1/input/

### Шаг 1.2: Установка mrjob

Установите библиотеку mrjob:

pip3 install mrjob --user

### Шаг 1.3: Создание MapReduce программы

Создайте файл wordcount.py:

from mrjob.job import MRJob import
re  class MRWordCount(MRJob):          def mapper(self, _, line):         # Разбиваем строку на слова         words = re.findall(r'\w+',
line.lower())         # Emit each word
with count 1         for word in words:             yield word, 1          def reducer(self, word, counts):         # Суммируем количество для каждого
слова         yield word,
sum(counts)  if __name__ ==
'__main__':     MRWordCount.run()

### Шаг 1.4: Запуск в локальном режиме (тестирование)

python3 wordcount.py books.txt

***Ожидаемый результат:***

"big" 2 "brown"   2 "data"    2 "dog"     2
"the"     4 ...

### Шаг 1.5: Запуск на Hadoop

Запустите задачу на кластере
Hadoop:

python3 wordcount.py -r hadoop hdfs:///user/$USER/lab1/input/books.txt
--output-dir hdfs:///user/$USER/lab1/output/wordcount

Проверьте результаты:

hdfs dfs -cat /user/$USER/lab1/output/wordcount/part-*

# ЗАДАЧА 2: АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА

### Описание задачи

Проанализируйте логи
веб-сервера: подсчитайте количество запросов от каждого IP-адреса.

### Шаг 2.1: Создание тестовых логов

cat > access.log << 'EOF' 192.168.1.10 - -
[01/Jan/2024:10:00:00] "GET /index.html HTTP/1.1" 200 1024
192.168.1.11 - - [01/Jan/2024:10:01:00] "GET /about.html HTTP/1.1"
200 2048 192.168.1.10 - - [01/Jan/2024:10:02:00] "POST /api/data
HTTP/1.1" 201 512 192.168.1.12 - - [01/Jan/2024:10:03:00] "GET
/products.html HTTP/1.1" 200 3072 192.168.1.10 - - [01/Jan/2024:10:04:00]
"GET /contact.html HTTP/1.1" 200 1536 192.168.1.11 - -
[01/Jan/2024:10:05:00] "GET /index.html HTTP/1.1" 200 1024
192.168.1.13 - - [01/Jan/2024:10:06:00] "GET /services.html HTTP/1.1"
200 2560 EOF

Загрузите в HDFS:

hdfs dfs -put access.log /user/$USER/lab1/input/

### Шаг 2.2: Создание программы анализа логов

Создайте файл log_analyzer.py:

from mrjob.job import MRJob import
re  class MRLogAnalyzer(MRJob):          def mapper(self, _, line):         # Извлекаем IP-адрес из строки
лога         ip_match =
re.match(r'^(\d+\.\d+\.\d+\.\d+)', line)
if ip_match:
ip_address = ip_match.group(1)             yield ip_address, 1          def reducer(self, ip_address,
counts):         # Подсчитываем
количество запросов от каждого IP
yield ip_address, sum(counts)  if
__name__ == '__main__':
MRLogAnalyzer.run()

### Шаг 2.3: Запуск анализа

python3 log_analyzer.py -r hadoop
hdfs:///user/$USER/lab1/input/access.log --output-dir
hdfs:///user/$USER/lab1/output/logs

**🎯
Ожидаемый результат:**

"192.168.1.10"    3 "192.168.1.11"  2 "192.168.1.12"  1 "192.168.1.13"    1

# ЗАДАЧА 3: РАСШИРЕННЫЙ АНАЛИЗ С COMBINER

### Описание задачи

Оптимизируйте задачу подсчета
слов с использованием combiner для уменьшения объема данных, передаваемых между
Map и Reduce фазами.

### Шаг 3.1: Создание программы с Combiner

Создайте файл
wordcount_optimized.py:

from mrjob.job import MRJob import re
class MRWordCountOptimized(MRJob):          def mapper(self, _, line):         words = re.findall(r'\w+',
line.lower())         for word in
words:             yield word, 1          def combiner(self, word, counts):         # Combiner суммирует на
уровне mapper         # Это уменьшает объем данных для
shuffle         yield word,
sum(counts)          def reducer(self,
word, counts):         yield word,
sum(counts)  if __name__ ==
'__main__':
MRWordCountOptimized.run()

**💡
Пояснение:**

●      Combiner
работает после mapper, но до shuffle фазы

●      Он
уменьшает объем данных, передаваемых по сети

●
Особенно эффективен при большом количестве
повторяющихся ключей

# ЗАДАЧА 4: РАБОТА С APACHE HIVE

### Описание задачи

Используйте Apache Hive для
выполнения SQL-запросов к данным о продажах.

### Шаг 4.1: Подготовка данных о продажах

cat > sales_data.csv << 'EOF'
id,product,category,amount,sale_date 1,Laptop,Electronics,1200.50,2024-01-15
2,Phone,Electronics,899.99,2024-01-16 3,Desk,Furniture,450.00,2024-01-17
4,Chair,Furniture,199.99,2024-01-18 5,Monitor,Electronics,350.00,2024-01-19
6,Keyboard,Electronics,89.99,2024-01-20 7,Table,Furniture,599.00,2024-01-21
8,Mouse,Electronics,29.99,2024-01-22 9,Bookshelf,Furniture,299.00,2024-01-23
10,Webcam,Electronics,129.99,2024-01-24 EOF

Загрузите в HDFS:

hdfs dfs -put sales_data.csv /user/$USER/lab1/input/

### Шаг 4.2: Запуск Hive

Подключитесь к Hive CLI:

hive

### Шаг 4.3: Создание таблицы в Hive

CREATE TABLE sales (     id
INT,     product STRING,     category STRING,     amount DECIMAL(10,2),     sale_date DATE ) ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' STORED AS TEXTFILE TBLPROPERTIES
("skip.header.line.count"="1");

### Шаг 4.4: Загрузка данных

LOAD DATA INPATH '/user/$USER/lab1/input/sales_data.csv' INTO TABLE
sales;

### Шаг 4.5: Выполнение аналитических запросов

**Запрос 1: ** Общая сумма
продаж по категориям

SELECT      category,      SUM(amount) as total_sales,     COUNT(*) as num_items FROM sales GROUP BY
category ORDER BY total_sales DESC;

**Запрос**** 2: ** Топ-5 самых дорогих товаров

SELECT      product,     category,     amount FROM sales ORDER BY amount DESC
LIMIT 5;

**Запрос**** 3: ** Средняя стоимость по категориям

SELECT      category,     AVG(amount) as avg_price,     MIN(amount) as min_price,     MAX(amount) as max_price FROM sales GROUP
BY category;

# ДОПОЛНИТЕЛЬНОЕ ЗАДАНИЕ

**⭐
Задание повышенной сложности:**

Создайте MapReduce программу для расчета среднего чека
по дням недели

Реализуйте multi-step MapReduce задачу (chain jobs)

Создайте партиционированную таблицу в Hive и загрузите
в нее данные

Оптимизируйте Hive запросы с использованием индексов

# КОНТРОЛЬНЫЕ ВОПРОСЫ

В чем разница между mapper, combiner и reducer?

Когда использование combiner наиболее эффективно?

Как Hive преобразует SQL-запросы в MapReduce задачи?

Какие оптимизации можно применить к MapReduce задачам?

В каких случаях лучше использовать Hive, а в каких -
чистый MapReduce?

# ЗАКЛЮЧЕНИЕ

Отличная работа! Вы освоили:

●      ✓
Разработку MapReduce приложений на Python

●      ✓
Оптимизацию с использованием Combiner

●      ✓
Анализ реальных данных (логи, продажи)

●
✓ Работу с Apache Hive и HiveQL

**🎓
Полезные ресурсы:**

●      mrjob documentation:
https://mrjob.readthedocs.io/

●      Apache Hive documentation:
https://hive.apache.org/

●      Hadoop MapReduce Tutorial:
https://hadoop.apache.org/docs/stable/
