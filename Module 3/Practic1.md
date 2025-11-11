# ПРАКТИЧЕСКАЯ

РАБОТА №1

## Работа с Hadoop

Distributed File System (HDFS)

Модуль
3: Apache Hadoop

**Длительность: ** 25 минут

**Формат: ** Индивидуальная
работа

**Платформа: ** Яндекс Облако
(Managed Service for Hadoop)

## Цели практической работы

По завершении данной работы вы
сможете:

Выполнять базовые операции с HDFS через командную
строку

Создавать директории и управлять файлами в
распределённой системе

Понимать механизм репликации данных в HDFS

Проверять целостность данных и статус блоков

Загружать и выгружать данные из HDFS

## Требования к окружению

●      ✓
Активный кластер Hadoop в Яндекс Облаке

●      ✓
SSH-клиент (PuTTY для Windows, терминал для MacOS/Linux)

●      ✓
SSH-ключ для доступа к кластеру

●
✓ Базовые знания работы с командной строкой Linux

# ЧАСТЬ 1: ПОДКЛЮЧЕНИЕ И ПРОВЕРКА КЛАСТЕРА

### Шаг 1.1: Получение учетных данных

**Действия:**

Откройте веб-консоль Яндекс Облака по адресу:
https://console.cloud.yandex.ru

Перейдите в раздел "Data Platform" →
"Managed Service for Hadoop"

Выберите кластер с именем
"hadoop-training-cluster"

На вкладке "Hosts" найдите master-узел (роль:
MASTERNODE)

Скопируйте публичный IP-адрес master-узла

**💡
Примечание: ** *IP-адрес будет иметь формат: 51.250.XXX.XXX*

### Шаг 1.2: Подключение по SSH

**Для Linux/MacOS:**

Откройте терминал и выполните
команду:

ssh -i ~/.ssh/hadoop_training_key
ubuntu@<MASTER_NODE_IP>

**Для Windows (PowerShell):**

ssh -i
C:\Users\YourName\.ssh\hadoop_training_key ubuntu@<MASTER_NODE_IP>

**⚠️
Важно: ** Замените <MASTER_NODE_IP> на реальный IP-адрес вашего
кластера!

### Шаг 1.3: Проверка статуса HDFS

После успешного подключения
выполните команду для проверки статуса HDFS:

hdfs dfsadmin -report

**Ожидаемый результат:**

●      Информация
о состоянии кластера

●      Количество
живых DataNode (обычно 3-5)

●      Общий
объем доступного пространства

●
Процент использованного пространства

***Пример вывода:***

Configured Capacity: 322122547200
(300 GB) Present Capacity: 305419018280 (284.42 GB)   DFS Remaining: 301573668864 (280.84 GB) DFS
Used: 3845349416 (3.58 GB) Live datanodes (3):

# ЧАСТЬ 2: БАЗОВЫЕ ОПЕРАЦИИ С HDFS

### Шаг 2.1: Создание структуры директорий

Создайте персональную рабочую
директорию в HDFS:

hdfs dfs -mkdir -p /user/$USER/practice1

hdfs dfs -mkdir /user/$USER/practice1/input

hdfs dfs -mkdir
/user/$USER/practice1/output

Проверьте созданную структуру:

hdfs dfs -ls /user/$USER/practice1

**💡
Пояснение команд:**

●      -mkdir
создает директорию в HDFS

●      -p
создает родительские директории при необходимости

●
$USER автоматически подставляет ваше имя пользователя

### Шаг 2.2: Подготовка тестовых данных

Создайте тестовый файл на
локальной файловой системе:

cat > test_data.txt <<
'EOF' Apache Hadoop is a framework for distributed storage and processing. HDFS
provides reliable storage for large datasets. MapReduce enables parallel
processing of big data. Hadoop ecosystem includes Hive, Pig, HBase, and Spark.
Data replication ensures fault tolerance in Hadoop. EOF

Создайте второй файл с большим
объемом данных:

for i in {1..1000}; do   echo "Line $i: Processing large dataset
with Hadoop HDFS" >> large_data.txt done

### Шаг 2.3: Загрузка файлов в HDFS

Загрузите файлы в HDFS:

hdfs dfs -put test_data.txt /user/$USER/practice1/input/

hdfs dfs -put large_data.txt
/user/$USER/practice1/input/

Проверьте загруженные файлы:

hdfs dfs -ls
/user/$USER/practice1/input/

Посмотрите содержимое файла:

hdfs dfs -cat
/user/$USER/practice1/input/test_data.txt

# ЧАСТЬ 3: РАБОТА С БЛОКАМИ И РЕПЛИКАЦИЕЙ

### Шаг 3.1: Проверка информации о блоках

Получите детальную информацию о
файле и его блоках:

hdfs fsck
/user/$USER/practice1/input/test_data.txt -files -blocks -locations

**Анализ вывода:**

●      Количество
блоков файла

●      Размер
каждого блока

●      Фактор
репликации (обычно 3)

●
Расположение реплик на DataNodes

### Шаг 3.2: Изменение фактора репликации

Измените фактор репликации для
файла:

hdfs dfs -setrep 2
/user/$USER/practice1/input/test_data.txt

Проверьте изменения:

hdfs fsck
/user/$USER/practice1/input/test_data.txt -files -blocks

**⚙️
Задание для самопроверки:**

●      Попробуйте
установить фактор репликации 1 и 4

●      Наблюдайте
за изменениями в выводе fsck

●
Подумайте: какие риски несет снижение фактора
репликации?

# ЧАСТЬ 4: ДОПОЛНИТЕЛЬНЫЕ ОПЕРАЦИИ

### Шаг 4.1: Копирование и перемещение файлов

Скопируйте файл внутри HDFS:

hdfs dfs -cp
/user/$USER/practice1/input/test_data.txt
/user/$USER/practice1/input/test_data_copy.txt

Переместите файл:

hdfs dfs -mv
/user/$USER/practice1/input/test_data_copy.txt /user/$USER/practice1/output/

### Шаг 4.2: Получение файлов из HDFS

Скачайте файл из HDFS на
локальную систему:

hdfs dfs -get
/user/$USER/practice1/output/test_data_copy.txt ./downloaded_data.txt

Проверьте содержимое:

cat downloaded_data.txt

### Шаг 4.3: Удаление файлов и директорий

Удалите отдельный файл:

hdfs dfs -rm
/user/$USER/practice1/output/test_data_copy.txt

Удалите директорию со всем
содержимым:

hdfs dfs -rm -r
/user/$USER/practice1/output

# ЧАСТЬ 5: МОНИТОРИНГ И ПРОВЕРКА

### Шаг 5.1: Проверка использования пространства

Посмотрите размер директории:

hdfs dfs -du -h
/user/$USER/practice1

Посмотрите общую статистику
файловой системы:

hdfs dfs -df -h

# КОНТРОЛЬНЫЕ ВОПРОСЫ

Ответьте на следующие вопросы
для закрепления материала:

Какой фактор репликации по умолчанию используется в
HDFS и почему?

В чем разница между командами put и copyFromLocal?

Что произойдет с данными, если откажет один из
DataNode?

Почему в HDFS используются большие размеры блоков
(128MB)?

Как HDFS обеспечивает отказоустойчивость?

# ДОПОЛНИТЕЛЬНЫЕ ЗАДАНИЯ (опционально)

**Задание 1: ** Создайте
скрипт для автоматической загрузки файлов в HDFS

**Задание 2: ** Изучите
команды для работы с правами доступа в HDFS (chmod, chown)

**Задание 3: ** Настройте
автоматическое удаление старых файлов с помощью retention policy

# ЗАКЛЮЧЕНИЕ

Поздравляем! Вы успешно освоили
базовые операции с HDFS:

●      ✓
Подключение к кластеру Hadoop

●      ✓
Создание и управление директориями

●      ✓
Загрузка и выгрузка файлов

●      ✓
Работа с репликацией и блоками

●
✓ Мониторинг файловой системы

**📌 Следующий шаг: ** Переходите
к Лабораторной работе №1 для изучения MapReduce
