# Лабораторная работа №1 (Managed Service)

## Работа с Yandex Managed Service for Apache Airflow™

### Цель работы

Освоить работу с управляемым сервисом Apache Airflow от Яндекс Облака, научиться создавать кластеры, загружать DAG файлы через Object Storage, настраивать подключения и работать с веб-интерфейсом Airflow в облачной среде.

### Задачи

1. Создать кластер Managed Service for Apache Airflow
2. Настроить Object Storage для хранения DAG
3. Загрузить и запустить первый DAG
4. Настроить подключения к внешним сервисам
5. Изучить возможности мониторинга

### Предварительные требования

- Аккаунт в Яндекс Облаке с активным биллингом
- Роли в облаке: `editor` или `admin`
- Базовые знания Python
- Установленный Yandex Cloud CLI (опционально)

> **✅ Преимущества Managed Service:** автоматическое обновление, масштабирование, резервное копирование, мониторинг и техническая поддержка от Яндекс Облака. Не требуется администрирование инфраструктуры!

---

## Часть 1. Создание кластера Airflow

### Шаг 1. Подготовка сети

1. Войдите в консоль Яндекс Облака (https://console.cloud.yandex.ru)
2. Перейдите в раздел **Virtual Private Cloud**
3. Создайте новую сеть или используйте существующую
4. Убедитесь, что есть подсети в зонах доступности (например, `ru-central1-a`)
5. Запишите ID сети - он понадобится при создании кластера

### Шаг 2. Создание сервисного аккаунта

6. Перейдите в раздел **Service Accounts**
7. Нажмите **Create service account**
8. Укажите имя: `airflow-sa`
9. Добавьте роли:
   - `storage.editor` (для доступа к Object Storage)
   - `managed-airflow.integrationProvider` (для работы с Airflow)
10. Нажмите **Create**

### Шаг 3. Создание Object Storage бакета

11. Перейдите в раздел **Object Storage**
12. Нажмите **Create bucket**
13. Укажите уникальное имя бакета: `airflow-dags-<ваше_имя>`
14. Выберите **Storage class:** Standard
15. **Access:** Ограниченный (Limited access)
16. Нажмите **Create bucket**
17. Внутри бакета создайте папку `dags/`

### Шаг 4. Создание кластера Airflow

18. В меню выберите **Managed Service for Apache Airflow**
19. Нажмите **Create cluster**
20. Укажите параметры кластера:
    - **Имя:** `airflow-cluster-lab`
    - **Версия Apache Airflow:** 2.8 (последняя доступная)
    - **Зона доступности:** `ru-central1-a`
    - **Сеть:** выбрать созданную ранее
    - **Подсеть:** выбрать в зоне `ru-central1-a`
    - **Сервисный аккаунт:** `airflow-sa`
    - **Логин администратора:** `admin`
    - **Пароль:** (придумайте надежный пароль)
21. В разделе **DAG files** источник выберите **Object Storage**
22. Укажите путь к бакету: `airflow-dags-<ваше_имя>/dags/`
23. Выберите конфигурацию:
    - **Scheduler:** s2.micro (2 vCPU, 8 GB RAM)
    - **Web server:** s2.micro (2 vCPU, 8 GB RAM)
    - **Workers:** s2.micro, автомасштабирование 1-3
24. Нажмите **Create cluster**
25. Дождитесь создания кластера (10-15 минут)

> **ℹ️ Статус кластера** изменится с `CREATING` на `RUNNING`, когда кластер будет готов к работе.

---

## Часть 2. Создание и загрузка DAG

### Шаг 5. Создание первого DAG файла

26. Создайте локально файл `my_first_managed_dag.py`
27. Добавьте следующий код:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_cloud_info():
    """Функция для вывода информации о среде выполнения"""
    print("Running on Yandex Cloud!")
    print("Managed Airflow Service")
    return "Success"


# Параметры по умолчанию для всех задач DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Определение DAG
dag = DAG(
    'my_first_managed_dag',
    default_args=default_args,
    description='First DAG in Managed Airflow',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['managed', 'tutorial'],
)

# Задача 1: Вывод текущей даты
task1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Задача 2: Вывод информации о среде
task2 = PythonOperator(
    task_id='print_cloud_info',
    python_callable=print_cloud_info,
    dag=dag,
)

# Задача 3: Список файлов в /tmp
task3 = BashOperator(
    task_id='list_files',
    bash_command='ls -la /tmp',
    dag=dag,
)

# Определение зависимостей между задачами
task1 >> task2 >> task3
```

28. Сохраните файл

### Шаг 6. Загрузка DAG в Object Storage

Есть два способа загрузки:

#### Способ 1: Через веб-интерфейс

29. Откройте консоль **Object Storage**
30. Перейдите в бакет `airflow-dags-<ваше_имя>`
31. Откройте папку `dags/`
32. Нажмите **Upload** и выберите файл `my_first_managed_dag.py`
33. Дождитесь завершения загрузки

#### Способ 2: Через Yandex Cloud CLI

```bash
yc storage s3api put-object \
  --bucket airflow-dags-<ваше_имя> \
  --key dags/my_first_managed_dag.py \
  --body my_first_managed_dag.py
```

> **ℹ️ Автоматическая синхронизация:** Airflow автоматически сканирует бакет каждые 5 минут и загружает новые DAG файлы.

---

## Часть 3. Работа с веб-интерфейсом

### Шаг 7. Доступ к веб-интерфейсу Airflow

34. В консоли **Managed Service for Apache Airflow** откройте кластер
35. Найдите раздел **Access** и скопируйте URL веб-интерфейса
36. Откройте URL в браузере
37. Войдите используя учетные данные:
    - **Username:** `admin`
    - **Password:** (пароль, указанный при создании кластера)
38. После входа вы увидите главную страницу Airflow

### Шаг 8. Запуск DAG

39. Подождите 5-7 минут, пока Airflow обнаружит новый DAG
40. Найдите DAG `my_first_managed_dag` в списке
41. Включите DAG, переключив тумблер
42. Нажмите кнопку **Trigger DAG** для ручного запуска
43. Кликните на имя DAG для просмотра деталей
44. Перейдите на вкладку **Graph** для визуализации
45. Дождитесь успешного выполнения всех задач (зеленый цвет)

### Шаг 9. Просмотр логов

46. На вкладке **Graph** кликните на задачу `print_cloud_info`
47. Выберите **Log** в меню
48. Проверьте вывод функции
49. Просмотрите логи остальных задач

---

## Часть 4. Настройка подключений

### Шаг 10. Создание подключения к Object Storage

50. В веб-интерфейсе Airflow откройте **Admin → Connections**
51. Нажмите **+** для создания нового подключения
52. Заполните поля:
    - **Connection Id:** `yc_s3_connection`
    - **Connection Type:** Amazon S3
    - **Extra:** `{"endpoint_url": "https://storage.yandexcloud.net"}`

53. Для доступа с ключами создайте статический ключ доступа:
    - Перейдите в **Service Accounts → airflow-sa**
    - Создайте **Static access key**
    - Скопируйте **Access Key ID** и **Secret Key**

54. Вернитесь в Airflow и добавьте в подключение:
    - **Login:** Access Key ID
    - **Password:** Secret Key

55. Нажмите **Save**

### Шаг 11. Создание DAG с использованием S3

56. Создайте новый файл `s3_example_dag.py`

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from datetime import datetime


# Определение DAG
dag = DAG(
    's3_example_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
)

# Задача: создание файла в S3
create_file = S3CreateObjectOperator(
    task_id='create_s3_file',
    s3_bucket='airflow-dags-<имя>',
    s3_key='output/test.txt',
    data='Hello from Airflow!',
    aws_conn_id='yc_s3_connection',
    dag=dag,
)
```

57. Загрузите файл в Object Storage в папку `dags/`
58. Дождитесь появления DAG и запустите его
59. Проверьте, что файл создан в бакете

---

## Часть 5. Мониторинг и управление

### Шаг 12. Мониторинг кластера

60. В консоли Яндекс Облака откройте кластер Airflow
61. Перейдите на вкладку **Monitoring**
62. Изучите доступные метрики:
    - CPU utilization компонентов
    - Использование памяти
    - Количество запущенных задач
    - Успешные и неуспешные выполнения
63. Настройте период отображения графиков

### Шаг 13. Настройка автомасштабирования

64. В настройках кластера перейдите в раздел **Workers**
65. Настройте автомасштабирование:
    - **Минимум workers:** 1
    - **Максимум workers:** 5
    - **Автоскейлинг по нагрузке**
66. Сохраните изменения

### Шаг 14. Работа с логами

67. Логи Airflow автоматически отправляются в **Yandex Cloud Logging**
68. Перейдите в раздел **Cloud Logging**
69. Выберите лог-группу кластера Airflow
70. Изучите логи компонентов:
    - Scheduler logs
    - Worker logs
    - Web server logs
71. Используйте фильтры для поиска по логам

---

## Сравнение подходов установки Airflow

### Таблица сравнения различных способов развертывания

| Критерий | Managed Service (Яндекс Облако) | Самостоятельная установка на ВМ | Windows + WSL2 |
|----------|----------------------------------|----------------------------------|----------------|
| Администрирование | ✅ Не требует | ❌ Требует | ❌ Требует (частично) |
| Обновления | ✅ Автоматические | ❌ Ручные | ❌ Ручные |
| Мониторинг | ✅ Встроенный | ❌ Нужна настройка | ❌ Отсутствует |
| Масштабирование | ✅ Автоматическое | ❌ Ручное | ❌ Ограниченное |
| Стоимость | ❌ Дороже | ✅ Дешевле для небольших проектов | ✅ Бесплатно |
| Гибкость настройки | ❌ Меньше | ✅ Полный контроль | ✅ Полный контроль |
| Применение | Продакшен | Продакшен / Разработка | Только разработка |

---

## Контрольные вопросы

1. Какие преимущества дает использование Managed Service?
2. Как Airflow узнает о новых DAG файлах в Object Storage?
3. Зачем нужен сервисный аккаунт при создании кластера?
4. Как работает автомасштабирование workers?
5. Где хранятся логи выполнения задач в Managed Service?

---

## Дополнительные задания

### Задание 1. DAG с обработкой файлов из S3

Создайте DAG, который:
- Читает CSV файл из Object Storage
- Обрабатывает данные с помощью Pandas
- Сохраняет результат обратно в S3

### Задание 2. Настройка алертов

Настройте уведомления в Yandex Monitoring при:
- Высокой загрузке CPU (>80%)
- Неуспешном выполнении DAG
- Превышении времени выполнения задачи

### Задание 3. Подключение к Yandex Managed PostgreSQL

Создайте подключение к Managed PostgreSQL и напишите DAG для:
- Выполнения SQL запроса
- Экспорта результата в S3

---

## Полезные советы

### Оптимизация затрат

- Используйте минимальную конфигурацию для тестирования
- Останавливайте кластер, когда он не используется
- Настройте автомасштабирование workers
- Используйте lifecycle policies для S3

### Безопасность

- Используйте сильные пароли для админа
- Храните секреты в Yandex Lockbox
- Ограничьте доступ к веб-интерфейсу через Security Groups
- Регулярно обновляйте версию Airflow

---

## Результат работы

По завершении лабораторной работы вы должны:

- ✅ Иметь работающий кластер Managed Airflow
- ✅ Понимать работу с Object Storage для DAG
- ✅ Уметь создавать и настраивать подключения
- ✅ Знать основы мониторинга и управления
- ✅ Понимать преимущества управляемого сервиса

---

## Полезные ссылки

- [Документация Managed Service for Apache Airflow](https://cloud.yandex.ru/docs/managed-airflow/)
- [Object Storage документация](https://cloud.yandex.ru/docs/storage/)
- [Apache Airflow провайдеры](https://airflow.apache.org/docs/apache-airflow-providers/)
- [Yandex Cloud CLI](https://cloud.yandex.ru/docs/cli/)
- [Тарифы Managed Airflow](https://cloud.yandex.ru/prices#managed-airflow)
