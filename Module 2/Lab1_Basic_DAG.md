# Лабораторная работа №1

## Создание базового DAG в Apache Airflow

### Цель работы

Освоить основы создания DAG (Directed Acyclic Graph) в Apache Airflow, научиться создавать простые задачи с использованием BashOperator, настраивать зависимости между задачами и работать с веб-интерфейсом Airflow.

### Задачи

1. Установить и настроить Apache Airflow в Яндекс Облаке
2. Создать простой DAG с несколькими задачами
3. Настроить зависимости между задачами
4. Запустить и протестировать DAG
5. Изучить логи выполнения в веб-интерфейсе

### Предварительные требования

- Аккаунт в Яндекс Облаке
- Базовые знания Linux и командной строки
- Базовые знания Python
- SSH-клиент для подключения к виртуальной машине

---

## Часть 1. Подготовка окружения в Яндекс Облаке

### Шаг 1. Создание виртуальной машины

1. Войдите в консоль Яндекс Облака (https://console.cloud.yandex.ru)
2. Перейдите в раздел "Compute Cloud" → "Виртуальные машины"
3. Нажмите кнопку "Создать ВМ"
4. Укажите параметры:
   - **Имя:** airflow-lab-vm
   - **Платформа:** Intel Ice Lake
   - **vCPU:** 2
   - **RAM:** 4 ГБ
   - **Загрузочный диск:** Ubuntu 22.04 LTS, 20 ГБ
5. В разделе "Доступ" создайте или выберите существующую SSH-пару ключей
6. Нажмите "Создать ВМ" и дождитесь запуска

### Шаг 2. Подключение к виртуальной машине

7. Скопируйте публичный IP-адрес созданной ВМ из консоли
8. Подключитесь к ВМ через SSH:

```bash
ssh ubuntu@<PUBLIC_IP>
```

9. Если подключение успешно, вы увидите приглашение командной строки Ubuntu

### Шаг 3. Установка необходимых пакетов

10. Обновите список пакетов:

```bash
sudo apt update && sudo apt upgrade -y
```

11. Установите Python 3.10 и pip:

```bash
sudo apt install python3.10 python3-pip python3.10-venv -y
```

12. Проверьте версию Python:

```bash
python3 --version
```

> **Ожидаемый результат:** Python 3.10.x или выше

---

## Часть 2. Установка и настройка Apache Airflow

### Шаг 4. Создание виртуального окружения

13. Создайте директорию для Airflow:

```bash
mkdir ~/airflow
cd ~/airflow
```

14. Создайте виртуальное окружение Python:

```bash
python3 -m venv airflow_venv
```

15. Активируйте виртуальное окружение:

```bash
source airflow_venv/bin/activate
```

> После активации в начале строки терминала появится `(airflow_venv)`

### Шаг 5. Установка Apache Airflow

16. Установите Airflow версии 2.8.0:

```bash
AIRFLOW_VERSION=2.8.0
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

> Установка может занять 5-10 минут

17. Проверьте установку:

```bash
airflow version
```

> **Ожидаемый результат:** 2.8.0

### Шаг 6. Инициализация базы данных

18. Установите переменную окружения AIRFLOW_HOME:

```bash
export AIRFLOW_HOME=~/airflow
```

19. Инициализируйте базу данных Airflow:

```bash
airflow db init
```

> Эта команда создаст директорию airflow с конфигурационными файлами и базой данных SQLite

20. Проверьте созданную структуру:

```bash
ls ~/airflow
```

> Вы должны увидеть: `airflow.cfg`, `airflow.db`, `logs`, `dags`

### Шаг 7. Создание пользователя-администратора

21. Создайте пользователя для доступа к веб-интерфейсу:

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

22. Введите пароль (дважды), когда система попросит

> **Рекомендуемый пароль:** admin123 (только для обучения!)

23. После успешного создания вы увидите сообщение "User admin created"

### Шаг 8. Запуск Airflow

24. Откройте новый терминал и подключитесь к ВМ по SSH

25. В первом терминале запустите веб-сервер:

```bash
cd ~/airflow
source airflow_venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow webserver --port 8080
```

26. Во втором терминале запустите планировщик:

```bash
cd ~/airflow
source airflow_venv/bin/activate
export AIRFLOW_HOME=~/airflow
airflow scheduler
```

27. Дождитесь сообщения о запуске обоих компонентов

### Шаг 9. Доступ к веб-интерфейсу

28. Откройте браузер и перейдите по адресу:

```
http://<PUBLIC_IP>:8080
```

> Замените `<PUBLIC_IP>` на публичный IP-адрес вашей ВМ

29. Войдите используя созданные учетные данные:
    - **Username:** admin
    - **Password:** admin123

30. После входа вы увидите главную страницу Airflow с списком DAG

---

## Часть 3. Создание первого DAG

### Шаг 10. Создание файла DAG

31. Откройте третий терминал и подключитесь к ВМ

32. Перейдите в директорию для DAG:

```bash
cd ~/airflow/dags
```

33. Создайте новый файл:

```bash
nano my_first_dag.py
```

34. Вставьте следующий код:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Параметры по умолчанию
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Определение DAG
dag = DAG(
    'my_first_dag',
    default_args=default_args,
    description='Мой первый DAG',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['tutorial'],
)

# Задача 1: Вывод даты
print_date = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Задача 2: Создание файла
create_file = BashOperator(
    task_id='create_file',
    bash_command='echo "Hello from Airflow!" > /tmp/airflow_test.txt',
    dag=dag,
)

# Задача 3: Чтение файла
read_file = BashOperator(
    task_id='read_file',
    bash_command='cat /tmp/airflow_test.txt',
    dag=dag,
)

# Задача 4: Удаление файла
delete_file = BashOperator(
    task_id='delete_file',
    bash_command='rm /tmp/airflow_test.txt',
    dag=dag,
)

# Определение зависимостей
print_date >> create_file >> read_file >> delete_file
```

35. Сохраните файл (Ctrl+O, Enter) и выйдите (Ctrl+X)

### Шаг 11. Проверка синтаксиса DAG

36. Активируйте виртуальное окружение:

```bash
source ~/airflow/airflow_venv/bin/activate
export AIRFLOW_HOME=~/airflow
```

37. Проверьте список всех DAG:

```bash
airflow dags list
```

> В списке должен появиться `my_first_dag`

38. Проверьте структуру конкретного DAG:

```bash
airflow dags show my_first_dag
```

39. Проверьте список задач в DAG:

```bash
airflow tasks list my_first_dag
```

> Вы должны увидеть: `print_date`, `create_file`, `read_file`, `delete_file`

---

## Часть 4. Запуск и мониторинг DAG

### Шаг 12. Запуск DAG через веб-интерфейс

40. Обновите страницу в веб-интерфейсе Airflow
41. Найдите DAG `my_first_dag` в списке
42. Включите DAG, переключив тумблер в положение "ON"
43. Нажмите на кнопку "▶" (Play) справа для ручного запуска
44. Выберите "Trigger DAG" в появившемся меню
45. Дождитесь выполнения всех задач

### Шаг 13. Изучение представлений DAG

46. Кликните на имя DAG для открытия детального представления

47. Изучите вкладку "Graph" - визуализация зависимостей задач:
    - **Зеленый цвет** - задача выполнена успешно
    - **Красный** - задача завершилась с ошибкой
    - **Желтый** - задача выполняется

48. Перейдите на вкладку "Tree" - иерархическое представление запусков

49. Изучите вкладку "Gantt" - временная диаграмма выполнения задач

### Шаг 14. Просмотр логов

50. На вкладке Graph кликните на задачу `print_date`
51. Выберите "Log" в появившемся меню
52. Изучите вывод команды `date`
53. Повторите для остальных задач:
    - `create_file` - проверьте создание файла
    - `read_file` - убедитесь, что видите "Hello from Airflow!"
    - `delete_file` - проверьте успешное удаление

---

## Контрольные вопросы

1. Что такое DAG и почему он должен быть ациклическим?
2. Для чего нужны параметры `default_args` в DAG?
3. Что означает параметр `catchup=False`?
4. Как определяются зависимости между задачами?
5. Где хранятся логи выполнения задач?

---

## Дополнительные задания

### Задание 1. Модификация расписания

Измените параметр `schedule_interval` на `"@hourly"` и протестируйте изменения.

### Задание 2. Добавление параллельных задач

Добавьте две задачи, которые выполняются параллельно после `print_date`:

- `task_a`: выводит hostname системы
- `task_b`: выводит текущего пользователя

Обе задачи должны завершиться перед выполнением `create_file`.

### Задание 3. Работа с ошибками

Создайте задачу, которая завершается с ошибкой (например, несуществующая команда). Изучите, как Airflow обрабатывает ошибки и выполняет retry.

---

## Результат работы

По завершении лабораторной работы вы должны:

- Иметь работающую установку Apache Airflow в Яндекс Облаке
- Создать и успешно запустить базовый DAG
- Понимать структуру файла DAG
- Уметь работать с веб-интерфейсом Airflow
- Уметь просматривать логи выполнения задач

---

## Полезные ссылки

- [Официальная документация Apache Airflow](https://airflow.apache.org/docs/)
- [Документация Яндекс Облака](https://cloud.yandex.ru/docs)
- [BashOperator Reference](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/bash.html)
