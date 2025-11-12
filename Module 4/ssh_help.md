bash

cat > /mnt/user-data/outputs/Quick_Start_SSH_Keys.md << 'QUICKSTART'
# 🚀 Быстрый старт: SSH-ключи для Airflow + Data Proc

## ⚡ За 5 минут

### 1️⃣ Генерация SSH-ключа

```bash
# Создайте новый SSH-ключ
ssh-keygen -t rsa -b 4096 -f ~/.ssh/yc-dataproc-key -N ""

# Конвертируйте в PEM формат
ssh-keygen -p -m PEM -f ~/.ssh/yc-dataproc-key -N "" -P ""

# Проверьте результат
head -1 ~/.ssh/yc-dataproc-key
# Должно быть: -----BEGIN RSA PRIVATE KEY-----
```

### 2️⃣ Создание Data Proc кластера

```bash
# Получите публичный ключ
cat ~/.ssh/yc-dataproc-key.pub

# Создайте кластер с этим ключом через UI:
# Data Proc → Создать → SSH ключ → [вставьте публичный ключ]
```

### 3️⃣ Загрузка ключа в Object Storage

```bash
# Создайте бакет
yc storage bucket create --name airflow-secrets

# Загрузите приватный ключ
aws s3 cp ~/.ssh/yc-dataproc-key \
  s3://airflow-secrets/ssh-keys/dataproc.pem \
  --endpoint-url=https://storage.yandexcloud.net
```

### 4️⃣ Монтирование в Airflow

**Через консоль:**
1. Airflow → Ваш кластер → Настройки
2. S3 buckets → Добавить: `airflow-secrets`
3. Путь монтирования: `/secrets`

### 5️⃣ Создание Connection

**В Airflow UI (Admin → Connections):**

```
Connection Id: dataproc_ssh
Connection Type: SSH
Host: <IP мастер-ноды Data Proc>
Username: ubuntu
Port: 22
Extra: {"key_file": "/secrets/ssh-keys/dataproc.pem", "no_host_key_check": true}
```

### 6️⃣ Тест подключения

```python
from airflow.providers.ssh.operators.ssh import SSHOperator

test = SSHOperator(
    task_id='test',
    ssh_conn_id='dataproc_ssh',
    command='hostname'
)
```

---

## 📋 Чек-лист

- [ ] Ключ сгенерирован и в PEM формате
- [ ] Data Proc создан с публичным ключом
- [ ] Приватный ключ в Object Storage
- [ ] Бакет примонтирован к Airflow  
- [ ] Connection создан и протестирован

---

## ❓ Частые ошибки

### "Permission denied (publickey)"
```bash
# Проверьте формат ключа
head -1 ~/.ssh/yc-dataproc-key
# Должно быть: -----BEGIN RSA PRIVATE KEY-----

# Если нет - конвертируйте:
ssh-keygen -p -m PEM -f ~/.ssh/yc-dataproc-key
```

### "No such file"
```bash
# Проверьте путь в Airflow:
# Должно быть: /secrets/ssh-keys/dataproc.pem
# где /secrets - точка монтирования бакета
```

### "Connection timeout"
```bash
# Проверьте доступность мастер-ноды:
telnet <dataproc-master-ip> 22

# Убедитесь, что Airflow и Data Proc в одной VPC
```

---

## 🎯 Готовый пример DAG

```python
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG('test_ssh', start_date=datetime(2024, 1, 1), 
         schedule_interval=None) as dag:
    
    SSHOperator(
        task_id='check_hdfs',
        ssh_conn_id='dataproc_ssh',
        command='hdfs dfs -ls /'
    )
```

---

**Полная документация:** SSH_Keys_Setup_Guide_YandexCloud.md
QUICKSTART
echo "✅ Быстрая шпаргалка создана"