---

## 📍 Детальный разбор: Где что выполняется

### **Пример: WordCount на 4-узловом кластере**

**Входные данные распределены:**

```
Node 1: file1.txt → "Hello World Hello"
Node 2: file2.txt → "World Hadoop Hello"
Node 3: file3.txt → "Hadoop is great"
Node 4: file4.txt → "Hello MapReduce"
```

---

### **ШАГ 1: MAP PHASE (на узлах с данными)**

**Node 1 (Mapper 1):**

```
Читает: "Hello World Hello"
Выдаёт:
  (Hello, 1)
  (World, 1)
  (Hello, 1)
```

**Node 2 (Mapper 2):**

```
Читает: "World Hadoop Hello"
Выдаёт:
  (World, 1)
  (Hadoop, 1)
  (Hello, 1)
```

**Node 3 (Mapper 3):**

```
Читает: "Hadoop is great"
Выдаёт:
  (Hadoop, 1)
  (is, 1)
  (great, 1)
```

**Node 4 (Mapper 4):**

```
Читает: "Hello MapReduce"
Выдаёт:
  (Hello, 1)
  (MapReduce, 1)
```

---

### **ШАГ 2: SHUFFLE & SORT PHASE**

**Hadoop группирует по ключам и отправляет на Reducer'ы:**

Допустим, у нас **2 Reducer'а** :

python

```python
# Партиционирование
hash("Hello")     % 2 = 0  → Reducer на Node 1
hash("World")     % 2 = 1  → Reducer на Node 2
hash("Hadoop")    % 2 = 0  → Reducer на Node 1
hash("is")        % 2 = 1  → Reducer на Node 2
hash("great")     % 2 = 1  → Reducer на Node 2
hash("MapReduce") % 2 = 0  → Reducer на Node 1
```

**Данные передаются по сети:**

```
ОТ всех Mapper'ов  →  К Reducer 0 (Node 1):
                       Hello:     [1, 1, 1, 1] ← с 4-х узлов
                       Hadoop:    [1, 1]       ← с 2-х узлов
                       MapReduce: [1]          ← с 1-го узла

ОТ всех Mapper'ов  →  К Reducer 1 (Node 2):
                       World: [1, 1]  ← с 2-х узлов
                       is:    [1]     ← с 1-го узла
                       great: [1]     ← с 1-го узла
```

**🔥 Важно:** Данные **передаются по сети** от Mapper'ов к Reducer'ам. Это называется **Shuffle** и это самая **медленная** часть MapReduce!

---

### **ШАГ 3: REDUCE PHASE**

**Node 1 (Reducer 0) обрабатывает:**

python

```python
def reducer(key, values):
    return (key, sum(values))

# Выполняет:
("Hello", [1,1,1,1])     → ("Hello", 4)
("Hadoop", [1,1])        → ("Hadoop", 2)
("MapReduce", [1])       → ("MapReduce", 1)
```

**Node 2 (Reducer 1) обрабатывает:**

python

```python
# Выполняет:
("World", [1,1])  → ("World", 2)
("is", [1])       → ("is", 1)
("great", [1])    → ("great", 1)
```

---

## 🎯 Важные правила распределения

### **Правило 1: Один ключ = один Reducer**

```
✅ ПРАВИЛЬНО:
  Reducer 0: обрабатывает "Hello" (все значения)
  Reducer 1: обрабатывает "World" (все значения)

❌ НЕПРАВИЛЬНО:
  Reducer 0: обрабатывает часть "Hello"
  Reducer 1: обрабатывает другую часть "Hello"
```

**Все значения для одного ключа ВСЕГДА попадают на ОДИН Reducer!**

Это гарантирует корректность агрегации.
