import csv
import random
from datetime import datetime, timedelta
import os

# Данные для генерации
categories = ["Электроника", "Одежда", "Продукты", "Книги", "Спорт"]
products = {
    "Электроника": ["Телефон", "Ноутбук", "Наушники", "Планшет"],
    "Одежда": ["Футболка", "Джинсы", "Кроссовки", "Куртка"],
    "Продукты": ["Хлеб", "Молоко", "Яйца", "Фрукты"],
    "Книги": ["Роман", "Учебник", "Комикс", "Энциклопедия"],
    "Спорт": ["Мяч", "Гантели", "Коврик", "Скакалка"]
}

# Генерация данных
transactions = []
base_date = datetime.now()

for i in range(1000):
    category = random.choice(categories)
    product = random.choice(products[category])
    quantity = random.randint(1, 10)
    price = round(random.uniform(100, 10000), 2)
    amount = round(quantity * price, 2)
    
    # Добавляем несколько некорректных записей
    if random.random() < 0.05:  # 5% плохих данных
        amount = -amount  # Отрицательная сумма
    
    transaction = {
        "transaction_id": i + 1,
        "date": (base_date - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
        "customer_id": random.randint(100, 200),
        "category": category,
        "product": product,
        "quantity": quantity,
        "price": price,
        "amount": amount
    }
    transactions.append(transaction)

# Определяем путь к файлу в той же директории, где находится скрипт
script_dir = "C:\\test\\" #os.path.dirname(os.path.abspath(__file__))
output_file = os.path.join(script_dir, "transactions.csv")

# Сохранение в CSV
try:
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["transaction_id", "date", "customer_id", "category", "product", 
                      "quantity", "price", "amount"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)
    
    print(f"Создано {len(transactions)} транзакций")
    print(f"Файл сохранён: {output_file}")
    
    # Статистика
    print("\n=== Статистика ===")
    
    # Подсчёт по категориям
    category_counts = {}
    negative_amounts = 0
    
    for transaction in transactions:
        category = transaction.get("category")
        if category:  # Проверяем, что category не None
            category_counts[category] = category_counts.get(category, 0) + 1
        
        if transaction.get("amount", 0) < 0:
            negative_amounts += 1
    
    print(f"\nРаспределение по категориям:")
    for category, count in sorted((k, v) for k, v in category_counts.items() if k is not None):
        percentage = (count / len(transactions)) * 100
        print(f"  {category}: {count} ({percentage:.1f}%)")
    
    print(f"\nНекорректных записей (отрицательные суммы): {negative_amounts} ({(negative_amounts/len(transactions)*100):.1f}%)")
    print(f"Уникальных клиентов: {len(set(t['customer_id'] for t in transactions))}")
    
except PermissionError:
    print(f"ОШИБКА: Нет прав доступа для записи в {output_file}")
    print("Попробуйте запустить скрипт от имени администратора")
except Exception as e:
    print(f"ОШИБКА при записи файла: {e}")
    print(f"Попробуйте указать другой путь или проверьте права доступа")