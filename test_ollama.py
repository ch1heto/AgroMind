import requests

print("⏳ Отправляю тестовый запрос к Ollama (qwen3.5:9b)...")
print("Ждем загрузки модели в видеопамять (может занять до 1-2 минут при первом запуске)...\n")

try:
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "qwen3.5:9b",
            "prompt": "Привет! Ответь очень коротко: ты меня слышишь и готова к работе?",
            "stream": False
        },
        timeout=300  # Даем 5 минут на случай, если модель медленно грузится с диска
    )
    
    response.raise_for_status()
    print("✅ УСПЕХ! Ответ от нейросети:")
    print("-" * 40)
    print(response.json().get("response"))
    print("-" * 40)

except Exception as e:
    print(f"❌ ОШИБКА: {e}")