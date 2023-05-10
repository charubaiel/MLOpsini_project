## Описание : 
Проектик для использования и тестирования технологий биг ~~и не очень~~ даты

# Stack
* Workflow Manager
    - dagster  
        * Универсальная и "питонячая" DAG платформа
        * Репродуцируемость эксперементов
        * Пайпланизация кода
        * Дата оркестратор
        * Шаблонизация запусков
* BD 
    - DuckDB
        * Колоночная бдшка с классной итеграцией по форматам и технологиям
        * Нативная поддержка 
            * .csv
            * .parquet
            * других бд (sqlite,postgres)
    - Clickhouse  
        * Царь колоночных бд
* DM  - Poetry 
    * Удобный и гибкий медеджер зависимостей
* Deploy
    - docker
        * Инструмент виртуализации окружения
        * Гарантируем воспроизведения среды разработки на любой консервной банке
    - FastAPI
        * Интерфейс построения удобных API интерфейсов
    - MLFlow
        * Менеджер эксперементов
            * Логирование метрик
            * Хранение моделей
            * Хранение метаданных
            * Версионирование моделей
            * Версионирование артефактов
* Parsing
    - Selenium
        * Эмуляция браузера для сбора открытых данных
    - bs4
        * Удобный парсинг html страниц

* Processing
    - Scikit-learn - no comments (bible of ds)
    - Optuna - Оптимизатор гиперпараметров
    - Natasha - Удобная работа с ru NLP 
    - Geopy - Получения координат местности и мелких подсчетов по географии
    - OSMnx - Работы с массивом карт как с графом
    - NetworkX - Работа с графами
        * Нахождение кратчайшего пути и тд
        * Оценка связанности
        * Длинна графа 
        * и тд


# Want to Implement
+ Bayesian Bandits


# local run

```bash
pip install poetry # если не установлен
poetry install
poetry shell

cd src && bash start.sh

```