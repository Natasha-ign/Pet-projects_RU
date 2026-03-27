## Автоматический отчет по динамике работы приложения

Задача: Создать автоматический отчет по ключевым метрикам приложения с ежедневной отправкой в Telegram.

Реализация: создан скрипт, забирающий данные из БД с помощью SQL, рассчитывающий метрики и строящий графики. Срипт обернут в DAG и с помощью Airflow отчет автоматически отправляется в Телеграм с помощью бота.

Использованы инструменты: SQL, Airflow, DAG,  telegram bot, seaborn, matplotlib

Результаты: файл.py, скриншоты отчета в телеграм.

<img width="612" height="646" alt="report_bot_telegram_msg" src="https://github.com/user-attachments/assets/25dbe253-b84b-4a7b-a5ba-d78c4fad3d02" />

![report_bot_telegram_graphs](https://github.com/user-attachments/assets/c8ae4788-7a69-4044-85be-55b863dd7a39)
