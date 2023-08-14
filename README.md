# DE_practicum_test
ETL скрипт для выгрузки данных через api с использованием docker и airflow + postgres

Инструкция:
Код разрабатывался под чип apple silicon

1) Скачать данный репозиторий к себе на компьютер
2) В корневой директории вызвать команду ```docker compose up```
3) Подключиться к базе данных черех удобные для вас утилиты (см <b><a href="#Connection"> параметры подключения</a></b>):
4) Зайти и авторизоваться на airflow по ссылке - http://localhost:8080/ . Логин - _airflow_ пароль - _airflow_
5) Перейти во вкладку DAG и запустить скрипт <a>yandex-practicum-load-currency</a>
<p> 
<a id="#Connection"> Параметры подключения:</a>
<li>
Хост - localhost
</li>
<li>
База данных - yandex
</li>
<li>
Порт - 5442
</li>
<li>
Пользователь - postgres
</li>
<li>
Пароль - postgres
</li>
<li>
Таблица - currency_pair
</li>
</p>