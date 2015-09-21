# icbda
Python scripts for preprocessing data

Новый план:
1. Формируем файл с данными для всех пользоватей с обучающими данными.
2. Индексируем хеш данные.
3. Чистим дату.
4. Преобразуем суммы в числа.
5. Понимаем, что нужно для RandomForest


План
1. вырезаем файл 10 000 строк: cut_10k.csv
2. выбираем из него пользователей, предполагаю, что их будет < 10 000: cut_10k_users.csv
3. вырезаем из большого файла все записи: cut_10k_users_all_records.csv
4. оставляем в файле ID пользователей, все остальные данные которые являются хешем - уникальным (если такие есть) удаляем.
5. попытаться заменить адресные данные на параметр, характеризующий
5. на полученных данных пытаемся понять, какие переменные являются значимыми.
