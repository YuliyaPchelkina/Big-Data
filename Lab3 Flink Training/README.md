### Лабораторная 3. Потоковая обработка в Apache Flink

Данные: Датасет с данными о поездках такси в Нью-Йорке https://github.com/apache/flink-training/blob/master/README.md#using-the-taxi-data-streams. 
Файлы nycTaxiFares.gz и nycTaxiRides.gz в папке data https://gitlab.com/ssau.tk.courses/big_data/-/tree/master/data.



Задания (из репозитория https://github.com/ververica/flink-training-exercises:):

1. RideCleanisingExercise
2. RidesAndFaresExercise
3. HourlyTipsExerxise
4. ExpiringStateExercise


git clone https://github.com/ververica/flink-training.git ververica-flink-training
cd ververica-flink-training
./gradlew test shadowJar
