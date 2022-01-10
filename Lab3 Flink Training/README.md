## Лабораторная 3. Потоковая обработка в Apache Flink

### Данные: 
- Датасет с данными о поездках такси в Нью-Йорке https://github.com/apache/flink-training/blob/master/README.md#using-the-taxi-data-streams 
- Файлы nycTaxiFares.gz и nycTaxiRides.gz в папке data https://gitlab.com/ssau.tk.courses/big_data/-/tree/master/data
- Задания (из репозитория https://github.com/ververica/flink-training-exercises)

### 1. RideCleanisingExercise
Задача состоит в том, чтобы отфильтровать данные о поездках.
Оставляем только те поездки, которые не выходят за пределы Нью-Йорка (начинаются и заканчиваются внутри города)


```scala
    
    // функция, проверяющая попадают ли данные поездки в область, ограниченную координатами Нью-Йорка
    def NewY(lon: Float, lat: Float) = !(lon > -73.7 || lon < -74.05) && !(lat > 41.0 || lat < 40.5)

    val filteredRides = rides
      // отфильтровываем лишние поездки 
      .filter(ride => NewY(ride.startLon, ride.startLat) && NewY(ride.endLon, ride.endLat))
```


### 2. RidesAndFaresExercise



### 3. HourlyTipsExerxise
Задача состоит в том, чтобы подсчитать размер чаевых каждого водителя за каждый час. 
Среди полученных данных находим наибольший результат



### 4. ExpiringStateExercise



