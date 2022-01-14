## Лабораторная 3. Потоковая обработка в Apache Flink

### Данные: 
- Датасет с данными о поездках такси в Нью-Йорке https://github.com/apache/flink-training/blob/master/README.md#using-the-taxi-data-streams 
- Файлы nycTaxiFares.gz и nycTaxiRides.gz в папке data https://gitlab.com/ssau.tk.courses/big_data/-/tree/master/data
- Задания (из репозитория https://github.com/ververica/flink-training-exercises)

### 1. RideCleanisingExercise
Задача состоит в том, чтобы отфильтровать данные о поездках.
Оставляем только те поездки, которые не выходят за пределы Нью-Йорка (начинаются и заканчиваются внутри города)
В в файл RideCleansingExercise.scala добавлена функция:

```scala
    
    // функция, проверяющая попадают ли данные поездки в область, ограниченную координатами Нью-Йорка
    def NewY(lon: Float, lat: Float) = !(lon > -73.7 || lon < -74.05) && !(lat > 41.0 || lat < 40.5)

    val FilterRides = rides
      // отфильтровываем лишние поездки 
      .filter(ride => NewY(ride.startLon, ride.startLat) && NewY(ride.endLon, ride.endLat))
```


### 2. RidesAndFaresExercise



### 3. HourlyTipsExerxise
Задача состоит в том, чтобы подсчитать размер чаевых каждого водителя за каждый час. 
Среди полученных данных находим наибольший результат

В в файл HourlyTipsExercise.scala изменена функция main:

```scala
    def main(args: Array[String]) {

    // read parameters
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", ExerciseBase.pathToFareData)

    val maxDelay = 60 // events are delayed by at most 60 seconds
    val speed = 600   // events of 10 minutes are served in 1 second

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(ExerciseBase.parallelism)

    // start the data generator
    val fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxDelay, speed)))

    // max tip total in each hour
    val hourlyMax = fares
      .map(fare => (fare.driverId, fare.tip))
      // key by driver id
      .keyBy(_._1)
      // convert to window stream
      .timeWindow(Time.hours(1))
      .reduce(
        // calculate total tips
        (f1, f2) => {
          (f1._1, f1._2 + f2._2)
        },
        new WrapWithWindowInfo()
      )
      .timeWindowAll(Time.hours(1))
      .maxBy(2)

    // print result on stdout
    printOrTest(hourlyMax)

    // execute the transformation pipeline
    env.execute("Hourly Tips (scala)")
  }

```
И добавлено описание класса:

```scala
  class WrapWithWindowInfo() extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
      val sumOfTips = elements.iterator.next()._2
      out.collect((context.window.getEnd, key, sumOfTips))
    }
  }
```

### 4. ExpiringStateExercise



