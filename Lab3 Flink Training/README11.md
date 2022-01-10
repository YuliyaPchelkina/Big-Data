# Laboratory #3
# Streaming processing in Apache Flink
# Mukhin Artem 6133-010402D

_All tests were successfully passed!_

# Exercise #1 (_RideCleansingExercise_)

## Task 
The task of the exercise is to filter a data stream of taxi ride records to keep only rides that 
start and end within New York City. The resulting stream should be printed.

## Solution

```java
private static class NYCFilter implements FilterFunction<TaxiRide> {
    @Override
    public boolean filter(TaxiRide taxiRide) throws Exception {
        return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
    }
}
```

## Solution explanation
In this task we just need to implement simple _Filter function_.

Nothing interesting here, see u in the next exercise.

# Exercise #2 (_RidesAndFaresExercise_)

## Task
The goal for this exercise is to enrich TaxiRides with fare information.

## Solution

```java
	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

    private ValueState<TaxiRide> taxiRideState;
    private ValueState<TaxiFare> taxiFareState;

    @Override
    public void open(Configuration config) throws Exception {
        ValueStateDescriptor<TaxiRide> taxiRideDescriptor = new ValueStateDescriptor<TaxiRide>(
                "persistedTaxiRide",
                TaxiRide.class
        );
        ValueStateDescriptor<TaxiFare> taxiFareDescriptor = new ValueStateDescriptor<TaxiFare>(
                "persistedTaxiFare",
                TaxiFare.class
        );

        taxiRideState = getRuntimeContext().getState(taxiRideDescriptor);
        taxiFareState = getRuntimeContext().getState(taxiFareDescriptor);
    }

    @Override
    public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiFare fare = taxiFareState.value();
        if (fare != null) {
            taxiFareState.clear();
            out.collect(new Tuple2<>(ride, fare));
        } else {
            taxiRideState.update(ride);
        }
    }

    @Override
    public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiRide ride = taxiRideState.value();
        if (ride != null) {
            taxiRideState.clear();
            out.collect(new Tuple2<>(ride, fare));
        } else {
            taxiFareState.update(fare);
        }
    }
}
```

## Solution explanation
In this exercise we get acquainted with `RichCoFlatMapFunction`.

Each element in the stream **needs** to have its event timestamp assigned.
This is usually done by accessing/extracting the timestamp. In our case `startTime` or `endTime` is using.

A `RichCoFlatMapFunction` represents a FlatMap transformation with two different input types. 

And the most important thing in this exercise:

`ValueState<T>` - keeps a value that can be updated and retrieved 
(**scoped to key** of the input element as mentioned above, so there will possibly be one value for each key that the operation sees).


# Exercise #3 (_HourlyTipsExercise_)

## Task
* The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
* then from that stream, find the highest tip total in each hour.

## Solution

```java
public class HourlyTipsExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        
//        ****************************************************
        
        DataStream<Tuple3<Long, Long, Float>> hourlyTips =
                fares.map(fare -> new Tuple2<Long, Float>(fare.driverId, fare.tip))
                        .returns(Types.TUPLE(Types.LONG, Types.FLOAT))
                        .keyBy(fare -> fare.f0)
                        .timeWindow(Time.hours(1))
                        .reduce(new ReduceSumFunction(), new MyProcessWindowFunction());


        DataStream<Tuple3<Long, Long, Float>> hourlyMax =
                hourlyTips.timeWindowAll(Time.hours(1))
                        .maxBy(2);
    }
}

private static class ReduceSumFunction implements ReduceFunction<Tuple2<Long, Float>> {
    @Override
    public Tuple2<Long, Float> reduce(Tuple2<Long, Float> v1, Tuple2<Long, Float> v2) throws Exception {
        return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
    }
}

private static class MyProcessWindowFunction
        extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow> {

    @Override
    public void process(
            Long key,
            ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context context,
            Iterable<Tuple2<Long, Float>> iterable,
            Collector<Tuple3<Long, Long, Float>> out
    ) throws Exception {
        Float sumOfTips = iterable.iterator().next().f1;
        out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
    }
}
```

## Solution explanation
As I mention above each element in the stream **needs** to have its event timestamp assigned. In this task
it's important to assign right watermark. But we don't worry about this, since exercise author's already assign
required watermark.

In this exercise I faced weird issue with java compiler. It's good that in the documentation I found the cure.
https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/datastream/java_lambdas/

With help of __WindowFunction__ we're able to solve the task.

# Exercise #4 (_ExpiringStateExercise_)

## Task
The goal for this exercise is to enrich TaxiRides with fare information.

## Solution

```java
public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

    private ValueState<TaxiRide> taxiRideState;
    private ValueState<TaxiFare> taxiFareState;

    @Override
    public void open(Configuration config) throws Exception {
        ValueStateDescriptor<TaxiRide> taxiRideDescriptor = new ValueStateDescriptor<TaxiRide>(
                "persistedTaxiRide",
                TaxiRide.class
        );
        ValueStateDescriptor<TaxiFare> taxiFareDescriptor = new ValueStateDescriptor<TaxiFare>(
                "persistedTaxiFare",
                TaxiFare.class
        );

        taxiRideState = getRuntimeContext().getState(taxiRideDescriptor);
        taxiFareState = getRuntimeContext().getState(taxiFareDescriptor);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        if (taxiFareState.value() != null) {
            ctx.output(unmatchedFares, taxiFareState.value());
            taxiFareState.clear();
        }
        if (taxiRideState.value() != null) {
            ctx.output(unmatchedRides, taxiRideState.value());
            taxiRideState.clear();
        }
    }

    @Override
    public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiFare fare = taxiFareState.value();
        if (fare != null) {
            taxiFareState.clear();
            context.timerService().deleteEventTimeTimer(ride.getEventTime());
            out.collect(new Tuple2<>(ride, fare));
        } else {
            taxiRideState.update(ride);
            context.timerService().registerEventTimeTimer(ride.getEventTime());
        }
    }

    @Override
    public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
        TaxiRide ride = taxiRideState.value();
        if (ride != null) {
            taxiRideState.clear();
            context.timerService().deleteEventTimeTimer(fare.getEventTime());
            out.collect(new Tuple2<>(ride, fare));
        } else {
            taxiFareState.update(fare);
            context.timerService().registerEventTimeTimer(fare.getEventTime());
        }
    }
}
```

## Solution explanation
The ProcessFunction can be thought of as a FlatMapFunction with access to keyed state and timers. 
It handles events by being invoked for each event received in the input stream(s).

KeyedProcessFunction, as an extension of ProcessFunction, gives access to the key of timers in its onTimer(...) method.

The TimerService deduplicates timers per key and timestamp, i.e., there is at most one timer per key and timestamp.
If multiple timers are registered for the same timestamp, the onTimer() method will be called just once.

So if there is _no ride_ for the target fare - we'll add that ride in the _unmatchedRides_ list. The same for fares.