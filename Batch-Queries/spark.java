import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.types.DataTypes;
import java.util.Scanner;
import static org.apache.spark.sql.functions.*;


public class spark {

    final static double max_latitude = 40.915263;
    final static double min_latitude = 40.542743;
    final static double max_longitude = -73.701435;
    final static double min_longitude = -74.255536;

    final static double centerx = min_longitude + (max_longitude - min_longitude) / 2;
    final static double centery = min_latitude + (max_latitude - min_latitude) / 2;

    static Dataset<Row> df;
    static SparkSession sparkSession;


    public static <timestamp> void main(String args[]) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf();
        conf.setAppName("spark_apps.JavaDataframeExample");
        conf.setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sparkSession = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
        df = sparkSession.read().format("csv").option("header", "true").option("inferSchema", "true").load("fares.csv");
        //coordinates inside NY
        df = df.where("pickup_latitude >= 40.542743 AND pickup_latitude <= +40.915263");
        df = df.where("dropoff_latitude >= +40.542743 AND dropoff_latitude <= +40.915263");
        df = df.where("pickup_longitude >= -74.255536 AND pickup_longitude <=  -73.701435");
        df = df.where("dropoff_longitude >= -74.255536 AND dropoff_longitude <=  -73.701435");

        sparkSession.udf().register("myAverage", new MyAverage());
        sparkSession.udf().register("myFunction", new UDF4<Double, Double, Double, Double, Double>() {
            @Override
            public Double call(Double lat1, Double lon1, Double lat2, Double lon2) throws Exception {
                if ((lat1 == lat2) && (lon1 == lon2)) {
                    return Double.valueOf(0);
                } else {
                    Double theta = lon1 - lon2;
                    Double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
                    dist = Math.acos(dist);
                    dist = Math.toDegrees(dist);
                    dist = dist * 60 * 1.1515;
                    dist = dist * 1.609344;
                    return (dist);
                }
            }
        }, DataTypes.DoubleType);
        //registers a new internal UDF
        sparkSession.udf().register("district9", new UDF2<Double, Double, Double>() {
            @Override
            public Double call(Double l1, Double l2) throws Exception {
                if (l1 > centerx) {
                    if (l2 > centery) {
                        return 1.0;
                    } else {
                        return 3.0;
                    }
                } else {
                    if (l2 > centery) {
                        return 2.0;
                    } else {
                        return 4.0;
                    }
                }
            }
        }, DataTypes.DoubleType);

        //create new Schema
        df = df.withColumn("Day", functions.dayofmonth(df.col("pickup_datetime")));
        df = df.withColumn("Month", functions.month(df.col("pickup_datetime")));
        df = df.withColumn("District_start", callUDF("district9", df.col("pickup_longitude"), df.col("pickup_latitude")));
        df = df.withColumn("District_final", callUDF("district9", df.col("dropoff_longitude"), df.col("dropoff_latitude")));
        df = df.withColumn("Distance", callUDF("myFunction", df.col("pickup_latitude").cast(DataTypes.DoubleType), df.col("pickup_longitude").cast(DataTypes.DoubleType), df.col("dropoff_latitude").cast(DataTypes.DoubleType), df.col("dropoff_longitude").cast(DataTypes.DoubleType)));
        df = df.withColumn("Distance", bround(col("Distance"), 3));
        df = df.withColumn("hour", functions.hour(col("pickup_datetime")));
        df = df.filter("Distance >0.0");

         while (true ) {
             System.out.println("\n");
             System.out.println("Type which query you want to execute");
             Scanner obj = new Scanner(System.in);
             int query = obj.nextInt();

             if (query == 1) {
                 firstQuery();
             } else if (query == 2) {
                 secondQuery();
             } else if (query == 3) {
                 thirdQueryBatch();
             } else if (query == 4) {
                 fourthQueryBatch();
             } else if (query == 5) {
                 fifthQuery();
             } else if (query == 6) {
                 sixthQueryBatch();
             } else if (query == 7) {
                 seventhQuery();
             }
             else if (query == 8) {
                 eigthQueryBatch();
             }
         }

    }


    public static void firstQuery() {

        System.out.println("--------First Query---------");
        Dataset<Row> firstquestion = df.select(df.col("Day"), df.col("District_start")).groupBy(df.col("Day"), df.col("District_start")).count();
        firstquestion = firstquestion.orderBy(df.col("Day"), df.col("District_start"));


        firstquestion.printSchema();

        Row[] rows = (Row[]) firstquestion.collect();
        for (Row row : rows) {
            System.out.println("{Day: " + row.get(0) + " Area: " + row.get(1) + "} -> " + row.get(2));
        }
    }

    public static void secondQuery() {
        System.out.println("--------Second Query---------");

        Dataset<Row> secondquestion_firstpart = df.groupBy(df.col("District_start")).agg((avg(df.col("trip_duration")).alias("trip_duration")));
        secondquestion_firstpart.show();
        secondquestion_firstpart.orderBy(desc("trip_duration")).select("District_start").show(1);

        Dataset<Row> secondquestion_secondpart = df.groupBy(df.col("District_start")).agg(functions.callUDF("myAverage", df.col("Distance")).alias("average_distance"));
        secondquestion_secondpart.show();
        secondquestion_secondpart.orderBy(desc("average_distance")).select("District_start").show(1);
    }

    public static void thirdQueryBatch() {
        System.out.println("--------Third Query---------");
        Dataset<Row> thirdquestion = df.select(df.col("Distance"),df.col("trip_duration"),df.col("passenger_count")).filter("Distance >= 1 AND trip_duration >= 600 AND passenger_count > 2");
        thirdquestion.show();
    }

    public static void fourthQueryBatch() {
        System.out.println("--------Fourth Query---------");
        df.groupBy(df.col("hour")).count().show();
    }

    public static void fifthQuery() {
        System.out.println("--------Fifth Query---------");
        Double longitude;
        Double latitude;
        int hour;
        Scanner scan = new Scanner(System.in);
		
        System.out.print("Enter longitude:");
        longitude = scan.nextDouble();
        System.out.print("Enter latitude:");
        latitude = scan.nextDouble();
        System.out.print("Enter hour:");
        hour = scan.nextInt();

        double quadrant;

        if (longitude > centerx) {
            if (latitude > centery) {
                quadrant = 1.0;
            } else {
                quadrant = 3.0;
            }
        } else {
            if (latitude > centery) {
                quadrant = 2.0;
            } else {
                quadrant = 4.0;
            }
        }

        System.out.println(df.filter(functions.hour(df.col("pickup_datetime")).equalTo(hour)).filter(df.col("District_start").equalTo(lit(quadrant))).count());
        df.filter(functions.hour(df.col("pickup_datetime")).equalTo(hour)).filter(df.col("District_start").equalTo(lit(quadrant))).show();
    }
    public static void sixthQueryBatch() {
        Dataset<Row> sixthquestion = df.withColumn("DayOfYear",functions.dayofyear(df.col("pickup_datetime")));
        sixthquestion = sixthquestion.groupBy(sixthquestion.col("DayOfYear"),sixthquestion.col("vendor_id"),sixthquestion.col("hour")).count();//.agg(df.col("count")).show();
        sixthquestion.printSchema();
        sixthquestion.groupBy(sixthquestion.col("DayOfYear"),sixthquestion.col("vendor_id"),sixthquestion.col("hour")).agg(functions.max(sixthquestion.col("count"))).show(500);	
    }

    public static void eigthQueryBatch() {
        System.out.println("--------Sixth Query---------");
        df = df.withColumn("ana_hour" , functions.hour(df.col("pickup_datetime")));
        df = df.withColumn("ana_day" , functions.dayofyear(df.col("pickup_datetime")));
        df.createOrReplaceTempView("dataset");

        Dataset <Row> res = sparkSession.sql("SELECT vendor_id,ana_hour, ana_day , COUNT(ana_hour) as countless FROM dataset GROUP BY ana_day , vendor_id,ana_hour  ORDER BY ana_day,vendor_id , countless DESC ");
        Row[] rows = (Row[]) res.collect();
        boolean counter=true;
        int i =0;
        int y=1;
        for(Row row : rows) {
            if (row.get(0).equals(1) && i==0){
                System.out.println("---Vendor 1----");
                i=i+1;
            }
            if ( y>7) {
                counter=false;
            }
            if (row.get(2).equals(y) && counter) {
                System.out.println("Day : " + row.get(2) +  " " +" max was: " + row.get(3) + " " +"at time:"+ " " + row.get(1)+":00");
                y = y+1;
            }
        }
        y=1;
        i=0;
        counter=true;
        for(Row row : rows) {
            if (row.get(0).equals(2) && i==0){
                System.out.println("---Vendor 2----");
                i=i+1;
            }
            if ( y>7) {
                counter=false;
            }
            if (row.get(0).equals(2) && row.get(2).equals(y) && counter) {
                System.out.println("Day : " + row.get(2) +  " " +" max was: " + row.get(3) + " " +"at time:" + " "+ row.get(1)+":00");
                y = y+1;
            }
        }
    }

    public static void seventhQuery() {
        System.out.println("--------Seventh Query---------");
        System.out.println("\n");
        df = df.withColumn("DayOf", dayofweek(df.col("pickup_datetime")));
        df = df.withColumn("Hour", hour(df.col("pickup_datetime")));
        df.createOrReplaceTempView("dataset");
        Dataset<Row> res = sparkSession.sql("SELECT COUNT(DayOf) AS NUM_OF_TAXIS ,Hour,DayOf  FROM dataset WHERE DayOf = 1 OR  DayOf = 7  GROUP BY DayOf , Hour ORDER BY DayOf , Hour");
        Row[] rows = (Row[]) res.collect();
        int i=0;
        int y=0;
        for(Row row : rows) {
             if (row.get(2).equals(1) && i==0){
                 System.out.println(" ----Num_Of_Taxis in Sunday by hour----" );
                 System.out.println("\n");
                 i=i+1;
             }
             if  (row.get(2).equals(1)){
                 System.out.println(" hour ->" + " "+  row.get(1) + ":00" + " "  + "taxis ->" + " " + row.get(0) );
            }
            if (row.get(2).equals(7) && y==0){
                System.out.println("\n");
                System.out.println(" ----Num_Of_Taxis in Saturday by hour----" );
                System.out.println("\n");
                y=y+1;
            }
            if  (row.get(2).equals(7)){
                System.out.println(" hour:" + " " + row.get(1)+ ":00" + " " + "taxis ->" + " " + row.get(0) );
            }
        }

    }

    public static void VendorCallsPerHourStream() {
        df.groupBy(window(df.col("pickup_datetime"), "24 hours")).count().show();
    }

    public static void CallsPerHourStream() {
        df.groupBy(window(functions.split(df.col("pickup_datetime"), " ").getItem(1), "1 hour")).count().show(24);
    }
}


