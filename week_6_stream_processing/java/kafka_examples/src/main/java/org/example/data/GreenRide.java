package org.example.data;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class GreenRide {
    public GreenRide(String[] arr) {
        VendorID = arr[0];
        lpep_pickup_datetime = LocalDateTime.parse(arr[1], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        lpep_dropoff_datetime = LocalDateTime.parse(arr[2], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        store_and_fwd_flag = arr[3];
        RatecodeID = Long.parseLong(arr[4]);
        PULocationID = Long.parseLong(arr[5]);
        DOLocationID = Long.parseLong(arr[6]);
        passenger_count = Integer.parseInt(arr[7]);
        trip_distance = Double.parseDouble(arr[8]);
        fare_amount = Double.parseDouble(arr[9]);
        extra = Double.parseDouble(arr[10]);
        mta_tax = Double.parseDouble(arr[11]);
        tip_amount = Double.parseDouble(arr[12]);
        tolls_amount = Double.parseDouble(arr[13]);
        ehail_fee = arr[14];
        improvement_surcharge = Double.parseDouble(arr[15]);
        total_amount = Double.parseDouble(arr[16]);
        payment_type = arr[17];
        trip_type =arr[18];
        congestion_surcharge = arr[19];
    }
    public GreenRide(){}
    public String VendorID;
    public LocalDateTime lpep_pickup_datetime;
    public LocalDateTime lpep_dropoff_datetime;
    public int passenger_count;
    public double trip_distance;
    public long RatecodeID;
    public String store_and_fwd_flag;
    public long PULocationID;
    public long DOLocationID;
    public String payment_type;
    public double fare_amount;
    public double extra;
    public double mta_tax;
    public double tip_amount;
    public double tolls_amount;
    public double improvement_surcharge;
    public double total_amount;
    public String congestion_surcharge;

    public String ehail_fee;

    public String trip_type;

}

