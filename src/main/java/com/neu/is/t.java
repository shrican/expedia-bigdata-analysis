package com.neu.is;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class t {

	public static void main(String[] args) throws ParseException {
		
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm");
		
		String values = "3,6/5/2013 17:56,5,219,NULL,NULL,219,4610,4,4.5,1,3,0.0062,0,275,0,10797,1,21,4,0,2,0,NULL,23.85,1,NULL,NULL,NULL,0,0,NULL,0,0,NULL,NULL,NULL,NULL,0,0,NULL,NULL,NULL,NULL,NULL,NULL,NULL,0,0,NULL";
		String[] data = values.toString().split(",");

		int satBooking = Integer.parseInt(data[22]);

		int weekdayBookings = 0;
		int weekendBookings = 0;
		int totalBookings = 1;

		if (satBooking == 1) {

			weekendBookings = 1;

		} else {
			weekdayBookings = 1;
		}

		String timeStamp = data[1];
		Date date = new Date();

		date = format.parse(timeStamp);

		Calendar c = Calendar.getInstance();
		c.setTime(date);

		int month = c.get(Calendar.MONTH);
		
		
		
		System.out.println(c.toString());
		System.out.println(month);
		
//		String[] data = s.split("//");
//
//		for (String v : data) {
//
//			System.out.println("11 : " + v);
//		}

	}
}
