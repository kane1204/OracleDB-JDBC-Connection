package mySQLDB;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class Entry {
    public LocalDate date ;
    public LocalTime time ;
    public String message;
    
    public Entry(String timedate, String message) {
        this.message = message;
        System.out.println(timedate);
        
        int dend = timedate.indexOf(" ");
        String datesub = timedate.substring(0 , dend);
        this.date = LocalDate.of(Integer.parseInt(datesub.substring(6,datesub.length())), Integer.parseInt(datesub.substring(3,5)), Integer.parseInt(datesub.substring(0,2)));
        
        String timesub = timedate.substring(dend+1,timedate.length());
        this.time = LocalTime.parse(timesub, DateTimeFormatter.ISO_TIME);
    }
    
    @Override
    public String toString() {
        return "entry [date=" + date + ", time=" + time + ", message=" + message + "]";
    }
}
