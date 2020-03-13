
package mySQLDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Iterator;

public class SQLConn {
    //Globals
    Connection conn;
    static ArrayList<Entry> entries;
    static String SQL_SELECT = "Select * from lloyds_data ORDER BY eid DESC";
    static ArrayList<String> finalValues;
    static ArrayList<Integer> noOfDates;
    
    // Connecting To Database Details
    static String USERNAME = "admin";
    static String PASSWORD = "pass";
    static String CONN_STR = "jdbc:mysql://127.0.0.1:3306/db";
    //Note the data base column names need to be changed
    
    public static void main(String[] args) {
        connectDBandRetreive();
        if(noOfDates != null) {
            calculateAllDates();
        }
        //printfinalValues();
        System.out.println("Finished!");
    }
    
    public static void connectDBandRetreive() {
     // auto close connection
        System.out.println("MySQL/OracleDB JDBC Connection ~");
        noOfDates = new ArrayList<>();
        entries = new ArrayList<>();


        try (Connection conn = DriverManager.getConnection(
                CONN_STR, USERNAME, PASSWORD);
             PreparedStatement preparedStatement = conn.prepareStatement(SQL_SELECT)) {

            ResultSet resultSet = preparedStatement.executeQuery();
           

            while (resultSet.next()) {
                
                String messagedate = resultSet.getString("messagedate");
                String message = resultSet.getString("message");
                
                
                Entry tempEntry = new Entry(messagedate,message);

                entries.add(tempEntry);
            }
            
           
            
            
            //entries.forEach(x -> System.out.println(x));

        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LocalDate tempDate = LocalDate.of(1888,04,12);

        for (int i = 0; i < entries.size(); i++) {
            //System.out.println("Temp Date: " + tempDate);
            if(i+1 < entries.size()){
                 if(!(tempDate.equals(entries.get(i+1).date)) && entries.get(i).message.substring(0, 3).equals("Sta")){
                    noOfDates.add(i);
                    tempDate = entries.get(i).date;
                }
//                 else if(!(tempDate.equals(entries.get(i+1).date)) && entries.get(i+1).message.substring(0, 3).equals("Eve")) {
//                        if(i+2 < entries.size()) {
//                            System.out.println(i);
//                            noOfDates.add(i+2);
//                            System.out.println("WARNING: EVENT OVERLAP, Between: " +tempDate + " - " +entries.get(i+1).date);
//                            tempDate = entries.get(i+2).date;
//                            i= i+2;
//                            //System.out.println(i);
//                            
//                            
//                        
//                    }else if(!(tempDate.equals(entries.get(i+1).date))) {
//                        noOfDates.add(i);
//                        tempDate = entries.get(i).date;
//                        System.out.println("jeff");
//                    }
//                }
               
            }
            //System.out.println(entries.get(i));
            //System.out.println(noOfDates);
        }   

    }
    public static void calculateAllDates() {
        
        for(int i=0 ; i < noOfDates.size() ;i++) {
            //System.out.println("Calculating next date running: "+ entries.get(noOfDates.get(i)).date);
            calculateDates(i);
        }
        
    }
    public static void calculateDates(int index) {
            int sidx = noOfDates.get(index);
            int didx = entries.size();
            ArrayList<Entry> tempList = new ArrayList<Entry>();
            if(noOfDates.size() != 1) {
                if((index+2) > noOfDates.size()) {
                    didx = entries.size();
                }
                else{
                didx = noOfDates.get(index+1);
                }
            }
            for(int i= sidx; i < didx ; i++) {
                //System.out.println(entries.get(i));
                //add to all the queries to a temp array passed in to the 3 functions below
                tempList.add(entries.get(i));
            }
            // call 3 functions to calculate time taken and number of 
            //files ran then finally add that data to the result array
            int totalSecs = calculateTotalTime(tempList,index+1);
            int noFiles = calculateTotalFiles(tempList);
            int noRecords = calculateTotalRecords(tempList);
            int noTemplates = calculateTotalTemplates(tempList);
            int noBeneAcc = calculateTotalBene(tempList);
            
            
            int hours = totalSecs / 3600;
            int minutes = (totalSecs % 3600) / 60;
            int seconds = totalSecs % 60;
            
            String str = tempList.get(0).date +"~ Total time: "+hours+ " hours "+ minutes +" mins "+  seconds +" secs"+", Number Of Files: "+ noFiles+" Number Of Records: "+ noRecords+", Number Of Templates: "+ noTemplates+", Number Of Benefiary Accounts: "+ noBeneAcc;                
            System.out.println(str);
    }
    public static int calculateTotalTime(ArrayList<Entry> list,int i) {
        // total time added all up
        int result = 0; 
        LocalTime startTime = null;
        LocalTime endTime = null;
        LocalDate startDate =null;
        LocalDate endDate = null;
        
        Iterator<Entry> listIterator2 = list.iterator();
        while (listIterator2.hasNext()) {
            Entry tempEntry = listIterator2.next();
            String mess = tempEntry.message.substring(0,3);
            //System.out.println(mess);
            if(mess.equals("Sta")) {
                startTime = tempEntry.time;
                startDate = tempEntry.date;
                //System.out.println(startTime);
                }
            else if(mess.equals("Eve")) {
                endTime = tempEntry.time;
                endDate = tempEntry.date;
                //System.out.println(endTime);
                }
            if(endTime != null && startTime != null) {
                    Duration duration = Duration.between(startTime, endTime);
                    if(!(startDate.equals(endDate))) {
                        result= (int) (result + duration.plusDays(1).getSeconds())  ;
                    }else {
                        result= (int) (result + duration.getSeconds());
                    }
                    endTime = null;
                    startTime = null;
                    endDate = null;
                    startDate = null;
                }
            }
            
        
        
        return result;
        
    }
    public static int calculateTotalFiles(ArrayList<Entry> list) {
        int result = 0;
        
        Iterator<Entry> listIterator = list.iterator();
        while (listIterator.hasNext()) {
            Entry tempEntry = listIterator.next();
            String mess = tempEntry.message.substring(0,3);
            //System.out.println(mess);
            if(mess.equals("Num")) {
                result++;
            }
        }
        return result;
        
    }
    public static int calculateTotalRecords(ArrayList<Entry> list) {
        // records processed added up for a day
        int result = 0;
        Iterator<Entry> listIterator = list.iterator();
        while (listIterator.hasNext()) {
            Entry tempEntry = listIterator.next();
            String sub0 = tempEntry.message.substring(0,3);
            
            if(sub0.equals("Num")) {
                String sub1 =  tempEntry.message.substring(tempEntry.message.indexOf(": ")+2,tempEntry.message.indexOf(","));
                result = result + Integer.parseInt(sub1);
            }
        }
        return result;
        
    }
    public static int calculateTotalTemplates(ArrayList<Entry> list) {
        int result = 0;
        Iterator<Entry> listIterator = list.iterator();
        while (listIterator.hasNext()) {
            Entry tempEntry = listIterator.next();
            String sub0 = tempEntry.message.substring(0,3);
            
            if(sub0.equals("Num")) {
                String sub1 =  tempEntry.message.substring(tempEntry.message.indexOf(",")+2);
                String sub2 =  sub1.substring(sub1.indexOf(": ")+2,sub1.indexOf(","));
                result = result + Integer.parseInt(sub2);
            }
        }
        return result;
        
    }
    public static int calculateTotalBene(ArrayList<Entry> list) {
        int result = 0;
        Iterator<Entry> listIterator = list.iterator();
        while (listIterator.hasNext()) {
            Entry tempEntry = listIterator.next();
            String sub0 = tempEntry.message.substring(0,3);
            
            if(sub0.equals("Num")) {
                String sub1 =  tempEntry.message.substring(tempEntry.message.indexOf(",")+2);
                String sub2 =  sub1.substring(sub1.indexOf(",")+2);
                String sub3 =  sub2.substring(sub2.indexOf(": ")+2,sub2.indexOf(","));
                result = result + Integer.parseInt(sub3);
            }
        }
        return result;
        
    }
}
