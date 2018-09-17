package doss.dash;

import static spark.Spark.*;
import spark.template.velocity.*;
import spark.ModelAndView;
import java.io.StringWriter;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import java.util.*;
import java.text.*;
import doss.dash.Database;
import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.*;



public class Dash {

    private static final long HISTORY = 7L;
    private int calObject = Calendar.DAY_OF_YEAR;
    private HashMap<Long,Long> containerArchiveTimes = new HashMap<>();
    private HashMap<Long,Long> blobsPerContainer = new HashMap<>();
    private HashMap<Long,Long> auditTimes = new HashMap<>();
    private HashMap<Long,Long> containerSizes = new HashMap<>();
    private HashMap<Long,Long> totalFy = new HashMap<>();

    public ModelAndView report() {
        Map<String, Object> model = new HashMap<>();
        return new ModelAndView(model,"report.wm");
    }

    public ModelAndView index() {
        Map<String, Object> model = new HashMap<>();
        DecimalFormat numFormat = new DecimalFormat("###,###,###,###,###,###");
        model.put("testobj", Database.DOSS_HOME);
        try (Connection db = Database.open()) {
            Statement st = db.createStatement();

            ResultSet audits = st.executeQuery("SELECT count(container_id) as b from digest_audits;");
            if (audits.next()) {
                model.put("auditCount",numFormat.format(audits.getInt(1)));
            }
            ResultSet legacyAudits = st.executeQuery("SELECT count(blob_id) as b from legacy_digest_audits;");
            if (legacyAudits.next()) {
                model.put("legacyAuditCount",numFormat.format(legacyAudits.getInt(1)));
            }
            ResultSet size = st.executeQuery("SELECT sum(size) from containers;");
            if (size.next()) {
                model.put("totalSize",numFormat.format(size.getLong(1)/1024/1024/1024));
            }
            ResultSet containers = st.executeQuery("SELECT count(container_id) as bc from containers where state = 3;");
            if (containers.next()) {
                model.put("containerCount",numFormat.format(containers.getInt(1)));
            }
            ResultSet blobs = st.executeQuery("SELECT count(blob_id) as bc from blobs;");
            if (blobs.next()) {
                model.put("blobCount",numFormat.format(blobs.getInt(1)));
            }
            ResultSet legacyPaths = st.executeQuery("SELECT count(blob_id) as lpc from blobs where legacy_path is not NULL;");
            if (legacyPaths.next()) {
                model.put("legacyPathCount",numFormat.format(legacyPaths.getInt(1)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return new ModelAndView(model,"index.wm");
     }

    public String getReportData(String fy, String graph) {
        debugInfo();
        populateContainerArchiveTimes();
        populateContainerSizes();
        if (graph.equals("collectionsize")) {
            getContainerSizesPerDay(containerArchiveTimes);
            List<Long> fyEvents = new ArrayList(totalFy.keySet());
            Collections.sort(fyEvents);
            long collectionTotal = 0;
            for (long event : fyEvents) {
                log("Adding FY Totals " + totalFy.get(event) + " to total " + collectionTotal);
                collectionTotal = collectionTotal + totalFy.get(event);
                totalFy.put(event,collectionTotal);
            }
            return buildJsonData("collectionsize",totalFy);
        }
        HashMap<Long,Long> data = new HashMap<Long,Long>();
        HashMap<Long,Long> tempdata = new HashMap<Long,Long>();
        String[] years = fy.split("/");
        int sYear = Integer.parseInt(years[0]) + 2000;
        int eYear = Integer.parseInt(years[1]) + 2000;

        java.util.Calendar cfy = Calendar.getInstance();
        //month.setTime(new Date(wantMonth));
        cfy.set(Calendar.HOUR_OF_DAY,0);
        cfy.set(Calendar.MINUTE,0);
        cfy.set(Calendar.SECOND,0);
        cfy.set(Calendar.MILLISECOND,0);
        cfy.set(Calendar.DAY_OF_MONTH,1);
        cfy.set(Calendar.YEAR,sYear);
        long fyStartTime = cfy.getTimeInMillis();
        for(int i=6;i<12;i++) {
            cfy.set(Calendar.MONTH,i);
            long monthStartTime = cfy.getTimeInMillis();
            log("FY " + fy + " : " + sYear + "-" + i + " : milli: " + monthStartTime);
            data.putAll(getContainerSizesForMonth(cfy,containerArchiveTimes));
        }
        cfy.set(Calendar.YEAR,eYear);
        for(int i=0;i<6;i++) {
            cfy.set(Calendar.MONTH,i);
            long monthStartTime = cfy.getTimeInMillis();
            log("FY " + fy + " : " + eYear + "-" + i + " : milli: " + monthStartTime);
            data.putAll(getContainerSizesForMonth(cfy,containerArchiveTimes));
        }

        List<Long> keys = new ArrayList(data.keySet());
        return buildJsonData("containerhistory",data);
    }
    public String json(String graph) {
        debugInfo();
        populateContainerArchiveTimes();
        return getJsonData(graph);
    }

    private String getJsonData(String g) {
        if (g.equals("todaysblobs")) {
            return getTodaysBlobs();
        } else if (g.equals("blobhistory")) {
            return  getBlobHistory();
        } else if (g.equals("containerhistory")) {
            return  getContainerHistory();
        } else if (g.equals("auditcontainers")) {
            return getAuditContainers();
        } else if (g.equals("auditblobs")) {
            return getAuditBlobs();
        }
        return "{\"label\": \"unknown graph\",\"data\": [],\"color\": \"#FFFFFF\"}";
    }

    private String buildJsonData(String label, HashMap<Long,Long> map) {
        log("Start buildJsonData");
        StringBuilder counts = new StringBuilder();
        StringJoiner countsArray = new StringJoiner(",");
        counts.append("{\"label\": \"" + label + "\", \"data\": [");
        List<Long> events = new ArrayList(map.keySet());
        Collections.sort(events);
        for (long event : events) {
            StringJoiner countEntry = new StringJoiner(",","[","]");
            long value = map.get(event);
            if (value <1) {
                value = 0 ;
            }
            countEntry.add(String.valueOf(event));
            countEntry.add(String.valueOf(value));
            countsArray.add(countEntry.toString());
            //log("buildJsonData: Adding event: "+event+" value: "+value);
        }
        counts.append(countsArray.toString());
        counts.append("], \"color\": null}");
        return counts.toString();
    }



    private String getTodaysBlobs() {
        populateBlobsPerContainer();
        return buildJsonData("todaysblobs",getBlobsPerContainer(getRecent(containerArchiveTimes,12L,Calendar.HOUR_OF_DAY)));
    }
    private String getBlobHistory() {
        populateBlobsPerContainer();
        return buildJsonData("blobhistory",getBlobsPerContainer(getRecent(containerArchiveTimes,HISTORY,calObject)));
    }
    private String getContainerHistory() {
        populateContainerSizes();
        return buildJsonData("containerhistory",getContainerSizesPerDay(getRecent(containerArchiveTimes,HISTORY,calObject)));
    }
    private String getAuditBlobs() {
        populateAuditTimes();
        populateBlobsPerContainer();
        return buildJsonData("auditblobs",getBlobsPerContainer(getRecent(auditTimes,HISTORY,calObject)));
    }
    private String getAuditContainers() {
        populateAuditTimes();
        return buildJsonData("auditcontainers",getContainersPerDay(getRecent(auditTimes,HISTORY,calObject)));
    }


    private void populateAuditTimes() {
        this.auditTimes = getAuditTimes();
    }
    private void populateBlobsPerContainer() {
        this.blobsPerContainer = getAllBlobsPerContainer();
    }
    private void populateContainerArchiveTimes() {
        //this.containerArchiveTimes = getAllTarContainers("/doss/display/data");
        this.containerArchiveTimes = getAllTarContainers(true);
    }
    private void populateContainerSizes() {
        this.containerSizes = getAllContainerSizes();
    }

        
    private HashMap getContainerSizesForMonth(Calendar wantMonth, HashMap<Long,Long> map) {
        Date timerStart = new Date();
        long monthStartTime = wantMonth.getTimeInMillis();
        java.util.Calendar wantMonthEnd = wantMonth;
        wantMonthEnd.set(Calendar.MONTH,wantMonth.get(Calendar.MONTH)+1);
        long monthEndTime = wantMonthEnd.getTimeInMillis();
        java.util.Calendar cal = Calendar.getInstance();
        HashMap<Long,Long> cS = new HashMap<>();
        long totalMonth = 0;
        for ( Map.Entry<Long,Long> entry : map.entrySet()) {
            cal.setTime(new Date(entry.getKey()));
            long eventTime = cal.getTimeInMillis();
            long cid = entry.getValue();
            long s = 0;
            if (containerSizes.containsKey(cid)) {
                s = containerSizes.get(cid)/1024/1024/1024;
            }
            //log(monthStartTime + " < " + eventTime + " > " + monthEndTime);
            if ( (eventTime >= monthStartTime) && (eventTime < monthEndTime) ) {
              if (cS.containsKey(monthStartTime)) {
                s += cS.get(monthStartTime);
                totalMonth += s;
              }
              cS.put(monthStartTime,s);
            }
              log("Added " + totalMonth + " to " + monthStartTime);
        }
        long elap = new Date().getTime() - timerStart.getTime();
        log("getContainersSizesForMonth took " + elap);
        return cS;
    }

    private HashMap getContainerSizesPerDay(HashMap<Long,Long> map) {
        Date timerStart = new Date();
        java.util.Calendar cal = Calendar.getInstance();
        java.util.Calendar fyCal = Calendar.getInstance();
        HashMap<Long,Long> cS = new HashMap<>();
        for ( Map.Entry<Long,Long> entry : map.entrySet()) {
            cal.setTime(new Date(entry.getKey()));
            cal.set(Calendar.HOUR_OF_DAY,0);
            cal.set(Calendar.MINUTE,0);
            cal.set(Calendar.SECOND,0);
            cal.set(Calendar.MILLISECOND,0);
            long eventTime = cal.getTimeInMillis();

            fyCal.setTime(new Date(entry.getKey()));
            fyCal.set(Calendar.MONTH,Calendar.JULY);
            fyCal.set(Calendar.DAY_OF_MONTH,1);
            fyCal.set(Calendar.HOUR_OF_DAY,0);
            fyCal.set(Calendar.MINUTE,0);
            fyCal.set(Calendar.SECOND,0);
            fyCal.set(Calendar.MILLISECOND,0);
            if (fyCal.get(Calendar.MONTH) < 6) {
                fyCal.add(Calendar.YEAR, -1);
            }
            long fy = fyCal.getTimeInMillis();
            long cid = entry.getValue();
            long s = 0;
            if (containerSizes.containsKey(cid)) {
                s = containerSizes.get(cid)/1024/1024/1024;
            }
            log("Incrementing FY " + fyCal.get(Calendar.YEAR) + " by " + s + " for container " + cid);
            if (cS.containsKey(eventTime)) {
                cS.put(eventTime,s + cS.get(eventTime));
            } else {
                cS.put(eventTime,s);
            }
            if (totalFy.containsKey(fy)) {
                totalFy.put(fy,s + totalFy.get(fy));
            } else {
                totalFy.put(fy,s);
            }
        }
        long elap = new Date().getTime() - timerStart.getTime();
        log("getContainersSizesPerDay took " + elap);
        return cS;
    }

    private HashMap getContainersPerDay(HashMap<Long,Long> map) {
        Date s = new Date();
        java.util.Calendar cal = Calendar.getInstance();
        HashMap<Long,Long> cPD = new HashMap<>();
        for ( Map.Entry<Long,Long> entry : map.entrySet()) {
            cal.setTime(new Date(entry.getKey()));
            cal.set(Calendar.HOUR_OF_DAY,0);
            cal.set(Calendar.MINUTE,0);
            cal.set(Calendar.SECOND,0);
            cal.set(Calendar.MILLISECOND,0);
            long eventTime = cal.getTimeInMillis();
            //log("cPD Event: "+eventTime +" value: "+entry.getValue());
            if (cPD.get(eventTime) != null) {
                    cPD.put(eventTime,cPD.get(eventTime) + 1);
            } else {
                    cPD.put(eventTime,1L);
            }
        }
        long elap =new Date().getTime() - s.getTime();
        log("getContainersPerDay took " + elap);
        return cPD;
    }

    private HashMap getRecent(HashMap<Long,Long> map, long age,int calObject) {
        Date s = new Date();
        java.util.Calendar cutOffCal = Calendar.getInstance();
        cutOffCal.add(calObject,-(int)age);
        long cutOff = cutOffCal.getTimeInMillis();
        log("Getting objects < "+age+" old");
        HashMap<Long,Long> newMap = new HashMap<>();
        for ( Map.Entry<Long,Long> entry : map.entrySet()) {
            long eventTime = entry.getKey();
            long val = entry.getValue();
            //log("getRecent: checking eventTime: "+eventTime+" cutoff: "+cutOff +" "+age);
            if (eventTime > cutOff) {
                //log("getRecent: Adding Recent key: "+ eventTime + " val "+ val);
                newMap.put(eventTime,val);
            }
        }
        long elap =new Date().getTime() - s.getTime();
        log("getRecent took " + elap);
        return addMissing(newMap,age,calObject);
    }

    private HashMap addMissing(HashMap<Long,Long> map, long age, int calObject) {
        java.util.Calendar timerStart = Calendar.getInstance();
        java.util.Calendar pointCal = Calendar.getInstance();
        java.util.Calendar eventCal = Calendar.getInstance();
        pointCal.set(Calendar.MINUTE,0);
        pointCal.set(Calendar.SECOND,0);
        pointCal.set(Calendar.MILLISECOND,0);
        pointCal.add(calObject,-(int)age);
        HashMap<Long,Long> tmpMap = new HashMap<>();
        for (long point = age; point >0; point--) {
            pointCal.add(calObject,1);
            boolean found = false;
            for ( Map.Entry<Long,Long> entry : map.entrySet()) {
                long eventTime = entry.getKey();
                eventCal.setTime(new Date(eventTime));
                if (pointCal.get(calObject) == eventCal.get(calObject)) {
                    found=true;
                    //log("plot points match "+ pointCal.get(calObject) + " vs event "+eventCal.get(calObject) + " adding val "+entry.getValue());
                    tmpMap.put(eventCal.getTimeInMillis(),entry.getValue());
                }
            }
            if (!found) {
                log("no data for plottime " +pointCal.get(calObject) + " adding val 0");
                tmpMap.put(pointCal.getTimeInMillis(),0L);
            }
        }
        java.util.Calendar timerEnd = Calendar.getInstance();
        long elap =timerEnd.getTimeInMillis() - timerStart.getTimeInMillis();
        log("addMissing took " + elap);
        return tmpMap;
    }

    private HashMap getBlobsPerContainer(HashMap<Long,Long> map) {
        Date s = new Date();
        HashMap<Long,Long> newMap = new HashMap<>();
        for ( Map.Entry<Long,Long> entry : map.entrySet()) {
            long eventTime = entry.getKey();
            long cid = entry.getValue();
            long count = 0;
            if ((blobsPerContainer.containsKey(cid)) && (cid >0)) {
                count = blobsPerContainer.get(cid);
            } else {
            log("Why no blobs for container "+cid);
            }
            newMap.put(eventTime,count);
        }
        long elap =new Date().getTime() - s.getTime();
        log("getBlobsPerContainer took " + elap);
        return newMap;
    }

    private HashMap getAuditTimes() {
        Date s = new Date();
        java.util.Calendar cal = Calendar.getInstance();
        HashMap<Long,Long> aTs = new HashMap<>();
        try (Connection db = Database.open()) {
            String SQL="SELECT time,container_id from digest_audits where cast(time as date) >= CURRENT_DATE() - "+ HISTORY +" order by time;";
            log("getAuditTimes: SQL: "+SQL);
            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery(SQL);
            while (rs.next()) {
                aTs.put(rs.getTimestamp(1,cal).getTime(), rs.getLong(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long elap =new Date().getTime() - s.getTime();
        log("getAuditTimes took " + elap);
        return aTs;
    }

    private HashMap getAllContainerSizes() {
        Date s = new Date();
        HashMap<Long,Long> aCS = new HashMap<>();
        String SQL = "SELECT container_id,size from containers where size >0 and container_id";

        log("SQL: "+SQL);
        try (Connection db = Database.open()) {
            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery(SQL);
            while (rs.next()) {
                aCS.put(rs.getLong(1), rs.getLong(2));
                log("ContainerSizes: " + rs.getLong(1) + " " + rs.getLong(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long elap =new Date().getTime() - s.getTime();
        log("getAllContainerSizes took " + elap);
        return aCS;
    }

    private HashMap getAllBlobsPerContainer() {
        Date s = new Date();
        HashMap<Long,Long> bPC = new HashMap<>();
        //StringJoiner containers = new StringJoiner(",");
        //for ( Map.Entry<Long,Long> entry : containerArchiveTimes.entrySet()) {
        //    containers.add(String.valueOf(entry.getValue()));
        //}
        //for ( Map.Entry<Long,Long> entry : auditTimes.entrySet()) {
        //    containers.add(String.valueOf(entry.getValue()));
       // }
        //String SQL = "SELECT container_id,count(blob_id) from blobs where container_id IN ("+containers.toString()+") group by container_id";
        String SQL = "SELECT container_id,count(blob_id) from blobs group by container_id";
        //log("SQL: "+SQL);
        try (Connection db = Database.open()) {
            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery(SQL);
            while (rs.next()) {
                bPC.put(rs.getLong(1), rs.getLong(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long elap =new Date().getTime() - s.getTime();
        log("getAllBlobsPerContainer took " + elap);
        return bPC;
    }

    // remove when db updated

    private HashMap getAllTarContainers(boolean all) {
        Date s = new Date();
        HashMap<Long,Long> m = new HashMap<>();
        //String SQL = "SELECT count(container_id),cast(time as date) as t from container_history where state = '3' and  time >= CURRENT_TIMESTAMP() -" + HISTORY +" group by t order by t asc;";
        String SQL = null;
        if (all) {
            SQL = "SELECT container_id,CAST(UNIX_TIMESTAMP(time) AS INT) from container_history where state = '3' order by time asc;";
        } else {
            SQL = "SELECT container_id,CAST(UNIX_TIMESTAMP(time) AS INT) from container_history where state = '3' and cas(time as date) >= CURRENT_DATEP() -" + HISTORY +"  order by time asc;";
        }
        log("SQL: "+SQL);
        try (Connection db = Database.open()) {
            Statement st = db.createStatement();
            ResultSet rs = st.executeQuery(SQL);
            while (rs.next()) {
                m.put(rs.getLong(2)*1000,rs.getLong(1));
               //log("getAllTarContainers:  found " + rs.getLong(2)*1000 + " - " + rs.getLong(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long elap =new Date().getTime() - s.getTime();
        log("getAllTarContainers took " + elap);
        return m;
    }

    private HashMap getAllTarContainersold(String fsRoot) {
        Date s = new Date();
        HashMap<Long,Long> m = new HashMap<>();
        Collection<File> files = FileUtils.listFiles(new File(fsRoot), new RegexFileFilter("(nla.doss-.*.tar)"),
            DirectoryFileFilter.DIRECTORY);
        for (File f : files) {
            long mtime = f.lastModified();
            if (mtime < System.currentTimeMillis() - 86400000L * HISTORY) {
                continue;
            }
            long cid = Long.parseLong(f.toString().substring(f.toString().lastIndexOf("-") + 1, f.toString().indexOf(".tar")));
            m.put(mtime,cid);
        log("getAllTarContainers:  found " + f.toString() + " - " + mtime);
        }
        long elap =new Date().getTime() - s.getTime();
        log("getAllTarContainers took " + elap);
        return m;
    }

    private void debugInfo() {
        Date now = new Date();
        long epoch = now.getTime();
        log("Date is " + now + " : " + epoch);
    }

    private void log(String l) {
        String debugLevel = System.getProperty("doss.debug");
        if (debugLevel != null) {
            System.out.println(l);
        }
    }
    private long getMidnightLastnight() {
        java.util.Calendar yesterdayCal = Calendar.getInstance();
        //yesterdayCal.setTimeZone(TimeZone.getTimeZone("UTC"));
        yesterdayCal.set(Calendar.HOUR_OF_DAY,0);
        yesterdayCal.set(Calendar.MINUTE,0);
        yesterdayCal.set(Calendar.SECOND,0);
        log("Midnight lastnight: " + yesterdayCal.getTimeInMillis());
        return yesterdayCal.getTimeInMillis();
    }
 }
