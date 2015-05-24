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
	private HashMap<Long,Long> containerArchiveTimes = new HashMap<>();
	private HashMap<Long,Long> blobsPerContainer = new HashMap<>();
	private HashMap<Long,Long> auditTimes = new HashMap<>();


    public ModelAndView index() {
		Map<String, Object> model = new HashMap<>();
		DecimalFormat numFormat = new DecimalFormat("###,###,###,###,###,###");
        model.put("testobj", Database.DOSS_HOME);
		try (Connection db = Database.open()) {
            Statement st = db.createStatement();

			ResultSet audits = st.executeQuery("SELECT count(container_id) as b from digest_audits;");
			if (audits.next()) {
				model.put("auditCount",audits.getInt(1));
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new ModelAndView(model,"index.wm");
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
		return buildJsonData("todaysblobs",addMissing(getRecent(containerArchiveTimes,0L),Calendar.HOUR_OF_DAY));
	}
	private String getBlobHistory() {
		populateBlobsPerContainer();
		return buildJsonData("blobhistory",getBlobsPerContainer(addMissing(getRecent(containerArchiveTimes,HISTORY),Calendar.DAY_OF_YEAR)));
	}
	private String getContainerHistory() {
		return buildJsonData("containerhistory",addMissing(getContainersPerDay(getRecent(containerArchiveTimes,HISTORY)),Calendar.DAY_OF_YEAR));
	}
	private String getAuditBlobs() {
		populateAuditTimes();
		populateBlobsPerContainer();
		return buildJsonData("auditblobs",addMissing(getBlobsPerContainer(getRecent(auditTimes,HISTORY)),Calendar.DAY_OF_YEAR));
	}
	private String getAuditContainers() {
		populateAuditTimes();
		return buildJsonData("auditcontainers",addMissing(getContainersPerDay(getRecent(auditTimes,HISTORY)),Calendar.DAY_OF_YEAR));
	}


	private void populateAuditTimes() {
		this.auditTimes = getAuditTimes();
	}
	private void populateBlobsPerContainer() {
		this.blobsPerContainer = getAllBlobsPerContainer();
	}
	private void populateContainerArchiveTimes() {
		this.containerArchiveTimes = getAllTarContainers("/doss/display/data");
	}

	private HashMap getContainersPerDay(HashMap<Long,Long> map) {
		Date s = new Date();
		java.util.Calendar cal = Calendar.getInstance();
		//cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		HashMap<Long,Long> cPD = new HashMap<>();
		for ( Map.Entry<Long,Long> entry : map.entrySet()) {
			cal.setTime(new Date(entry.getKey()));
			cal.set(Calendar.MINUTE,0);
			cal.set(Calendar.SECOND,0);
			cal.set(Calendar.MILLISECOND,0);
			long eventHour = cal.get(Calendar.HOUR_OF_DAY);
			long eventTime = cal.getTimeInMillis();
			//log("cPD Event: "+eventTime +" value: "+entry.getValue());
			if (cPD.get(eventTime) != null) {
					cPD.put(eventTime,cPD.get(eventTime) + 1);
			} else {
					cPD.put(eventTime,1L);
			}
			//log("cPD set Increment: "+eventTime + " "+cPD.get(eventTime));
		}
		long elap =new Date().getTime() - s.getTime();
		log("getContainersPerDay took " + elap);
		return cPD;
	}

	private HashMap getRecent(HashMap<Long,Long> map, Long age) {
		Date s = new Date();
		log("Getting objects < "+age+" days old");
		HashMap<Long,Long> newMap = new HashMap<>();
		for ( Map.Entry<Long,Long> entry : map.entrySet()) {
			long eventTime = entry.getKey();
			long val = entry.getValue();
			long nowTime = System.currentTimeMillis();
			long cutOff = nowTime - (86400000L * age);
			//log("getRecent: eventTime: "+eventTime+" cutoff: "+cutOff +" ("+nowTime+") "+age);
			if (eventTime > cutOff) {
			//log("getRecent: Adding  key: "+ eventTime + " val "+ val);
		        newMap.put(eventTime,val);
			}
		}
		long elap =new Date().getTime() - s.getTime();
		log("getRecent took " + elap);
		return newMap;
	}

	private HashMap addMissing(HashMap<Long,Long> map, int calObject) {
		Date s = new Date();
		HashMap<Long,Long> newMap = new HashMap<>();
		long gap = 86400;
		if (calObject == Calendar.HOUR_OF_DAY) {
			gap = 3600;
		}
		List<Long> checkKeys = new ArrayList(map.keySet());
		Collections.sort(checkKeys);
		if (checkKeys.size() <1) {
			HashMap<Long,Long> tmpMap = new HashMap<>();
			if (calObject == Calendar.HOUR_OF_DAY) {
				java.util.Calendar nowCal = Calendar.getInstance();
				//nowCal.setTimeZone(TimeZone.getTimeZone("UTC"));
				nowCal.setTime(new Date());
				java.util.Calendar cal = Calendar.getInstance();
				//cal.setTimeZone(TimeZone.getTimeZone("UTC"));
				cal.setTime(new Date());
				cal.set(Calendar.HOUR_OF_DAY,0);
				cal.set(Calendar.MINUTE,0);
				cal.set(Calendar.SECOND,0);
				log("Today Data empty, adding blanks " +nowCal.get(Calendar.HOUR_OF_DAY) +" "+nowCal.get(Calendar.HOUR));
				for (int h=0;h<=nowCal.get(Calendar.HOUR_OF_DAY);h++) {
					cal.add(calObject,1);
				log("adding blanks " +cal.getTime());
					tmpMap.put(cal.getTimeInMillis(),0L);
				}
				return tmpMap;
			} else {
			log("Data empty, adding blank");
				java.util.Calendar cal = Calendar.getInstance();
				//cal.setTimeZone(TimeZone.getTimeZone("UTC"));
					tmpMap.put(cal.getTimeInMillis(),0L);
				return tmpMap;
			}
		}
		long currTime = System.currentTimeMillis();
		long lastTime = checkKeys.get(checkKeys.size()-1);
		long nowDiff = currTime - lastTime;

		Date lastDate = new Date(lastTime);
		log("LAST date is " + lastDate + " : " + lastTime);
		if (nowDiff >(3600*1000)) {
		log("Adding end padd for diff " +nowDiff + " of " + currTime + " - " + lastTime);
			java.util.Calendar yesterdayCal = Calendar.getInstance();
			//yesterdayCal.setTimeZone(TimeZone.getTimeZone("UTC"));
			yesterdayCal.set(Calendar.HOUR,0);
			yesterdayCal.set(Calendar.MINUTE,0);
			yesterdayCal.set(Calendar.SECOND,0);
			yesterdayCal.add(calObject,-1);
			//map.put(yesterdayCal.getTimeInMillis(),0L);
			map.put(currTime,0L);
		}
		List<Long> keys = new ArrayList(map.keySet());
		Collections.sort(keys);
		for (long key : keys) {
			newMap.put(key,map.get(key));
			int idx = keys.indexOf(key);
			if (idx <0 || idx+1 == keys.size()) {
				continue;
			}
			long nextKey = keys.get(idx + 1);
			long diff = (nextKey - key) /(1000 * gap);
			//log("diff for " + nextKey + " - " + key+ " is " + diff);
			if (diff >1) {
				int j=0;
				for (j=0; j<(diff - 1); j++) {
					java.util.Calendar newcal = Calendar.getInstance();
					//newcal.setTimeZone(TimeZone.getTimeZone("UTC"));
					newcal.setTime(new Date(key));
					newcal.set(Calendar.HOUR_OF_DAY,0);
					newcal.set(Calendar.MINUTE,0);
					newcal.set(Calendar.SECOND,0);
					newcal.add(calObject,j+1);
					newMap.put(newcal.getTimeInMillis(),0L);
					//log("Added a blank " + j +" " +newcal.getTimeInMillis());
				}
			}
		//log("addMissing passing on "+ key + " : " + map.get(key));
		}
		long elap =new Date().getTime() - s.getTime();
		log("addMissing took " + elap);
        return newMap;
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
		//cal.setTimeZone(TimeZone.getTimeZone("UTC"));
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

	private HashMap getAllBlobsPerContainer() {
		Date s = new Date();
		HashMap<Long,Long> bPC = new HashMap<>();
		StringJoiner containers = new StringJoiner(",");
		for ( Map.Entry<Long,Long> entry : containerArchiveTimes.entrySet()) {
			containers.add(String.valueOf(entry.getValue()));
		}
		for ( Map.Entry<Long,Long> entry : auditTimes.entrySet()) {
			containers.add(String.valueOf(entry.getValue()));
		}
		String SQL = "SELECT container_id,count(blob_id) from blobs where container_id IN ("+containers.toString()+") group by container_id";

	log("SQL: "+SQL);
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
	//String SQL = "SELECT count(container_id),cast(time as date) as t from container_history where state = '3' and
	//  time >= CURRENT_TIMESTAMP() -" + HISTORY +" group by t order by t asc;";
	private HashMap getAllTarContainers(String fsRoot) {
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
}
