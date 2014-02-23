package code.simplyhired.smart_test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.simplyhired.config.Config;

public class Allocator {
	private static final Logger logger = Logger.getLogger("marketplaces");
	
	static Connection lookerConn = null;
	static Connection inClickConn = null;
	static Connection inClickTargetConn = null;
	static Connection statsDbConn = null;
	
	
	public static void main(String[] args) {
		init(args);
		
		Set<Long> advertiserIds = getValidAdvertiserIds();
		if (advertiserIds.size() == 0) {
			logger.error("Empty advertiserIds. Skipping further processing");
			return;
		}
		Set<Long> campaignIds = getCampaignIds(advertiserIds);
		scenarioZero(campaignIds);
		scenarioOne(campaignIds);
		scenarioTwo(campaignIds);
		scenarioThree(campaignIds);
		scenarioFour(campaignIds);
		scenarioFourExt(campaignIds);
		scenarioFive(campaignIds);
		scenarioSix(campaignIds);
		scenarioSeven(campaignIds);
		scenarioEight(campaignIds);
		scenarioNine(campaignIds);
		thirdFeed(campaignIds);		
		excludeCampaignsByClients();
		includeCampaignsByClients(campaignIds);
		updateFeedID();
		updateActiveFlag(2);
		updateActiveFlag(3);
		updateActiveFlag(4);
		criteoCamp(campaignIds);
		updateCriteoFeedID();
		updateCriteoActiveFlag(5);
	}
	
	public static Set<Long> getValidAdvertiserIds() {
		Set<Long>  advertiserIds = new HashSet<Long>();
		
		String vertSql = "SELECT advertiser_id FROM metrics_budget_utilization.AdvertiserVertical " + 
		            	  " WHERE type NOT IN ('NA', 'OTHER') " + 
		                  " AND NOT (type = 'Job Board' AND raa_name = '')" +
		                  " AND type != ''";
		try {
			Statement stmt = lookerConn.createStatement();
			ResultSet rs = stmt.executeQuery(vertSql);
			while (rs.next()) {
				long advertiserId = rs.getLong("advertiser_id");
				advertiserIds.add(advertiserId);
			}
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
		return advertiserIds;
	}
	
	public static Set<Long> getCampaignIds(Set<Long> advertiserIds) {
		Set<Long>  campaignIds = new HashSet<Long>();
		
		String campSql = "select distinct camp_id " +
						 "from inclick_campaigns ic " +
						 "join  inclick_user iu on ic.user_id = iu.id " +
						 "where ic.camp_type = 'pjl' " +
					     " and ic.status = 'yes' " +
					     " and ic.camp_start <= curdate() " +
					     " and ic.camp_end >= curdate() " +
					     " and iu.status = 1 " +
					     " and iu.company not like '%Simply Post ATS%'" +
					     " and iu.product != 'selfserve' " +
					     " and iu.country != 'ca' " +
					     " and iu.total_credits - iu.total_debits > 0 " +
					     " and ic.user_id in (" + StringUtils.join(advertiserIds, ',') + ")";
		try {
			Statement stmt = inClickConn.createStatement();
			ResultSet rs = stmt.executeQuery(campSql);
			while (rs.next()) {
				long campId = rs.getLong("camp_id");
				campaignIds.add(campId);
			}
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
		return campaignIds;
	}
	
	
	public static void insertOptimizationEntries(Connection conn, String sql, int scenarioNum, Set<String> exclusionSet) {
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			int resultCount = 0;
			while (rs.next()) { //TODO : Explore possibility of buffering
				if (exclusionSet != null) {
					long pubId = rs.getLong("publisher_id");
					double cpc = rs.getDouble("publisher_cpc");
					long campId = rs.getLong("camp_id");
					String key = new StringBuilder(String.valueOf(pubId)).append(String.valueOf(cpc)).append(String.valueOf(campId)).toString();
					if (exclusionSet.contains(key)) {
						continue;
					}
				}
				PreparedStatement inStmt =  inClickTargetConn.prepareStatement("INSERT INTO `opt_target_spend`(`date_date`,`publisher_id`, `publisher_cpc`, `camp_id`, `target_spend`, `stepNum`) VALUES (?, ?, ?, ?, ?,?)");
				inStmt.setDate(1, rs.getDate("date_to"));
				inStmt.setLong(2, rs.getLong("publisher_id"));
				inStmt.setDouble(3, rs.getDouble("publisher_cpc"));
				inStmt.setLong(4, rs.getLong("camp_id"));
				inStmt.setDouble(5, rs.getDouble("target_spend"));
				inStmt.setInt(6, scenarioNum);
				inStmt.executeUpdate();
				inStmt.close();
				resultCount++;
			}
			logger.info("Scenario " + scenarioNum  +": " + resultCount);
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
	}
	
	public static void insertOptimizationEntries(String sql, int scenarioNum) {
		insertOptimizationEntries(inClickConn, sql, scenarioNum, null);
	}
	
	public static void insertOptimizationEntries(Connection conn, String sql, int scenarioNum) {
		insertOptimizationEntries(conn, sql, scenarioNum, null);
	}
	
	public static void insertOptimizationEntries(String sql, int scenarioNum, Set<String> exclusionSet) {
		insertOptimizationEntries(inClickConn, sql, scenarioNum, exclusionSet);
	}
	
	public static Set<String> getExistingCampaignIds() { //TODO: Rename this after verifying it with Yiping
		
		Set<String>  exitCampaignIds = new HashSet<String>();
		

		String campSql = "select distinct publisher_id, publisher_cpc, camp_id from opt_target_spend where date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY)";
		try {
			Statement stmt = inClickTargetConn.createStatement();
			ResultSet rs = stmt.executeQuery(campSql);
			while (rs.next()) {
				long pubId = rs.getLong("publisher_id");
				double cpc = rs.getDouble("publisher_cpc");
				long campId = rs.getLong("camp_id");
				exitCampaignIds.add(new StringBuilder(String.valueOf(pubId)).append(String.valueOf(cpc)).append(String.valueOf(campId)).toString()) ;
			}
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
	
		return exitCampaignIds;
	}
	
	public static void scenarioZero(Set<Long> campaignIds) {				
		String sql = " select date_to, publisher_id, publisher_cpc, camp_id, case when target_spend > 100 then 100 else round(target_spend, 2) end as target_spend " +
				     " from ( " +
				     " select distinct DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) date_to, ic.user_id, 30647 as publisher_id, 0.11 as publisher_cpc, ic.camp_id, iu.account_system_daily_budget, ic.system_daily_budget, ic.cost_today, ic.cost_per_click, " +
				     "        case when ic.cost_per_click > 0 then (ic.system_daily_budget - ic.cost_today) / ic.cost_per_click * 0.11  " +
				     "             when ic.cost_per_click = 0 then (ic.system_daily_budget - ic.cost_today) * 0.40 " +
				     "        end  target_spend, " +
				     "        case when ic.cost_per_click > 0 then floor((ic.system_daily_budget - ic.cost_today) / ic.cost_per_click)   " +
				     "             when ic.cost_per_click = 0 then floor((ic.system_daily_budget - ic.cost_today) * 0.40 / 0.11) " +
				     "        end  target_click " +
				     " from inclick_campaigns ic " +
				     " join inclick_user iu on iu.id = ic.user_id " +
				     " where ic.status = 'yes' and camp_type = 'pjl' and ic.cost_per_click > 0.18 and ic.system_daily_budget-3 >= ic.cost_today and ic.cost_today = 0 " +
				     " ) sz " +
					 " where sz.camp_id in ( " + StringUtils.join(campaignIds, ',')  +" ) and sz.target_spend > 0.5 ";
				
		insertOptimizationEntries(sql, 0);
	}
	
	
	public static void scenarioOne(Set<Long> campaignIds) {

                Set<String> excludedCampaigns = getExistingCampaignIds();
				
		String sql =  " select DATE_ADD(a.date_date, INTERVAL 1 DAY) as date_to, 30647 as publisher_id, 0.11 as publisher_cpc, a.camp_id as camp_id, 0 as target_spend " +
				      " from  " +
				      " ( " +
				      " select iat.date_date, iat.camp_id as camp_id, " +
				      "        sum(if(iat.partner_source=30647, if(iat.reason_code = 99, iat.click_cost, 0),0)) as zpc_rev, " +
				      "        ic.system_daily_budget, " +
				      "        ic.cost_today, " +
				      "        ic.cost_per_click        " +
				      " from inclick_ads_tracking iat  " +
				      " join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
				      " where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.18 and ic.system_daily_budget > 0 and ic.cost_today <= ic.system_daily_budget and ic.system_daily_budget-3 < ic.cost_today " +
				      " group by iat.date_date, iat.camp_id, ic.system_daily_budget, ic.cost_today, ic.cost_per_click " +
				      " having zpc_rev = 0 " +
				      " ) a " +
					  " where a.camp_id in (" +  StringUtils.join(campaignIds, ',') +  ")" ;
		insertOptimizationEntries(sql, 1, excludedCampaigns);
		
	}
	
	public static void scenarioTwo(Set<Long> campaignIds) {	
              
                Set<String> excludedCampaigns = getExistingCampaignIds(); 
			
		String sql =    "select DATE_ADD(b.date_date, INTERVAL 1 DAY) as date_to, 30647 as publisher_id, b.zcpc as publisher_cpc, b.camp_id as camp_id, "+
						" case when b.zpc_paid_clicks * b.zcpc * 0.9 > 100 then 100 else round(b.zpc_paid_clicks * b.zcpc * 0.9, 2) end target_spend  "+
						" from  "+
						" ( "+
						" select date_date, iat.camp_id as camp_id, "+
						       " sum(if(partner_source=30647, if(reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks, "+
						       " sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev, "+
						       " ic.system_daily_budget, "+
						       " ic.cost_today, "+
						       " ic.cost_per_click, "+
						       " 0.11 as zcpc  "+
						" from inclick_ads_tracking iat "+ 
						" join inclick_campaigns ic on ic.camp_id = iat.camp_id "+ 
						" join click_batches cb on iat.record_id = cb.click_id  "+
						" join job_export_batch jeb on cb.batch_id = jeb.id   "+
						" where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.18 and feed - 1 = 1 and ic.system_daily_budget > 0 and ic.cost_today <= ic.system_daily_budget and ic.system_daily_budget-3 < ic.cost_today "+
						" group by date_date, iat.camp_id, ic.system_daily_budget, ic.cost_today, ic.cost_per_click, zcpc "+
						" having zpc_rev > 0 "+
						
						" union "+
						
						" select date_date, iat.camp_id as camp_id, "+
						       " sum(if(partner_source=30647, if(reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks, "+
						       " sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev, "+
						       " ic.system_daily_budget, "+
						       " ic.cost_today, "+
						       " ic.cost_per_click, "+
						       " 0.25 as zcpc  "+
						" from inclick_ads_tracking iat "+ 
						" join inclick_campaigns ic on ic.camp_id = iat.camp_id "+ 
						" join click_batches cb on iat.record_id = cb.click_id  "+
						" join job_export_batch jeb on cb.batch_id = jeb.id   "+
						" where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.40 and feed - 1 = 2 and ic.system_daily_budget > 0 and ic.cost_today <= ic.system_daily_budget and ic.system_daily_budget-3 < ic.cost_today "+
						" group by date_date, iat.camp_id, ic.system_daily_budget, ic.cost_today, ic.cost_per_click, zcpc "+
						" having zpc_rev > 0 "+
						") b " + 
						" where b.camp_id in (" + StringUtils.join(campaignIds, ',')  +" ) and  b.zpc_paid_clicks * b.zcpc * 0.9 > 0.5 " ;
		

		insertOptimizationEntries(sql, 2, excludedCampaigns);
		
	}
	
	
	public static void scenarioThree(Set<Long> campaignIds) {

                Set<String> excludedCampaigns = getExistingCampaignIds();
				
		String sql = " select DATE_ADD(c.date_date, INTERVAL 1 DAY) as date_to, 30647 as publisher_id, 0.11 as publisher_cpc, c.camp_id, " +
				     "        case when (c.system_daily_budget - c.cost_today) / avg_click_cost * 0.11 * 0.40 > 100 then 100 else round((c.system_daily_budget - c.cost_today) / avg_click_cost * 0.11 * 0.40, 2) end target_spend" +
				     " from " +
				     " (" +
				     " select  date_date, iat.camp_id as camp_id," +       
				     " 	       sum(if(reason_code = 99, click_cost, 0)) / sum(if(reason_code = 99, 1 ,0)) as avg_click_cost," +
				     " 	       sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev," +
				     " 	       ic.system_daily_budget," +
				     " 	       ic.cost_today," +
				     " 	       ic.cost_per_click" +       
				     " from inclick_ads_tracking iat" + 
				     " join inclick_campaigns ic on ic.camp_id = iat.camp_id" +
				     " where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.18 and ic.system_daily_budget-3 >= ic.cost_today and ic.cost_today > 0" + 
				     " group by date_date, iat.camp_id, ic.system_daily_budget, ic.cost_today, ic.cost_per_click" +
				     " having zpc_rev = 0" +
				     " ) c" +
				     " where c.camp_id in (" + StringUtils.join(campaignIds, ',')  + ") and (c.system_daily_budget - c.cost_today) / avg_click_cost * 0.11 * 0.40 > 0.5";
		insertOptimizationEntries(sql, 3, excludedCampaigns);
	}
	
	public static void scenarioFour(Set<Long> campaignIds) {

        Set<String> excludedCampaigns = getExistingCampaignIds();
				
		String sql = " select date_to, publisher_id, publisher_cpc, camp_id, case when f.target_spend > 100 then 100 else round(f.target_spend, 2) end target_spend " +
				     " from  " +
				     " ( " +
				     " select DATE_ADD(d.date_date, INTERVAL 1 DAY) as date_to, 30647 as publisher_id, e.zcpc as publisher_cpc, d.camp_id, " +
				     " case when e.zcpc = 0.11 and e.cost_per_click > 0.18 and e.zpc_rev = d.zpc_rev then e.zpc_paid_clicks * e.zcpc + (e.system_daily_budget - e.cost_today) * 0.25 / d.avg_click_cost * e.zcpc " +
				     "      when e.zcpc = 0.11 and d.cost_per_click > 0.18 and e.zpc_rev < d.zpc_rev then e.zpc_paid_clicks * e.zcpc   " +
				     "      when e.zcpc = 0.25 and d.cost_per_click > 0.40 and e.zpc_rev <= d.zpc_rev then e.zpc_paid_clicks * e.zcpc + (e.system_daily_budget - e.cost_today) * 0.40 / d.avg_click_cost * e.zcpc  " +
				     " else -1 " +
				     " end target_spend, " +
				     " case when e.zcpc = 0.11 and d.cost_per_click > 0.18  and e.zpc_rev = d.zpc_rev then e.zpc_paid_clicks + floor((e.system_daily_budget - e.cost_today) * 0.25 / d.avg_click_cost) " +
				     "      when e.zcpc = 0.11 and d.cost_per_click > 0.18  and e.zpc_rev < d.zpc_rev then e.zpc_paid_clicks     " +
				     "      when e.zcpc = 0.25 and d.cost_per_click > 0.40  and e.zpc_rev <= d.zpc_rev then e.zpc_paid_clicks  + floor((e.system_daily_budget - e.cost_today) * 0.40 / d.avg_click_cost)   " +
				     " else -1 " +
				     " end  target_clicks, " +
				     " d.system_daily_budget, d.cost_today, d.cost_per_click    " +
				     " from  " +
				     " ( " +
				     " select iat.date_date, iat.camp_id as camp_id, " +       
				     "        sum(if(iat.reason_code = 99, iat.click_cost, 0)) / sum(if(iat.reason_code = 99, 1 ,0)) as avg_click_cost, " +
				     "        sum(if(iat.partner_source=30647, if(iat.reason_code = 99, iat.click_cost, 0),0)) as zpc_rev, " +
				     "        ic.system_daily_budget, " +
				     "        ic.cost_today, " +
				     "        ic.cost_per_click           " +
				     " from inclick_ads_tracking iat  " +
				     " join inclick_campaigns ic on ic.camp_id = iat.camp_id " +
				     " where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.system_daily_budget-3 >= ic.cost_today and ic.cost_today > 0  " +
				     " group by iat.date_date, iat.camp_id, ic.system_daily_budget, ic.cost_today, ic.cost_per_click  " +
				     " having zpc_rev > 0 " +
				     " ) d " +
				     " join  " +
				     " ( " +
				     " select iat.date_date, iat.camp_id as camp_id, " +
				     "        sum(if(iat.partner_source=30647, if(iat.reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks,  " +
				     "        sum(if(iat.partner_source=30647, if(iat.reason_code = 99, iat.click_cost, 0),0)) as zpc_rev, " +
				     "        ic.system_daily_budget, " +
				     "        ic.cost_today, " +
				     "        ic.cost_per_click, " +
				     "        case jeb.feed-1 when 1 then 0.11  " +
				     "                        when 2 then 0.25  " +
				     "        end zcpc  " +
				     " from inclick_ads_tracking iat  " +
				     " join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
				     " join click_batches cb on iat.record_id = cb.click_id  " +
				     " join job_export_batch jeb on cb.batch_id = jeb.id   " +
				     " where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and feed - 1 > 0 and ic.system_daily_budget-3 >= ic.cost_today and ic.cost_today > 0 " +
				     " group by iat.date_date, iat.camp_id, ic.system_daily_budget, ic.cost_today, ic.cost_per_click, zcpc " +
				     " having zpc_rev > 0 " +
				     " ) e on d.camp_id = e.camp_id " +
				     " ) f " +
				     " where f.target_spend > 0.5 and f.camp_id in (" + StringUtils.join(campaignIds, ',')  + ")" ;
			

		insertOptimizationEntries(sql, 4, excludedCampaigns);
		
	}
	
	
	
    public static void scenarioFourExt(Set<Long> campaignIds) {

        Set<String> excludedCampaigns = getExistingCampaignIds();
		
        String sql = " select date_to, publisher_id, publisher_cpc, camp_id, case when f.target_spend > 100 then 100 else round(f.target_spend, 2) end target_spend " +
		             " from  " +
		             " ( " +
		             " select DATE_ADD(d.date_date, INTERVAL 1 DAY) as date_to, 30647 as publisher_id, 0.25 as publisher_cpc, d.camp_id, " +
		             " case when e.zcpc = 0.11 and e.cost_per_click > 0.40 and e.zpc_rev = d.zpc_rev then (e.system_daily_budget - e.cost_today) * 0.15 / d.avg_click_cost * 0.25 " +
		             " else -1 " +
		             " end target_spend, " +
		             " d.system_daily_budget, d.cost_today, d.cost_per_click    " +
		             " from  " +
		             " ( " +
		             " select iat.date_date, iat.camp_id as camp_id, " +       
		             "        sum(if(iat.reason_code = 99, iat.click_cost, 0)) / sum(if(iat.reason_code = 99, 1 ,0)) as avg_click_cost, " +
		             "        sum(if(iat.partner_source=30647, if(iat.reason_code = 99, iat.click_cost, 0),0)) as zpc_rev, " +
		             "        ic.system_daily_budget, " +
		             "        ic.cost_today, " +
		             "        ic.cost_per_click           " +
		             " from inclick_ads_tracking iat  " +
		             " join inclick_campaigns ic on ic.camp_id = iat.camp_id " +
		             " where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.system_daily_budget-3 >= ic.cost_today and ic.cost_today > 0  " +
		             " group by iat.date_date, iat.camp_id, ic.system_daily_budget, ic.cost_today, ic.cost_per_click " +
		             " having zpc_rev > 0 " +
		             " ) d " +
		             " join  " +
		             " ( " +
		             " select iat.date_date, iat.camp_id as camp_id, " +
		             "        sum(if(iat.partner_source=30647, if(iat.reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks,  " +
		             "        sum(if(iat.partner_source=30647, if(iat.reason_code = 99, iat.click_cost, 0),0)) as zpc_rev, " +
		             "        ic.system_daily_budget, " +
		             "        ic.cost_today, " +
		             "        ic.cost_per_click, " +
		             "        case jeb.feed-1 when 1 then 0.11  " +
		             "                        when 2 then 0.25  " +
		             "        end zcpc  " +
		             " from inclick_ads_tracking iat  " +
		             " join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
		             " join click_batches cb on iat.record_id = cb.click_id  " +
		             " join job_export_batch jeb on cb.batch_id = jeb.id   " +
		             " where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and feed - 1 > 0 and ic.system_daily_budget-3 >= ic.cost_today and ic.cost_today > 0 " +
		             " group by iat.date_date, iat.camp_id, ic.system_daily_budget, ic.cost_today, ic.cost_per_click, zcpc " +
		             " having zpc_rev > 0 " +
		             " ) e on d.camp_id = e.camp_id " +
		             " ) f " +
		             " where f.target_spend > 0.5 and f.camp_id in (" + StringUtils.join(campaignIds, ',')  + ")" ;
	

        insertOptimizationEntries(sql, 41, excludedCampaigns);

    }	
	
	
	
	
	
	public static void scenarioFive(Set<Long> campaignIds) {

                Set<String> excludedCampaigns = getExistingCampaignIds();
				
		String sql =  " select s21.date_to, 30647 as publisher_id, s21.zcpc as publisher_cpc, s21.camp_id, target_spend " +
				" from  " +
				" ( " +
				" select distinct DATE_ADD(camprev.date_date, INTERVAL 1 DAY) as date_to, ic.user_id as user_id, ic.camp_id as camp_id, sysnull.account_system_daily_budget, sysnull.user_spent, ic.system_daily_budget, ic.cost_today, camprev.zpc_rev as rev_fromZip,  0.11 as zcpc,ic.cost_per_click, 0 as target_spend, 0 as target_clicks " +
				" from inclick_campaigns ic  " +
				" join ( " +
				" select iu.id, iu.account_system_daily_budget, sum(ic.cost_today) as user_spent " +
				" from inclick_campaigns ic " +
				" join inclick_user iu " +
				" on ic.user_id =  iu.id " +
				" where iu.account_system_daily_budget is not NULL " +
				" group by iu.id, iu.account_system_daily_budget " +
				" ) sysnull " +
				" on ic.user_id = sysnull.id " +
				" join ( " +
				" select iat.date_date, iat.camp_id as camp_id, " +
				"        sum(if(iat.partner_source=30647, if(iat.reason_code = 99, iat.click_cost, 0),0)) as zpc_rev " +     
				" from inclick_ads_tracking iat  " +
				" join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
				" where iat.date_date = DATE(NOW())   " +
				" group by iat.date_date, iat.camp_id " +
				" having zpc_rev = 0 " +
				" ) camprev  " +
				" on ic.camp_id = camprev.camp_id " +
				" where ic.status = 'yes' and ic.camp_type = 'pjl' and sysnull.account_system_daily_budget > 0 and sysnull.user_spent <= sysnull.account_system_daily_budget and sysnull.user_spent > sysnull.account_system_daily_budget - 3 " +
				" and ic.system_daily_budget is NULL " +
				" ) s21 " +
				" where s21.camp_id in (" + StringUtils.join(campaignIds, ',')  + ") and s21.target_spend > 0.5" ;
			
		insertOptimizationEntries(sql, 5, excludedCampaigns);
		
	}
	
	
	
	public static void scenarioSix(Set<Long> campaignIds) {	

                Set<String> excludedCampaigns = getExistingCampaignIds();
			
		String sql =  	" select s22.date_to, 30647 as publisher_id, s22.zcpc as publisher_cpc, s22.camp_id, " +
						" case when s22.target_spend * 0.9 > 100 then 100 else round(s22.target_spend * 0.9, 2) end target_spend " +
						" from  " +
						" ( " +
						" select DATE_ADD(camprev.date_date, INTERVAL 1 DAY) as date_to, sysnull.id as user_id, ic.camp_id as camp_id, sysnull.account_system_daily_budget, sysnull.user_spent, ic.system_daily_budget, ic.cost_today, camprev.zpc_rev as rev_fromZip,  camprev.zcpc, ic.cost_per_click, camprev.zpc_paid_clicks* camprev.zcpc as target_spend, camprev.zpc_paid_clicks as target_clicks " +
						" from inclick_campaigns ic  " +
						" join  " +
						" ( " +
						" select iu.id, iu.account_system_daily_budget, sum(if(ic.system_daily_budget is NULL, 1, 0)) as camps_num, sum(ic.cost_today) as user_spent " +
						" from inclick_campaigns ic " +
						" join inclick_user iu " +
						" on ic.user_id =  iu.id " +
						" where iu.account_system_daily_budget is not NULL " +
						" group by iu.id, iu.account_system_daily_budget " +
						" ) sysnull " +
						" on sysnull.id = ic.user_id " +
						" join  " +
						" ( " +
						" select date_date, iat.camp_id as camp_id, " +
						"        sum(if(partner_source=30647, if(reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks, " +
						"        sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev, " +
						"        0.11 as zcpc  " +
						" from inclick_ads_tracking iat  " +
						" join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
						" join click_batches cb on iat.record_id = cb.click_id  " +
						" join job_export_batch jeb on cb.batch_id = jeb.id   " +
						" where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.18 and feed - 1 = 1  " +
						" group by date_date, iat.camp_id, zcpc " +
						" having zpc_rev > 0 " +
					
						" union " +
					
						" select date_date, iat.camp_id as camp_id, " +
						"        sum(if(partner_source=30647, if(reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks, " +
						"        sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev, " +
						"        0.25 as zcpc  " +
						" from inclick_ads_tracking iat  " +
						" join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
						" join click_batches cb on iat.record_id = cb.click_id  " +
						" join job_export_batch jeb on cb.batch_id = jeb.id   " +
						" where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.40 and feed - 1 = 2  " +
						" group by date_date, iat.camp_id, zcpc " +
						" having zpc_rev > 0 " +
						" ) camprev  " +
						" on ic.camp_id = camprev.camp_id " +
						" where ic.status = 'yes' and camp_type = 'pjl' and sysnull.account_system_daily_budget > 0 and sysnull.user_spent <= sysnull.account_system_daily_budget and sysnull.user_spent > sysnull.account_system_daily_budget - 5 " +
						" and ic.system_daily_budget is NULL " +
						" ) s22 " +
						" where s22.camp_id in (" + StringUtils.join(campaignIds, ',')  + ") and s22.target_spend * 0.9 > 0.5 ";
						
		insertOptimizationEntries(sql, 6, excludedCampaigns);
		
	}
	
	public static void scenarioSeven(Set<Long> campaignIds) {

                Set<String> excludedCampaigns = getExistingCampaignIds();
				
		String sql =    " select s23.date_to, 30647 as publisher_id, s23.zcpc as publisher_cpc, s23.camp_id, " +
						" case when s23.target_spend > 100 then 100 else round(s23.target_spend, 2) end target_spend " +
						" from  " +
						" ( " +
						" select DATE_ADD(camprev.date_date, INTERVAL 1 DAY) as date_to, ic.user_id as user_id, ic.camp_id, sysnull.account_system_daily_budget, sysnull.user_spent, ic.system_daily_budget, ic.cost_today, camprev.zpc_rev as rev_fromZip, camprev.zcpc, ic.cost_per_click,  " +
						" case when sysnull.account_system_daily_budget >= sysnull.user_spent " +
						"           and sysnull.user_spent >= sysnull.campsNotNull_budget  " +
						"              then ((sysnull.account_system_daily_budget - sysnull.user_spent) * 0.40 / (sysnull.campsNull_num * ic.cost_per_click) + zpc_paid_clicks) * camprev.zcpc   " +
						"      when sysnull.account_system_daily_budget >= sysnull.campsNotNull_budget  " +
						"           and sysnull.user_spent < sysnull.campsNotNull_budget  " +
						"              then ((sysnull.account_system_daily_budget - sysnull.campsNotNull_budget) * 0.40 / (sysnull.campsNull_num * ic.cost_per_click) + zpc_paid_clicks) * camprev.zcpc  " + 
						" end as target_spend     " + 
						" from inclick_campaigns ic  " +
						" join  " +
						" ( " +
						" select iu.id, iu.account_system_daily_budget, sum(if(ic.system_daily_budget is not NULL, ic.system_daily_budget, 0)) as campsNotNull_budget,  " +
						" sum(if(ic.system_daily_budget is NULL, 1, 0)) as campsNull_num, sum(ic.cost_today) as user_spent " +
						" from inclick_campaigns ic " +
						" join inclick_user iu " +
						" on ic.user_id =  iu.id " +
						" where iu.account_system_daily_budget is not NULL and ic.status = 'yes' and ic.camp_type = 'pjl' and ic.camp_start <= curdate() and ic.camp_end >= curdate() " +
						" group by iu.id, iu.account_system_daily_budget " +
						" ) sysnull " +
						" on sysnull.id = ic.user_id " +
						" join  " +
						" ( " +
						" select date_date, iat.camp_id as camp_id, " +
							"    sum(if(partner_source=30647, if(reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks, " +
							"    sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev, " +
							"    0.11 as zcpc " +
						" from inclick_ads_tracking iat  " +
						" join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
						" join click_batches cb on iat.record_id = cb.click_id  " +
						" join job_export_batch jeb on cb.batch_id = jeb.id   " +
						" where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.18 and feed - 1 = 1  " +
						" group by date_date, iat.camp_id, zcpc " +
						
						" union " +
						
						" select date_date, iat.camp_id as camp_id, " +
						"        sum(if(partner_source=30647, if(reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks, " +
						"        sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev, " +
						"        0.25 as zcpc  " +
						" from inclick_ads_tracking iat  " +
						" join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
						" join click_batches cb on iat.record_id = cb.click_id  " +
						" join job_export_batch jeb on cb.batch_id = jeb.id   " +
						" where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.40 and feed - 1 = 2  " +
						" group by date_date, iat.camp_id, zcpc " +
						" ) camprev  " +
						" on ic.camp_id = camprev.camp_id " +
						" where ic.status = 'yes' and camp_type = 'pjl' and sysnull.account_system_daily_budget > 0 and sysnull.user_spent <= sysnull.account_system_daily_budget - 5 " + 
						" and sysnull.account_system_daily_budget >= sysnull.campsNotNull_budget  " +
						" and ic.system_daily_budget is NULL  " +
						" ) s23 " +
						" where s23.camp_id in (" + StringUtils.join(campaignIds, ',') + " ) and s23.target_spend > 0.5 ";
						
		insertOptimizationEntries(sql, 7, excludedCampaigns);		
	}
	
	public static void scenarioEight(Set<Long> campaignIds) {	
		
		Set<String> excludedCampaigns = getExistingCampaignIds();

		String sql =    " select s23.date_to, 30647 as publisher_id, s23.zcpc as publisher_cpc, s23.camp_id, case when s23.target_spend > 100 then 100 else round(s23.target_spend, 2) end target_spend " +
				        " from (  " +
				        " select DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) as date_to, ic.camp_id, 0.11 as zcpc,  " +
				        " case when sysnull.account_system_daily_budget >= sysnull.user_spent " +
				        "             and sysnull.user_spent >= sysnull.campsNotNull_budget  " +
				        "               then ((sysnull.account_system_daily_budget - sysnull.user_spent) * 0.40 / (sysnull.campsNull_num * ic.cost_per_click) ) * 0.11   " +
				        "      when sysnull.account_system_daily_budget >= sysnull.campsNotNull_budget  " +
				        "             and sysnull.user_spent < sysnull.campsNotNull_budget  " +
				        "               then ((sysnull.account_system_daily_budget - sysnull.campsNotNull_budget) * 0.40 / (sysnull.campsNull_num * ic.cost_per_click) ) * 0.11   " +
				        " end as target_spend     " +
				        " from inclick_campaigns ic  " +
				        " join (select iu.id, iu.account_system_daily_budget, sum(if(ic.system_daily_budget is not NULL, ic.system_daily_budget, 0)) as campsNotNull_budget,  " +
				        "      sum(if(ic.system_daily_budget is NULL, 1, 0)) as campsNull_num, sum(ic.cost_today) as user_spent " +
				        " from inclick_campaigns ic " +
				        " join inclick_user iu on ic.user_id =  iu.id " +
				        " where iu.account_system_daily_budget is not NULL and ic.status = 'yes' and ic.camp_type = 'pjl' and ic.camp_start <= curdate() and ic.camp_end >= curdate() " +
				        " group by iu.id, iu.account_system_daily_budget " +
				        " ) sysnull on sysnull.id = ic.user_id " +
				        " where ic.status = 'yes' and camp_type = 'pjl' and sysnull.account_system_daily_budget > 0 and sysnull.user_spent <= sysnull.account_system_daily_budget - 3 and ic.system_daily_budget is NULL  " +
				        " and ic.camp_id not in (select distinct jec.camp_id from job_export_campaignstats jec join job_export_batch jeb on jec.batch_id = jeb.id where date(jeb.date) = DATE(NOW()) and publisher = 30647)  " +
				        " ) s23 " +
				        " where s23.camp_id in (" + StringUtils.join(campaignIds, ',') + " ) and s23.target_spend > 0.5 ";
						
		insertOptimizationEntries(sql, 8, excludedCampaigns);		
	}
	
	
	public static void scenarioNine(Set<Long> campaignIds) {	

                Set<String> excludedCampaigns = getExistingCampaignIds();
			
		String sql = " select s3.date_to, 30647 as publisher_id, s3.zcpc as publisher_cpc, s3.camp_id, case when s3.target_spend > 100 then 100 else round(s3.target_spend, 2) end target_spend " +
				" from  " +
				" ( " +
				" 	select DATE_ADD(camprev.date_date, INTERVAL 1 DAY) as date_to, accnull.id as user_id, ic.camp_id, accnull.account_system_daily_budget, accnull.user_spent, ic.system_daily_budget, ic.cost_today, camprev.zpc_rev as rev_fromZip,  camprev.zcpc, ic.cost_per_click,  " +
						
				" 		case when zpc_paid_clicks * camprev.zcpc < 50 then zpc_paid_clicks * camprev.zcpc + 10  " +
				" 		     else zpc_paid_clicks * camprev.zcpc " +
				" 		end as target_spend,  " +
						
				" 		case when zpc_paid_clicks * camprev.zcpc < 50 then floor(zpc_paid_clicks + 10 / camprev.zcpc)  " +
				" 		     else zpc_paid_clicks " +
				" 		end as target_clicks " +
						
				" 		from inclick_campaigns ic  " +
				" 		join  " +
				" 		( " +
				" 		select iu.id as id, iu.account_system_daily_budget as account_system_daily_budget, sum(ic.cost_today) as user_spent " +
				" 		from inclick_campaigns ic " +
				" 		join inclick_user iu " +
				" 		on ic.user_id =  iu.id " +
				" 		where iu.account_system_daily_budget is NULL " +
				" 		group by iu.id, iu.account_system_daily_budget " +
				" 		) accnull " +
				" 		on accnull.id = ic.user_id " +
				" 		join  " +
				" 		( " +
				" 		select date_date, iat.camp_id as camp_id, " +
				" 			       sum(if(partner_source=30647, if(reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks, " +
				" 		       sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev, " +
				" 		       0.11 as zcpc  " +
				" 		from inclick_ads_tracking iat  " +
				" 		join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
				" 		join click_batches cb on iat.record_id = cb.click_id  " +
				" 		join job_export_batch jeb on cb.batch_id = jeb.id   " +
				" 		where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.18 and feed - 1 = 1  " +
				" 		group by date_date, iat.camp_id, zcpc " +
						
				" 		union " +
						
				" 		select date_date, iat.camp_id as camp_id, " +
				" 		       sum(if(partner_source=30647, if(reason_code = 99, 1 ,0), 0)) as zpc_paid_clicks, " +
				" 		       sum(if(partner_source=30647, if(reason_code = 99, click_cost, 0),0)) as zpc_rev, " +
				" 		       0.25 as zcpc  " +
				" 		from inclick_ads_tracking iat  " +
				" 		join inclick_campaigns ic on ic.camp_id = iat.camp_id  " +
				" 		join click_batches cb on iat.record_id = cb.click_id  " +
				" 		join job_export_batch jeb on cb.batch_id = jeb.id   " +
				" 		where ic.status = 'yes' and camp_type = 'pjl' and iat.date_date = DATE(NOW()) and ic.cost_per_click > 0.40 and feed - 1 = 2  " +
				" 		group by date_date, iat.camp_id, zcpc " +
				" 		) camprev  " +
				" 		on ic.camp_id = camprev.camp_id " +
				" 		where ic.status = 'yes' and camp_type = 'pjl' and accnull.account_system_daily_budget is NULL and ic.system_daily_budget is NULL " +
				" 		) s3 " +
				" where s3.camp_id in  (" + StringUtils.join(campaignIds, ',')  +") and s3.target_spend > 0.5";
						
		insertOptimizationEntries(sql, 9, excludedCampaigns);		
	}
	
	public static void excludeCampaignsByClients() {
		String sql = " select distinct a.camp_id, a.publisher_cpc " +
				" from ( " +
						" select ots.camp_id, ots.publisher_cpc " +
						" from opt_target_spend ots " +
						" where ots.date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) " +
						     " and ots.publisher_cpc = 0.11 " +
						     " and ots.camp_id in ( " +
						     " 0 " +
						     " ) " +

						" union " +

						" select ots.camp_id, ots.publisher_cpc " +
						" from opt_target_spend ots " +
						" where ots.date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY)  " +
						      " and ots.publisher_cpc = 0.25 " +
						      " and ots.camp_id in ( " +
						      " 0 " +
						      " ) " +

						" union " +

						" select distinct ots.camp_id, ots.publisher_cpc " +
						" from opt_target_spend ots " +
						" join inclick_campaigns ic on ic.camp_id = ots.camp_id " +
						" join inclick_user iu on iu.id = ic.user_id " +
						" where ots.date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) " + 
						      " and ots.publisher_cpc = 0.11 "+
						      " and ic.user_id in ( " +
						      " 19718, " +
						      " 44887, " +
						      " 18847, " +
						      " 44886, " +
						      " 22273, " +
						      " 6844,  " +
						      " 18846, " +
						      " 44425, " +
						      " 43758, " +
						      " 42620, " +
						      " 49768, " +
						      " 48117, " +
						      " 43213, " +
						      " 49708, " +
						      " 23047, " +
						      " 41338, " +
						      " 41032, " +
						      " 50238, " +
						      " 50237, " +
						      " 21432, " +
						      " 41038, " +
						      " 49769, " +
						      " 26129  " +
						      " ) " +

						" union " +

						" select distinct ots.camp_id, ots.publisher_cpc " +
						" from opt_target_spend ots " +
						" join inclick_campaigns ic on ic.camp_id = ots.camp_id " +
						" join inclick_user iu on iu.id = ic.user_id " +
						" where ots.date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) " + 
						      " and ots.publisher_cpc = 0.25 " +
						      " and ic.user_id in ( " +
						      " 19718, " +
						      " 44887, " +
						      " 18847, " +
						      " 44886, " +
						      " 22273, " +
						      " 6844, " +
						      " 18846, " +
						      " 44425, " +
						      " 43758, " +
						      " 42620, " +
						      " 49768, " +
						      " 48117," +
						      " 43213," +
						      " 41038, " +
						      " 49708," +
						      " 23047," +
						      " 41338," +
						      " 41032," +
						      " 50238," +
						      " 50237," +
						      " 21432," +
						      " 49769," +
						      " 26129" +
						      ")" +
						") a" ;
		int resultCount = 0;
		try {
			Statement stmt = inClickTargetConn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				long campaignId = rs.getLong("camp_id");
				double cpc = rs.getDouble("publisher_cpc");
				logger.info("Excluded campaign number (camp_id, cpc): " + campaignId +" " + rs.getDouble("publisher_cpc"));
				PreparedStatement upStmt =  inClickTargetConn.prepareStatement("update opt_target_spend ots " +
																			   " set ots.target_spend = 0, stepNum =? where date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) and " + 
																			    " camp_id = ? and publisher_cpc = ?" );
				upStmt.setInt(1, 10);
				upStmt.setLong(2, campaignId);
				upStmt.setDouble(3, cpc);
				upStmt.executeUpdate();
				upStmt.close();
				resultCount++;
			}
			
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
		logger.info("Scenario Exclusions "  +resultCount);
	}

	public static void includeCampaignsByClients(Set<Long> campaignIds) {
		String sql = "  select DATE_ADD(date(now()), INTERVAL 1 DAY) as date_to, 30647 as publisher_id, a.publisher_cpc, a.camp_id, a.target_spend " +
					 "	from ( " +
					 "	select ic.camp_id, ic.camp_name, 0.11 as publisher_cpc, ic.user_id, 5 as target_spend " +
					 "	from inclick_campaigns ic " +  
					 "	join inclick_user iu on ic.user_id = iu.id " +
					 "	where ic.cost_per_click > 0.18 and ic.camp_id not in (select distinct camp_id from opt_target_spend where date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) and publisher_cpc = 0.11) " +  
						      " and iu.id in ( " +
						      " 20279, " +
						      " 68657, " +
						      " 31824, " +
						      " 30874, " +
						      " 57503, " +
						      " 34544, " +
						      " 40454, " +
						      " 13398, " +
						      " 19038, " +
						      " 19438, " +
						      " 19568, " +
						      " 22032, " +
						      " 23912, " +
						      " 24525, " +
						      " 25210, " +
						      " 28747, " +
						      " 33780, " +
						      " 33977, " +
						      " 36936, " +
						      " 40454, " +
						      " 41031, " +
						      " 42604, " +
						      " 47455, " +
						      " 48238, " +
						      " 49247, " +
						      " 49972, " +
						      " 50951, " +
						      " 2523, " +
						      " 41031, " +
						      "18252, " +
						      "12279, " +
						      "34541, " +
						      "14423, " +
						      "36705, " +
						      "22153, " +
						      "33977, " +
						      "41288, " +
						      "2442 " +
						      ") " +
						 
						" union  " +
						
						" select ic.camp_id, ic.camp_name, 0.11 as publisher_cpc, ic.user_id, 5 as target_spend " + 
						" from inclick_campaigns ic " +
						" where  ic.cost_per_click > 0.18  and ic.camp_id not in (select distinct camp_id from opt_target_spend where date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) and publisher_cpc = 0.11)  " +
						"      and ic.camp_id in ( " +
						"      0 " + 
						"      ) " +
						
						" union " +
						
						" select ic.camp_id, ic.camp_name, 0.25 as publisher_cpc, ic.user_id, 5 as target_spend " +
						" from inclick_campaigns ic  " +
						" join inclick_user iu on ic.user_id = iu.id " +
						" where ic.cost_per_click > 0.40 and ic.camp_id not in (select distinct camp_id from opt_target_spend where date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) and publisher_cpc = 0.25)  " +  
						     " and iu.id in (  " +
						      " 20279, " +
						      " 68657, " +
						      " 31824, " +
						      " 30874, " +
						      " 57503, " +
						      " 34544, " +
						      " 40454, " +	
						      " 14254, " +
						      " 13398, " +
						      " 22032, " +
						      " 25210, " +
						      " 28747, " +
						      " 33780, " +
						      " 33977, " +
						      " 40454, " +
						      " 41031, " +
						      " 47455, " +
                              " 41880, " +
						      " 48238, " +
						      " 49247, " +
						      " 49972, " +
						      " 50951, " +
						      " 51862, " +
						      " 54778, " +
						      " 45899, " +
						      " 50636, " +
						      " 49564 "  +
						      " ) " + 
						
						" union " +
						
						" select ic.camp_id, ic.camp_name, 0.25 as publisher_cpc, ic.user_id, 5 as target_spend  " + 
						" from inclick_campaigns ic  " +
						" where ic.cost_per_click > 0.40 and ic.camp_id not in (select distinct camp_id from opt_target_spend where date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY)  and publisher_cpc = 0.25) " +
						      " and ic.camp_id in (  " +
						      " 15643, " +
						      " 15644, " +
						      " 15645, " +
						      " 15646, " +
						      " 15853, " +
						      " 15854, " +
						      " 16560, " +
						      " 19700, " +
						      " 19701, " +
						      " 19702,  " +
						      " 19703,  " +
						      " 19704, " +
						      " 19705, " +
						      " 19706, " +
						      " 19707, " +
						      " 19708, " +
						      " 19709, " +
						      " 19710, " +
						      " 19711, " +
						      " 19712, " +
						      " 19713, " +
						      " 19714, " +
						      " 19715, " +
						      " 19716, " +
						      " 19718, " +
						      " 19719, " +
						      " 19894, " +
						      " 20159, " +
						      " 20356, " +
						      " 22222, " +
						      " 22518, " +
						      " 22519, " +
						      " 23040, " +
						      " 23191, " +
						      " 23390, " +
						      " 23575, " +
						      " 23576, " +
						      " 23577, " +
						      " 24113,  " +
						      " 23880," +
						      " 25282 " +
						      " )  " +
						      
						" union " +
						
						" select ic.camp_id, ic.camp_name, 0.41 as publisher_cpc, ic.user_id, 41 as target_spend  " + 
						" from inclick_campaigns ic  " +
						" where ic.cost_per_click > 0.55 and ic.camp_id not in (select distinct camp_id from opt_target_spend where date_date = DATE_ADD(DATE(NOW()), INTERVAL 1 DAY) and publisher_cpc = 0.41) " +
						      " and ic.camp_id in (  " +
						      " 17002, " +
						      " 17028, " +
						      " 23466 " +
						      " )  " +						      
						" ) a " +
						" where a.camp_id in (" + StringUtils.join(campaignIds, ',')  +" ) and a.target_spend > 0.5  ";
		insertOptimizationEntries(inClickTargetConn, sql, 11);	
		
	}
	
	public static void thirdFeed(Set<Long> campaignIds) {

		
		String userIDThirdFeed = "2517, "  +
				"6877, "  +
				"8470, "  +
				"8947, "  +
				"9530, "  +
				"9532, "  +
				"10001, "  +
				"10773, "  +
				"12279, "  +
				"12552, "  +
				"13398, "  +
				"14385, "  +
				"19889, "  +
				"20122, "  +
				"20718, "  +
				"21373, "  +
				"26994, "  +
				"28021, "  +
				"28103, "  +
				"28260, "  +
				"28293, "  +
				"28562, "  +
				"29028, "  +
				"30618, "  +
				"30949, "  +
				"31876, "  +
				"32366, "  +
				"34630, "  +
				"36227, "  +
				"37216, "  +
				"39773, "  +
				"39777, "  +
				"40637, "  +
				"41158, "  +
				"41288, "  +
				"42534, "  +
				"42926, "  +
				"43218, "  +
				"43559, "  +
				"44612, "  +
				"44718, "  +
				"44719, "  +
				"44833, "  +
				"45088, "  +
				"45153, "  +
				"46004, "  +
				"46203, "  +
				"46204, "  +
				"47058, "  +
				"47480, "  +
				"47481, "  +
				"47483, "  +
				"47678, "  +
				"47995, "  +
				"48263, "  +
				"48942, "  +
				"49247, "  +
				"49287, "  +
				"49707, "  +
				"49726, "  +
				"49754, "  +
				"49956, "  +
				"50280, "  +
				"50519, "  +
				"50529, "  +
				"50735, "  +
				"51152, "  +
				"51457, "  +
				"51639, "  +
				"51681, "  +
				"51751, "  +
				"51884, "  +
				"52070, "  +
				"52235, "  +
				"52789, "  +
				"53058, "  +
				"53114, "  +
				"53128, "  +
				"53321, "  +
				"53492, "  +
				"53817, "  +
				"53821, "  +
				"53848, "  +
				"54489, "  +
				"54846, "  +
				"55350, "  +
				"55526, "  +
				"56406, "  +
				"56408, "  +
				"56497, "  +
				"56672, "  +
				"56860, "  +
				"56892, "  +
				"56893, "  +
				"57039, "  +
				"57040, "  +
				"57041, "  +
				"57042, "  +
				"57050, "  +
				"57059" ;
		
		String sql = " select DATE_ADD(date(now()), INTERVAL 1 DAY) as date_to, 30647 as publisher_id,  0.41 as publisher_cpc, ic.camp_id as camp_id, round(sum(target_spend/publisher_cpc)*0.41, 2) as target_spend " + 
				     " from inclick_campaigns ic " +
				     " join inclick_user iu on ic.user_id = iu.id " + 
				     " join opt_target_spend ots on ic.camp_id = ots.camp_id " + 
				     " where ots.date_date = DATE_ADD(date(now()), INTERVAL 1 DAY)  and ic.cost_today < 20 and ic.cost_per_click >= 0.55 and ic.camp_id in (select distinct camp_id from opt_target_spend ots where ots.date_date = DATE_ADD(date(now()), INTERVAL 1 DAY)) " +
				     " and ic.user_id in (" + userIDThirdFeed + ") " +
				     " group by date_date, publisher_id, camp_id ";
		
		insertOptimizationEntries(inClickTargetConn, sql, 12);	
		
		
		sql = " update opt_target_spend join inclick_campaigns on opt_target_spend.camp_id = inclick_campaigns.camp_id " + 
			  " set opt_target_spend.target_spend = 0, stepNum = 13 " + 
			  " where opt_target_spend.publisher_cpc in (0.11, 0.25) and opt_target_spend.date_date = DATE_ADD(date(now()), INTERVAL 1 DAY) " +
			  " and inclick_campaigns.cost_today < 20 and inclick_campaigns.cost_per_click >=0.55 and inclick_campaigns.user_id in (" + userIDThirdFeed + ") ";
		
		int resultCount;
		int scenarioNum = 13;	
		
		try {
			Statement stmt = inClickTargetConn.createStatement();
			resultCount = stmt.executeUpdate(sql);
			
			logger.info("Scenario " + scenarioNum  +": " + resultCount);
			
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
		
		Set<String> excludedCampaigns = getExistingCampaignIds();
		
		sql = " select DATE_ADD(date(now()), INTERVAL 1 DAY) as date_to, 30647 as publisher_id, 0.41 as publisher_cpc, ic.camp_id as camp_id, 41 as target_spend " + 
		      " from inclick_campaigns ic " +
		      " join inclick_user iu on ic.user_id = iu.id " +
		      " where ic.system_daily_budget is NULL and ic.cost_per_click >= 0.55 and ic.camp_id not in (select distinct camp_id from opt_target_spend ots where ots.date_date = DATE_ADD(date(now()), INTERVAL 1 DAY) ) " +
		      " and ic.camp_id in (" + StringUtils.join(campaignIds, ',')  +") " + "and ic.user_id in (" + userIDThirdFeed + ") ";
		
		insertOptimizationEntries(inClickTargetConn, sql, 14, excludedCampaigns);
		
		excludedCampaigns = getExistingCampaignIds();
		
		sql = " select DATE_ADD(date(now()), INTERVAL 1 DAY) as date_to, 30647 as publisher_id, 0.41 as publisher_cpc, ic.camp_id as camp_id, case when floor((ic.system_daily_budget - ic.cost_today)/ic.cost_per_click) * 0.41 > 600 then 600 else floor((ic.system_daily_budget - ic.cost_today)/ic.cost_per_click) * 0.41 end as target_spend " +
              " from inclick_campaigns ic " +
              " join inclick_user iu on ic.user_id = iu.id " +
              " where ic.system_daily_budget is not NULL and ic.cost_per_click >= 0.55 and ic.camp_id not in (select distinct camp_id from opt_target_spend ots where ots.date_date = DATE_ADD(date(now()), INTERVAL 1 DAY)) " +
              " and ic.camp_id in (" + StringUtils.join(campaignIds, ',')  +") " + "and ic.user_id in (" + userIDThirdFeed + ") ";
		
		insertOptimizationEntries(inClickTargetConn, sql, 15, excludedCampaigns);
		
		
		
		sql = " update opt_target_spend join inclick_campaigns on opt_target_spend.camp_id = inclick_campaigns.camp_id " + 
			  " set opt_target_spend.target_spend = 1000, stepNum = 16 " + 
			  " where opt_target_spend.publisher_cpc = 0.41 and opt_target_spend.date_date = DATE_ADD(date(now()), INTERVAL 1 DAY) " +
			  " and inclick_campaigns.user_id in (" +  		
					"53128,"+
					"2517,"+
					"20718,"+
		            "40637,"+
		            "46004,"+
		            "54846,"+
		            "48263,"+
		            "51751,"+
		            "28103,"+
		            "46203,"+
		            "49726,"+
		            "48942,"+
		            "46204,"+
		            "54489,"+
		            "50280,"+
		            "53114,"+
		            "30618,"+
		            "56497,"+
		            "44833,"+
		            "56408,"+
		            "57050,"+
		            "36227,"+
		            "51639,"+
		            "56406,"+
		            "31876,"+
		            "47058,"+
		            "56672,"+
		            "45153,"+
		            "53848,"+
		            "43218,"+
		            "47995"+
		         ") ";
			
		resultCount = 0;
		scenarioNum = 16;	
		
		try {
			Statement stmt = inClickTargetConn.createStatement();
			resultCount = stmt.executeUpdate(sql);
			
			logger.info("Scenario " + scenarioNum  +": " + resultCount);
			
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}	
		
	}	
	
	
	public static void updateFeedID() {				

		String sql = " update opt_target_spend " + 
				     " set job_export_feed_id = (case when publisher_cpc = 0.11 then 2 " +
				     " when publisher_cpc = 0.25 then 3 " +
				     " when publisher_cpc = 0.41 then 4 " +
				     " end ) " +
				     " where publisher_cpc in (0.11, 0.25, 0.41) and date_date = DATE_ADD(date(now()), INTERVAL 1 DAY) ";
		
		int updatedNum = 0;	
		
		try {
			Statement stmt = inClickTargetConn.createStatement();
			updatedNum = stmt.executeUpdate(sql);
			
			logger.info("The updated row number for Feed ID: " + updatedNum );
			
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
		
	}
	
	
	public static void updateActiveFlag(int job_export_feed_id) {				

		String sql = " update opt_target_spend " + 
				     " set active_flag = 1 " +
				     " where date_date = DATE_ADD(date(now()), INTERVAL 1 DAY) and target_spend > 0 and job_export_feed_id = " + job_export_feed_id;
		
		int updatedNum = 0;	
		
		try {
			Statement stmt = inClickTargetConn.createStatement();
			updatedNum = stmt.executeUpdate(sql);
			
			logger.info("Updated the row number of " + updatedNum + " for " + job_export_feed_id + " to the active status." );
			
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
		
	}	

	
	public static void criteoCamp(Set<Long> campaignIds) {

        Set<String> excludedCampaigns = getExistingCampaignIds();
		
        String sql = " select distinct date_date as date_to, 68927 as publisher_id, 0.25 as publisher_cpc, ic.camp_id, round(sum(ots.target_spend / ots.publisher_cpc) * 0.25, 2) as target_spend " +  
		             " from opt_target_spend ots " +
        		     " join inclick_campaigns ic on ic.camp_id = ots.camp_id " +
		             " where ic.cost_per_click > 0.45 and date_date = DATE_ADD(date(now()), INTERVAL 1 DAY) " +
		             " and stepNum in (0, 3, 4, 41, 7, 8, 9, 11, 12, 14, 15, 16) and target_spend > 0 and active_flag = 1 " +          
		             " and ots.camp_id in (" + StringUtils.join(campaignIds, ',')  + ") " +
		             " group by date_date, publisher_id, ic.camp_id " + 
		             " having sum(ots.target_spend / ots.publisher_cpc) * 0.25 > 0.45 ";
        
        insertOptimizationEntries(sql, 601, excludedCampaigns);
        
    }	
	

	public static void updateCriteoFeedID() {				

		String sql = " update opt_target_spend " + 
				     " set job_export_feed_id = 5 " +
				     " where publisher_id = 68927 and publisher_cpc = 0.25 and date_date = DATE_ADD(date(now()), INTERVAL 1 DAY) ";
		
		int updatedNum = 0;	
		
		try {
			Statement stmt = inClickTargetConn.createStatement();
			updatedNum = stmt.executeUpdate(sql);
			
			logger.info("The updated row number for Criteo Feed ID: " + updatedNum );
			
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
		
	}	
	

	public static void updateCriteoActiveFlag(int job_export_feed_id) {				

		String sql = " update opt_target_spend " + 
				     " set active_flag = 1 " +
				     " where publisher_id = 68927 and date_date = DATE_ADD(date(now()), INTERVAL 1 DAY) and target_spend > 0 and job_export_feed_id = " + job_export_feed_id;
		
		int updatedNum = 0;	
		
		try {
			Statement stmt = inClickTargetConn.createStatement();
			updatedNum = stmt.executeUpdate(sql);
			
			logger.info("Updated the row number of " + updatedNum + " for Criteo feed ID " + job_export_feed_id + " to the active status." );
			
		} catch (Throwable ex) {
			logger.error(ex.toString());
		}
		
	}		
	
	
	public static void init() {
		String[] a = new String[1];
		Arrays.fill(a, "");
		Allocator.init(a);
	}
	
	public static void init(String[] args) {
		String configFile = null;
		
		for (int i = 0; i < args.length; i++) {
			String a = args[i];
			
			if (a.equals("-config")) {
				configFile = args[++i];
			}
		}
		
		// Load config file, if provided
		if (configFile != null) {
			Config.load(configFile);
		}
		
		 		    
		lookerConn =		
					DBHandler.getConn(Config.LookerDB.HOST,
					Config.LookerDB.PORT, Config.LookerDB.NAME,
					Config.LookerDB.USER, Config.LookerDB.PASSWORD);
		
		
		inClickConn = 
				DBHandler.getConn(Config.InClickDB.HOST,
						Config.InClickDB.PORT, Config.InClickDB.NAME,
						Config.InClickDB.USER, Config.InClickDB.PASSWORD);
		
		 inClickTargetConn = 
				DBHandler.getConn(Config.InClickTargetDB.HOST,
						Config.InClickTargetDB.PORT, Config.InClickTargetDB.NAME,
						Config.InClickTargetDB.USER, Config.InClickTargetDB.PASSWORD);
		
		 statsDbConn = 
				DBHandler.getConn(Config.StatsDB.HOST,
						Config.StatsDB.PORT, Config.StatsDB.NAME,
						Config.StatsDB.USER, Config.StatsDB.PASSWORD);
	}
	
	

}
