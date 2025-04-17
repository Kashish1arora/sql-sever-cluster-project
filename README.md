# sql-sever-cluster-project

solution 





SELECT  cast([Destination_Latitude] as float) as [Destination_Latitude]
      ,convert(float,[Destination_Longitude])[Destination_Longitude]
      ,cast([Total_Demand] as float)[Total_Demand]
      ,cast([ClusterNo] as tinyint)[ClusterNo]
into #cluster_data1
  FROM [DB1].[dbo].[2 Cluster Output data from Python 5 clusters (2)]

  select *,
		[Destination_Latitude]*[Total_Demand] as Weighted_Lat,
		[Destination_Longitude]*[Total_Demand] as Weighted_Lon
 into #cluster_data2
  from #cluster_data1

  select * from #cluster_data2

  select ROW_NUMBER()over(order by ClusterNo) as WH,
         ClusterNo,
		 SUM(Weighted_Lat) as Sum_Lat,
		 SUM(Weighted_Lon) as Sum_Lon,
		 Sum([Total_Demand]) AS Sum_Demand,
		 COUNT(ClusterNo) Count_Cluster,
		 SUM(Weighted_Lat)/Sum([Total_Demand]) as Wh_Lat,
		 SUM(Weighted_Lon)/Sum([Total_Demand]) as Wh_Lon
  into #cluster_data3
  from #cluster_data2
  group by ClusterNo
  having COUNT(ClusterNo)>11

  select CONCAT([Name],' ',WH) as Wh_No,[Values]
  into #cluster_data4
  from #cluster_data3
  unpivot
  ([Values] for [Name] in (Wh_Lat,Wh_Lon))a

  select *
  from #cluster_data4
  pivot
  (Sum([Values]) for Wh_No in ([Wh_Lat 1],[Wh_Lat 2],[Wh_Lat 3],[Wh_Lon 1],[Wh_Lon 2],[Wh_Lon 3]))a


  select * 
  into #cluster_data5
  from [DB1].[dbo].[Sheet1$]
  cross join (select *
  from #cluster_data4
  pivot
  (Sum([Values]) for Wh_No in ([Wh_Lat 1],[Wh_Lat 2],[Wh_Lat 3],[Wh_Lon 1],[Wh_Lon 2],[Wh_Lon 3]))a) s


  Select *,
2 * 6371* ASIN(SQRT(POWER(((SIN(([Wh_Lat 1]*(3.14159/180)-[Destination_Latitude]*(3.14159/180))/2))),2)+COS([Wh_Lat 1]*(3.14159/180))*COS([Destination_Latitude]*(3.14159/180))*POWER(SIN((([Wh_Lon 1]*(3.14159/180)-[Destination_Longitude]*(3.14159/180))/2)),2)))as Dist1,
2 * 6371* ASIN(SQRT(POWER(((SIN(([Wh_Lat 2]*(3.14159/180)-[Destination_Latitude]*(3.14159/180))/2))),2)+COS([Wh_Lat 2]*(3.14159/180))*COS([Destination_Latitude]*(3.14159/180))*POWER(SIN((([Wh_Lon 2]*(3.14159/180)-[Destination_Longitude]*(3.14159/180))/2)),2)))as Dist2,
2 * 6371* ASIN(SQRT(POWER(((SIN(([Wh_Lat 3]*(3.14159/180)-[Destination_Latitude]*(3.14159/180))/2))),2)+COS([Wh_Lat 3]*(3.14159/180))*COS([Destination_Latitude]*(3.14159/180))*POWER(SIN((([Wh_Lon 3]*(3.14159/180)-[Destination_Longitude]*(3.14159/180))/2)),2)))as Dist3
Into #cluster_data6
from #cluster_data5


Select *,
  Case
  When Dist1<Dist2 and Dist1<Dist3 Then Dist1
  When Dist2<Dist1 and Dist2<Dist3 Then Dist2
  When Dist3<Dist2 and Dist3<Dist1 Then Dist3
  End as Min_Dist
into #cluster_data7
from #cluster_data6

select *,
case 
when Min_Dist =Dist1 Then 'WH 1'
when Min_Dist = Dist2 Then 'WH 2'
when Min_Dist = Dist3 Then 'WH 3'
end as [WH NO]
Into #cluster_data8
from #cluster_data7


select [WH NO],
		AVG(Min_Dist) Average_Min_Dis,
		MIN(Min_Dist) Minimum_Min_Dis,
		Max(Min_Dist) Maximum_Min_Dis,
		COUNT([WH NO]) [Count_WH NO],
		Sum(cast(replace(REPLACE([Total_Demand],'$',''),',','') as int)) as Total_Demand
into #cluster_data9
from #cluster_data8
Group by [WH NO]


select *,cast(Total_Demand as float)/cast(Grand_Total_Demand as float) [%Contribution]
from #cluster_data9
cross join (
Select Sum(cast(replace(REPLACE([Total_Demand],'$',''),',','') as int)) as Grand_Total_Demand
from #cluster_data8)a


select a.*,b.Total_Demand as Totaldemand,cast(cast(replace(REPLACE(a.[Total_Demand],'$',''),',','') as int) as float)/cast(b.Total_Demand as float) as [%contribution]
into #cluster_data10
from #cluster_data8 a
join #cluster_data9 b
on a.[WH NO]=b.[WH NO]

select *,sum([%contribution])over(partition by [WH NO] order by Min_Dist asc) as Cum_Contribution
from #cluster_data10

select  a.[WH NO],min(a.Min_Dist)
from (select *,sum([%contribution])over(partition by [WH NO] order by Min_Dist asc) as Cum_Contribution
from #cluster_data10)a
where a.Cum_Contribution> 0.5
Group by a.[WH NO]
 
