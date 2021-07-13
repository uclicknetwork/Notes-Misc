USE [UC_Report]
GO
/** Object:  StoredProcedure [dbo].[SP_RPTAggregateSummaryReport]    Script Date: 7/11/2021 11:28:00 PM **/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   Procedure [dbo].[SP_RPTAggregateSummaryReport]
(
	@AggregationName varchar(100),
	@AccountIDList varchar(max),
    @StartDate date,
	@EndDate date,
	@AggregationStatus int,
	@DirectionID int,
	@CommitmentType int ,
	@GracePeriod int,
	@Penalty int,
	@AggregationCriteriaID int,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As
----------------------------------
-- Sample Data input to Report
----------------------------------
--	@AccountIDList varchar(max) = '0',
--  @StartDate date = '2020-01-01',
--	@EndDate date = '2021-06-30',
--	@AggregationStatus int = 999,   -- Expired or Active doesnt matter
--	@DirectionID int = 2,
--	@CommitmentType int = 999999,   -- Any Commitment
--	@GracePeriod int = 9999999,     -- Grace or no Grace doesnt matter
--	@Penalty int = 999999,          -- Penalty does not matter
--	@AggregationCriteriaID int = 0, -- Any Aggregation Criteria


set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AllAccountFlag int = 0,
		@SQLStr1 nvarchar(max),
		@SQLStr2 nvarchar(max),
		@SQLStr3 nvarchar(max),
		@SQLStr  nvarchar(max),
		@SQLStr4 nvarchar(max),
		@SQLStr5 nvarchar(max)

if (( @AggregationName is not Null ) and ( len(@AggregationName) = 0 ) )
	set @AggregationName = NULL

-------------------------------------------------------
-- Check to ensure that the start date is less than
-- equal to end date
-------------------------------------------------------
if (@StartDate > @EndDate)
Begin

	set @ErrorDescription = 'ERROR !!! Start Date cannot be greater than End Date.'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

------------------------------------------------------
-- Ensure that Direction ID is one of the following
-- values:
-- 1. All (0)
-- 2. Inbound (1)
-- 3. Outbound (2)
------------------------------------------------------
if (@DirectionID not in (0,1,2) )
Begin

	set @ErrorDescription = 'ERROR !!! Invalid value passed for DirectionID. Valid Values are (0,1,2)'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

if(@DirectionID = 0) -- Since we need to consider all directions, its not impotant in search criteria
	set @DirectionID = NULL

-------------------------------------------------------
-- In case the Commitment Type is not (0,1,2) then set 
-- it to NULL which means that its not important
-- criteria to search the aggregations
-------------------------------------------------------
if (isnull(@CommitmentType, 99999) not in (0,1,2))
	set @CommitmentType = NULL

-------------------------------------------------------
-- In case the Grace Period is not (0,1) then set 
-- it to NULL which means that its not important
-- criteria to search the aggregations
-------------------------------------------------------
if (isnull(@GracePeriod, 99999) not in (0,1))
	set @GracePeriod = NULL

-------------------------------------------------------
-- In case the Penalty is not (0,1) then set 
-- it to NULL which means that its not important
-- criteria to search the aggregations
-------------------------------------------------------
if (isnull(@Penalty, 99999) not in (0,1))
	set @Penalty = NULL

-------------------------------------------------------
-- In case the Aggregation Criteria is not (0,1) then set 
-- it to NULL which means that its not important
-- criteria to search the aggregations
-------------------------------------------------------
if (isnull(@AggregationStatus, 99999) not in (0,1))
	set @AggregationStatus = NULL

-------------------------------------------------------------
-- Ensure that Aggregation Criteria is one of the following
-- values:
-- 1. All (0)
-- 2. Till end of Commitment (-1)
-- 3. Till End of Cycle (-2)
-------------------------------------------------------------
if (@AggregationCriteriaID not in (0,-1,-2) )
Begin

	set @ErrorDescription = 'ERROR !!! Invalid value passed for Aggregation Criteria. Valid Values are (0,-1,-2)'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

if(@AggregationCriteriaID = 0) -- if its 0, then it means Aggregation Criteria is not important for search
	set @AggregationCriteriaID = NULL

-------------------------------------------------------
-- Parse the Account ID list to store in Temp table
-------------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAccountIDTable') )
		Drop table #TempAccountIDTable

Create Table #TempAccountIDTable (AccountID varchar(100) )


insert into #TempAccountIDTable
select * from FN_ParseValueList ( @AccountIDList )

----------------------------------------------------------------
-- Check to ensure that none of the values are non numeric
----------------------------------------------------------------

if exists ( select 1 from #TempAccountIDTable where ISNUMERIC(AccountID) = 0 )
Begin

	set @ErrorDescription = 'ERROR !!! List of Account IDs passed contain a non numeric value'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

------------------------------------------------------
-- Check if the All the Accounts have been selected 
------------------------------------------------------

if exists (
				select 1 
				from #TempAccountIDTable 
				where AccountID = 0
			)
Begin

            set @AllAccountFlag = 1
			GOTO GENERATEREPORT
				  
End
		
-----------------------------------------------------------------
-- Check to ensure that all the Account IDs passed are valid values
-----------------------------------------------------------------
		
if exists ( 
				select 1 
				from #TempAccountIDTable 
				where AccountID not in
				(
					Select AccountID
					from ReferenceServer.UC_Reference.dbo.tb_Account
					where flag & 1 <> 1
				)
			)
Begin

	set @ErrorDescription = 'ERROR !!! List of Account IDs passed contain value(s) which are not valid or do not exist'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

GENERATEREPORT:

-------------------------------------------------------------------
-- Extract the relevant aggregations from aggregate summary schema
-- based on the passed start and end date. We will select all
-- the aggregation sysles, that have ovelapping dates with the
-- passed Start and End Dates
-------------------------------------------------------------------
Begin Try

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregateSummary') )
				Drop table #TempAggregateSummary

		select *
		into #TempAggregateSummary
		from vw_AggregateSummary
		where 
		(	
			ActualCycleStartDate <= @StartDate
			and 
			ActualCycleEndDate >= @EndDate
		)
		or
		(
			 ActualCycleStartDate >= @StartDate
			 and 
			 ActualCycleEndDate <= @EndDate
		)
		or
		(
			ActualCycleStartDate >= @StartDate
			and 
			ActualCycleStartDate <= @EndDate
			and 
			ActualCycleEndDate >= @EndDate
		)
		or
		(
			ActualCycleStartDate <= @StartDate
			and 
			ActualCycleEndDate >= @StartDate
			and 
			ActualCycleEndDate <= @EndDate
		)

		set @SQLStr1 = 'Select Account, AggregationName , Direction, AggregationCycleID , AggregationCycle ,' + char(10)+
					   'CommitmentType, GracePeriod, Penalty, AggregationStatus, AggregationCriteria, '+ char(10)+
					   'CycleStartDate, CycleEndDate, GracePeriodStartDate, GracePeriodEndDate, CommittedMinutes, '+ char(10)+
					   'CommittedAmount, AggregationStartDate, AggregationEndDate, TrafficAggregatedIncycle, ' + char(10)+
					   'TrafficAggregatedInGracePeriod, TotalTrafficAggregated, TotalAmountAggregated,TrafficAggregatedOverCommitment, '+ char(10)+
					   'TrafficAggregatedInPercent, ShortfallMinutes, PenaltyAmount, AggregationBlendedRate, AggregationBlendedRateAfterPenalty' + char(10)

		set @SQLStr2 = 'From #TempAggregateSummary tbl1' + char(10) +
					   Case
							When @AllAccountFlag = 1 Then ''
							Else 'inner join #TempAccountIDTable tbl2 on tbl1.AccountID = tbl2.AccountID' + char(10) 
					   End

		set @SQLStr3 = 'Where 1 = 1' + char(10)+
					   Case
							When @DirectionID is NULL Then ''
							Else 'and tbl1.DirectionID = ' + convert(varchar(10) , @DirectionID) + char(10)
					   End +
					   Case
							When @AggregationCriteriaID is NULL Then ''
							Else 'and tbl1.AggregationCriteriaID = ' + convert(varchar(10) , @AggregationCriteriaID) + char(10)
					   End +
					   Case
							When @CommitmentType is NULL Then ''
							Else 'and tbl1.CommitmentTypeVal = ' + convert(varchar(10) , @CommitmentType) + char(10)
					   End +
					   Case
							When @GracePeriod is NULL Then ''
							Else 'and tbl1.GracePeriodVal = ' + convert(varchar(10) , @GracePeriod) + char(10)
					   End +
					   Case
							When @Penalty is NULL Then ''
							Else 'and tbl1.PenaltyVal = ' + convert(varchar(10) , @Penalty) + char(10)
					   End +
					   Case
							When @AggregationStatus is NULL Then ''
							Else 'and tbl1.AggregationStatusFlag = ' + convert(varchar(10) , @AggregationStatus) + char(10)
					   End 

		set @SQLStr4 = 
				Case
					   When (@AggregationName is NULL) then ''
					   When (@AggregationName = '') then ' and tbl1.AggregationName like '  + '''' + '%' + '[]' + '%' + '''' + char(10)
					   When ( ( Len(@AggregationName) =  1 ) and ( @AggregationName = '%') ) then ''
					   When ( right(@AggregationName ,1) = '%' ) then ' and tbl1.AggregationName like ' + '''' + substring(@AggregationName,1 , len(@AggregationName) - 1) + '%' + '''' + char(10)
					   Else ' and tbl1.AggregationName like ' + '''' + @AggregationName + '%' + '''' + char(10)
				End

		set @SQLStr5 = 'Order by tbl1.Direction , tbl1.Account , tbl1.AggregationName , tbl1.CycleStartDate'

		set @SQLStr = @SQLStr1 + @SQLStr2 + @SQLStr3 + @SQLStr4 +  @SQLStr5

		-- DEBUG STATEMENT
		--print @SQLStr

		Exec (@SQLStr)

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While Generating Report for the qualifying aggregations. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	GOTO ENDPROCESS

End Catch

ENDPROCESS:

Select @ErrorDescription

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAccountIDTable') )
		Drop table #TempAccountIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregateSummary') )
		Drop table #TempAggregateSummary

return 0