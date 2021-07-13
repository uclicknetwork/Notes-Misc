USE [UC_Reference]
GO
/****** Object:  StoredProcedure [dbo].[SP_BSAggregateMain]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create    Procedure [dbo].[SP_BSAggregateMain]
(
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

------------------------------------------------------
-- Loop through all accounts for which aggregations
-- exist in the tb_Aggregate schema
------------------------------------------------------
Declare @VarAccountID int

DECLARE db_Aggregate_perAccount CURSOR FOR 
select Distinct tbl2.AccountID
from Tb_Aggregation tbl1
inner join tb_Agreement tbl2 on tbl1.AgreementID = tbl2.AgreementID


OPEN db_Aggregate_perAccount
FETCH NEXT FROM db_Aggregate_perAccount 
INTO @VarAccountID 

While @@FETCH_STATUS = 0   
Begin
	Begin Try

		set @ErrorDescription = NULL
		set @ResultFlag = 0

		-----------------------------------------------------------------
		-- Call Aggregation procedure for each of the qualifying Account
		-----------------------------------------------------------------

		Exec SP_BSAggregatePerAccount @VarAccountID , @ErrorDescription Output , @ResultFlag Output


		------------------------------------------------------------------
		-- Check the Result flag to establish if there was any exception
		------------------------------------------------------------------
		if (@ResultFlag = 1)
		Begin

				set @ResultFlag = 1

				CLOSE db_Aggregate_perAccount  
				DEALLOCATE db_Aggregate_perAccount

				return 1

		End


	End Try

	Begin Catch

			set @ErrorDescription = 'ERROR !!! While running aggregation of traffic for Account  :(' + 
									convert(varchar(20) , @VarAccountID) + '). ' + ERROR_MESSAGE()
	          
			set @ResultFlag = 1

			CLOSE db_Aggregate_perAccount  
			DEALLOCATE db_Aggregate_perAccount

			return 1

	End Catch

 	FETCH NEXT FROM db_Aggregate_perAccount
    INTO @VarAccountID 

End 
 
CLOSE db_Aggregate_perAccount  
DEALLOCATE db_Aggregate_perAccount

return 0

GO
/****** Object:  StoredProcedure [dbo].[SP_BSAggregatePerAccount]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE    Procedure [dbo].[SP_BSAggregatePerAccount]
(
	@AccountID int,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

Declare @AggregationRunDate date = DateAdd(dd , -1 , convert(date , getdate()));

--------------------------------------------------------------------
-- Get list of all the aggregation cycles in the order we want to 
-- process them based on the following paramters:
-- 1. Aggregation Cycle Start Date
-- 2. Aggregation Priority
-- 3. Aggregation Cycle Creation Date
---------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggrgegationsToRecalculate') )
	Drop table #TempAggrgegationsToRecalculate;

With CTE_AllAggregationsForAccount As
(
	select Row_Number() over (partition by tbl3.DirectionID order by tbl1.StartDate , tbl2.AggregationtypePriorityID , tbl1.CycleCreationDate) as RecordID,
		   tbl3.AggregationID,
		   tbl1.AggregationCycleID ,
		   tbl1.AggregationtypeID,
		   tbl1.StartDate, 
		   tbl1.EndDate,
		   tbl3.DirectionID,
		   Case
				When tbl1.AggregationtypeID in (-1,-2 , -6 , -7) then -- Grace Period based Agrgegations
					Case
						When tbl1.GracePeriodEndDate < @AggregationRunDate Then 0
						Else 1
					End
				Else -- Non Grace Period based Aggregations
					Case
						When tbl1.EndDate < @AggregationRunDate Then 0
						Else 1
					End			
		   End as IsAggregationActiveFlag,
		   Case
				When tbl1.AggregationtypeID in (-1,-2 , -6 , -7) then GracePeriodEndDate -- Grace Period based Agrgegations
				Else tbl1.EndDate -- Non Grace Period based Aggregations			
		   End as AggregationEndDate,
		   datediff(dd , StartDate ,@AggregationRunDate) as NumDaysPastCycleStart
	from Tb_AggregationCycle tbl1
	inner join Tb_AggregationtypePriority tbl2 on tbl1.AggregationtypeID = tbl2.AggregationtypeID
	inner join tb_Aggregation tbl3 on tbl1.AggregationID = tbl3.AggregationID
	inner join tb_Agreement tbl4 on tbl3.AgreementID = tbl4.AgreementID
	Where tbl4.AccountID = @AccountID	
	and tbl1.StartDate <= @AggregationRunDate -- Only pick up expired or currently active aggregations. Skip future aggregation cycles.
),
CTE_AllAggregationsForAccount_2 As
(
	select *
	from CTE_AllAggregationsForAccount
	where IsAggregationActiveFlag = 1 
	or
	(
		IsAggregationActiveFlag = 0
		and
		datediff(dd , StartDate , @AggregationRunDate) < 730 -- Default value since we are going to archive data in the system after 2 years
	)
)
select *
into #TempAggrgegationsToRecalculate
from CTE_AllAggregationsForAccount_2

-- DEBUG STATEMENT
--Select * from #TempAggrgegationsToRecalculate
--order by RecordID

-----------------------------------------------------------
-- Exit the procedure if there are no entries in the 
-- Temp table for aggregation recalculations
-----------------------------------------------------------
if ((Select count(*) from #TempAggrgegationsToRecalculate) = 0)
	GOTO ENDPROCESS

-------------------------------------------------------------
-- For all the qualifying aggregations remove the historical
-- data from the Aggregate Summary tables  
-------------------------------------------------------------
Begin Try

	-------------------------------
	-- TB_AGGREGATESUMMARY
	-------------------------------
	Delete tbl1
	from tb_AggregateSummary tbl1
	inner join #TempAggrgegationsToRecalculate tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID

	---------------------------------
	-- TB_AGGREGATESUMMARYRATEDETAILS
	---------------------------------
	Delete tbl1
	from tb_AggregateSummaryRateDetails tbl1
	inner join #TempAggrgegationsToRecalculate tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID

	-----------------------------------------
	-- TB_AGGREGATESUMMARYRATETIERALLOCATION
	-----------------------------------------
	Delete tbl1
	from tb_AggregateSummaryRateTierAllocation tbl1
	inner join #TempAggrgegationsToRecalculate tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID

	---------------------------------
	-- TB_AGGREGATESUMMARYTRAFFIC
	---------------------------------
	Delete tbl1
	from tb_AggregateSummaryTraffic tbl1
	inner join #TempAggrgegationsToRecalculate tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While Deleting historcal data from summary tables for Aggregations of Account : (' + 
	                        convert(varchar(10) ,@AccountID) + '). ' + ERROR_MESSAGE()

	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

----------------------------------------------------------------
-- Loop through each of the aggregations and recalculate based
-- on the aggregation type
----------------------------------------------------------------
Declare @VarAggregationCycleID int,
        @VarAggregationTypeID int

DECLARE db_RecalculateAggregation_Cur CURSOR FOR 
select AggregationCycleID , AggregationtypeID
from #TempAggrgegationsToRecalculate
order by RecordID

OPEN db_RecalculateAggregation_Cur
FETCH NEXT FROM db_RecalculateAggregation_Cur 
INTO @VarAggregationCycleID , @VarAggregationTypeID 

While @@FETCH_STATUS = 0   
Begin

	Begin Try

		set @ErrorDescription = NULL
		set @ResultFlag = 0

		-----------------------------------------------------------------
		-- Call the Appropriate procedure based on the Aggregation type
		-- of the qualifying cycle
		-----------------------------------------------------------------

		if (@VarAggregationTypeID in (-1,-2 , -6, -7))
			Exec SP_BSAggregateTrafficForCommitmentWithGraceTypeCycle @VarAggregationCycleID , @ErrorDescription Output , @ResultFlag Output

		if (@VarAggregationTypeID in (-3,-4 , -8 , -9))
			Exec SP_BSAggregateTrafficForCommitmentNoGraceTypeCycle @VarAggregationCycleID , @ErrorDescription Output , @ResultFlag Output

		if (@VarAggregationTypeID  = -5)
			Exec SP_BSAggregateTrafficForAggregateTypeCycle @VarAggregationCycleID , @ErrorDescription Output , @ResultFlag Output

		------------------------------------------------------------------
		-- Check the Result flag to establish if there was any exception
		------------------------------------------------------------------
		if (@ResultFlag = 1)
		Begin

				set @ResultFlag = 1

				CLOSE db_RecalculateAggregation_Cur  
				DEALLOCATE db_RecalculateAggregation_Cur

				GOTO ENDPROCESS

		End


	End Try

	Begin Catch

			set @ErrorDescription = 'ERROR !!! While calculating summary for aggregation cycle :(' + 
									convert(varchar(20) , @VarAggregationcycleID) + '). ' + ERROR_MESSAGE()
	          
			set @ResultFlag = 1

			CLOSE db_RecalculateAggregation_Cur  
			DEALLOCATE db_RecalculateAggregation_Cur

			GOTO ENDPROCESS

	End Catch

 	FETCH NEXT FROM db_RecalculateAggregation_Cur
    INTO @VarAggregationCycleID , @VarAggregationTypeID 

End 
 
CLOSE db_RecalculateAggregation_Cur  
DEALLOCATE db_RecalculateAggregation_Cur

ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggrgegationsToRecalculate') )
	Drop table #TempAggrgegationsToRecalculate

Return 0







GO
/****** Object:  StoredProcedure [dbo].[SP_BSAggregateTrafficForAggregateTypeCycle]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE     Procedure [dbo].[SP_BSAggregateTrafficForAggregateTypeCycle]
(
    @AggregationCycleID int,
    @ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As


set @ErrorDescription = NULL
set @ResultFlag = 0

--------------------------------------------------------------------
-- This logic is for simple aggregation where we need to aggregate
-- traffic till the end of cycle without any committment overheads.
-- Ensure that the aggregation passed is of the type "Aggregate"
--------------------------------------------------------------------
if not exists (select 1 from Tb_AggregationCycle where AggregationCycleID = @AggregationCycleID and AggregationtypeID  = -5)
Begin

		set @ErrorDescription = 'ERROR !!!! The Aggregation either does not exist in the system or is not of the type Aggregate'
		set @ResultFlag = 1
		GOTO ENDPROCESS

End

-------------------------------------------------------------------
-- Get essential details regarding cycle period , grace period,
-- Aggregation type and Commitment Minutes
-------------------------------------------------------------------
Declare @CycleStartDate date,
        @CycleEndDate date,
		@GracePeriodEndDate date,
		@GracePeriodStartDate date,
		@Commitment Decimal(19,4),
		@AggregationTypeID int,
		@AggregationCriteriaID int,
		@AggregationID int,
		@AccountID int,
		@CommittedMinutes Decimal(19,4),
		@CommittedAmount Decimal(19,4),
		@AggregationRunDate date,
		@AggregationBlendedRate Decimal(19,6) = 0

-- Set the Aggregation Run Date to Current - 1
set @AggregationRunDate = DateAdd(dd , -1 , convert(date , getdate()))

Select @CycleStartDate = StartDate,
       @CycleEndDate = EndDate,
	   @AggregationTypeID = AggregationtypeID,
	   @AggregationID = AggregationID
from Tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

--------------------------------------------------------------------------
-- Since this aggregation is of the type Aggregate, there will be
-- no Commitement and the aggregation criteria will be "Till End of Cycle"
--------------------------------------------------------------------------
set @Commitment = 0
set @AggregationCriteriaID = -2 -- Default value for "Aggregate" type aggregation

------------------------------------------------------
-- Get the Direction for the aggregation to decide
-- if we need to aggregate on settlement (inbound)
-- or routing (outbound) destinations
------------------------------------------------------
Declare @DirectionID int

Select @DirectionID = tbl1.directionID,
       @AccountID = tbl2.AccountID
from Tb_Aggregation tbl1
inner join tb_Agreement tbl2 on tbl1.AgreementID = tbl2.AgreementID
where AggregationID = @AggregationID

-----------------------------------------------------
-- There is no Grace Period since this is a Aggregate
-- type aggregation
-----------------------------------------------------
set @GracePeriodStartDate = NULL
set @GracePeriodStartDate = NULL

--------------------------------------------------------
-- Since there is no committment, we need to set the 
-- Committed Minutes and Committed Amount to 0
--------------------------------------------------------
set @CommittedMinutes = 0
set @CommittedAmount = 0

----------------------------------------------------
-- Check whether this is an ACTIVE or an INACTIVE
-- aggregation based on the Aggregation Run Date (T - 1) 
-- and the Cycle End Date
-----------------------------------------------------
Declare @IsAggregationActiveFlag int = 1 -- Default to Active

-- For simple Aggregation we compare current date to cycle end date 
if (@CycleEndDate < @AggregationRunDate)
	set @IsAggregationActiveFlag = 0 -- Aggregation is no longer active


-- DEBUG STATEMENT
--Select @CycleStartDate CycleStartDate , 
--       @CycleEndDate CycleEndDate,
--       @GracePeriodStartDate GracePeriodStartDate , 
--	   @GracePeriodEndDate GracePeriodEndDate, 
--	   @AggregationTypeID AggregationTypeID,
--	   @AggregationCriteriaID AggregationCriteriaID, 
--	   @CommittedMinutes CommittedMinutes,
--	   @CommittedAmount CommittedAmount,
--	   @IsAggregationActiveFlag IsAggregationActiveFlag

-----------------------------------------------------------
-- Get the aggregation grouping for the cycle in a temp
-- table
-----------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGrouping') )
	Drop table #TempAggregationGrouping

Select agrp.*
into #TempAggregationGrouping
from Tb_AggregationGrouping agrp
inner join Tb_AggregationCycle agcyc on agrp.AggregationID = agcyc.AggregationID
where agcyc.AggregationCycleID = @AggregationCycleID

----------------------------------------------------------------------------
-- Get the traffic from Summarization mart at call date level, based on the 
-- aggregation grouping criteria
----------------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyINUnionOutFinancial') )
	Drop table #TempDailyINUnionOutFinancial

Select CallDate , CommercialTrunkID , INServiceLevelID,CallDuration,
       SettlementDestinationID , RoutingDestinationID, CallTypeID
into #TempDailyINUnionOutFinancial
From ReportServer.Uc_Report.dbo.tb_DailyINUnionOutFinancial
Where AccountID = @AccountID
and DirectionID = @DirectionID
and CallDate Between @CycleStartDate and @AggregationRunDate
and CallDuration > 0


if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficSummaryDetail') )
	Drop table #TempDailyTrafficSummaryDetail

Select convert(date ,CallDate) as CallDate , summ.CommercialTrunkID , agrp.ServiceLevelID,
       agrp.DestinationID , agrp.CalltypeID , convert(Decimal(19,4) ,sum(Callduration)/60.0) as Minutes
into #TempDailyTrafficSummaryDetail
from #TempDailyINUnionOutFinancial summ
inner join #TempAggregationGrouping agrp on
									summ.CommercialTrunkID = agrp.CommercialTrunkID
								and
									summ.INServiceLevelID = agrp.ServiceLevelID
								and
									Case
										When @DirectionID = 1 Then summ.SettlementDestinationID
										When @DirectionID = 2 Then summ.RoutingDestinationID
									End  = agrp.DestinationID
								and
									summ.CalltypeID = agrp.CallTypeID
Where summ.CallDate between agrp.StartDate and isnull(agrp.EndDate , summ.CallDate)
Group by CallDate , summ.CommercialTrunkID , agrp.ServiceLevelID,
       agrp.DestinationID , agrp.CalltypeID;

------------------------------------------------------------------------
-- Remove records for CallDate, where the grouping of Commercial Trunk,
-- Service Level , Destination and Call Type have been used for satisfying
-- different aggregation cycle
------------------------------------------------------------------------
Delete tbl1
from #TempDailyTrafficSummaryDetail tbl1
inner join tb_AggregateSummaryTraffic tbl2 on tbl1.CallDate = tbl2.CallDate
											and
											  tbl1.CommercialTrunkID = tbl2.CommercialTrunkID
											and
											  tbl1.ServiceLevelID = tbl2.ServiceLevelID
											and
											  tbl1.DestinationID = tbl2.DestinationID
											and
											  tbl1.CallTypeID =  tbl2.CalltypeID
where tbl2.AggregationCycleID <> @AggregationCycleID -- Important to ensure that records of same aggregation cycle are not considered

------------------------------------------------------------------------
-- Summarize the data by Call Data for qualifying records and calculate 
-- the cumulative traffic minutes for each call date
------------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailySummaryCummulative') )
	Drop table #TempDailySummaryCummulative;

With CTE_SummaryByCallDate As
(
	Select CallDate , convert(Decimal(19,4) ,sum(Minutes)) as Minutes
	from #TempDailyTrafficSummaryDetail
	Group by CallDate
)
Select  CallDate , Minutes , sum(Minutes) over (order by CallDate) as CummulativeMinutes
into #TempDailySummaryCummulative
from CTE_SummaryByCallDate

-- DEBUG STATEMENT
--Select * from #TempDailySummaryCummulative

--------------------------------------------------------
-- Check the aggregation criteria and on basis of that
-- decide whether we need to aggregate till end of
-- commitment or end of cycle
--------------------------------------------------------
Declare @AggregationEndDate date,
        @AggregationStartDate date,
        @TrafficAggregatedInGracePeriod Decimal(19,4) = 0,
		@TrafficAggregatedInCycle Decimal(19,4) = 0,
		@TrafficAggregatedOverCommitment Decimal(19,4) = 0,
		@TotalTrafficAggregated Decimal(19,4) = 0,
		@TotalAmountAggregated Decimal(19,4) = 0

------------------------------------------------------------
--  Get the actual date when aggregation of traffic started
------------------------------------------------------------
Select @AggregationStartDate = min(CallDate)
from #TempDailySummaryCummulative



-------------------------------------------------------------
-- ******************** SCENARIO **************************
-------------------------------------------------------------
-- Calculate the aggregated traffic for the aggregation based
-- on parameters like whether aggregation is active
-- or not
-------------------------------------------------------------


-------------------------------------------------
-- Set the Aggregation End Date based on whether
-- This is an expired aggregation or an active
-- aggregation
-------------------------------------------------
set @AggregationEndDate = Case 
		                    -- Its an expired aggregation so we will aggregate till end of cycle
							When @IsAggregationActiveFlag = 0 Then @CycleEndDate
							-- Its as active aggregation so we will aggregate till Aggregate Run Date
							When @IsAggregationActiveFlag =  1 Then @AggregationRunDate
						End

-----------------------------------------------------------------------------------------
--           ************************ IMPORTANT NOTE *********************
-----------------------------------------------------------------------------------------					     
-- ***** The following logic should work for both active and expired aggregations ******
-----------------------------------------------------------------------------------------

-- Set the Traffic Aggregated in Grace Period to 0 as there will be no grace period
-- for this type of aggregation
set @TrafficAggregatedInGracePeriod = 0

--------------------------------------------------------------------
-- Calculate the Total Minutes aggregated in the cycle based on 
-- Aggregation Start Date and Aggregation End Date
--------------------------------------------------------------------

------------------------------------------------------------------------------------
-- NOTE : IMPORTANT
-- In scenarios when no traffic has been aggregated for the cycle, there will be no
-- aggregation start date. In such cases we need to set the Traffic Aggregated in cycle
-- to 0
------------------------------------------------------------------------------------
if (@AggregationStartDate is NULL)
Begin

		set @TrafficAggregatedInCycle = 0

End

Else
Begin

		Select @TrafficAggregatedInCycle = isnull(sum(Minutes) , 0)
		from #TempDailyTrafficSummaryDetail
		where CallDate between @AggregationStartDate and @AggregationEndDate

End

---------------------------------------------
-- Get the Total Traffic Minutes Aggregated 
----------------------------------------------
set @TotalTrafficAggregated = @TrafficAggregatedInCycle + @TrafficAggregatedInGracePeriod
			
----------------------------------------------------------
-- Total traffic aggregated over the commitement will always
-- be zero for such aggregations as there is no committment
----------------------------------------------------------
set @TrafficAggregatedOverCommitment = 0

----------------------------------------------------------------------------------------------
-- ***************** LOGIC TO GET THE RATING DETAILS FOR AGGREGATED AMOUNT *******************
----------------------------------------------------------------------------------------------

---------------------------------------------------------
-- Create the temp table to store all the Rating details
-- for the aggregated amount, based on the rate structure
---------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRatingDetails') )
	Drop table #TempAggregationRatingDetails

Create Table #TempAggregationRatingDetails
(
	TierID int,
	TierMinutes Decimal(19,4),
	TierAmount Decimal(19,4),
	TierRate Decimal(19,6)
)

------------------------------------------------------------------------
-- Call the Procedure the get the Rating details for Aggregated Amount
------------------------------------------------------------------------
Begin Try

	Exec SP_BSGetRatingDetailsForAggregatedAmount @AggregationCycleID , @TotalTrafficAggregated

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!!! While getting Rating details for Aggregated Traffic amount for Aggregation cycle :' +
							convert(varchar(10) ,@AggregationCycleID) + '. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

-- DEBUG STATEMENT
--Select sum(TierMinutes) as TotalTierMinutes , 
--       sum(TierAmount) as TotalTierAmount
--from #TempAggregationRatingDetails

-- DEBUG STATEMENT
--select * from #TempAggregationRatingDetails

------------------------------------------------------------------------------------------------------
-- ******** LOGIC TO ALLOCATE TIER OR BLENDED RATE FOR EACH CALL DATE BASED ON RATING DETAILS *******
------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------
-- Get the traffic from the Cumulative Summary schema that is actually
-- part of the aggregation for the cycle
----------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficMinutes') )
	Drop table #TempDailyTrafficMinutes

select *
into #TempDailyTrafficMinutes
from #TempDailySummaryCummulative
where CallDate between @AggregationStartDate and @AggregationEndDate

-- DEBUG
--Select * from #TempDailyTrafficMinutes

--------------------------------------------------------------------
-- Create a Temporary schema to store the details related to 
-- Rate Tier and Rate allocation for traffic minutes on each
-- qualifying Call Date for aggregation
--------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempTierAllocationDetails') )
	Drop table #TempTierAllocationDetails

Create Table #TempTierAllocationDetails
(
	CallDate date,
	TierID int,
	Rate Decimal(19,6),
	AggregatedMinutes Decimal(19,4),
	RatedAmount Decimal(19,4)

)

------------------------------------------------------------------------
-- Call the Procedure to allocate rate tier to each aggregate record
-- in the summary schema
------------------------------------------------------------------------
Begin Try

	Exec SP_BSGetRateTierAllocationForAggregation 

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!!! While allocating rate tiers to aggregated traffic for Aggregation cycle :' +
							convert(varchar(10) ,@AggregationCycleID) + '. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

---------------------------------------------------
-- Get the total amount for the aggregated minutes
---------------------------------------------------
Select @TotalAmountAggregated  = isnull(sum(RatedAmount),0) 
from #TempTierAllocationDetails

-- DEBUG STATEMENT
--select * from #TempTierAllocationDetails


-----------------------------------------------------------
-- Calculate the Aggregation Blended Rate as this will
-- represent the net effective rate applied, considering
-- multipe rates have been used to rate the traffic since
-- it falls under different rate tiers of the rate structure
-----------------------------------------------------------

--------------------------------
-- AGGREGATION BLENDED RATE
--------------------------------
set @AggregationBlendedRate = Case
									When @TotalTrafficAggregated > 0  Then convert(Decimal(19,6) , @TotalAmountAggregated/@TotalTrafficAggregated)
									Else NULL
							  End




-- DEBUG STATEMENT
--Select @AggregationStartDate AggregationStartDate,
--       @AggregationEndDate AggregationEndDate,
--	   @TrafficAggregatedInCycle TrafficAggregatedInCycle,
--	   @TrafficAggregatedInGracePeriod TrafficAggregatedInGracePeriod,
--	   @TotalTrafficAggregated TotalTrafficAggregated,
--	   @TotalAmountAggregated TotalAmountAggregated,
--	   @TrafficAggregatedOverCommitment TrafficAggregatedOverCommitment,
--	   convert(Decimal(19,2) ,(@TotalTrafficAggregated/@CommittedMinutes) * 100) as TrafficAggregatedInPercent,
--	   @AggregationBlendedRate AggregationBlendedRate

---------------------------------------------------------------------------
-- ***************** REFRESH DATA IN THE AGGREGATE SCHEMA *****************
---------------------------------------------------------------------------
Begin Transaction UpdateAggregationSummary

Begin Try

	------------------------------------------------------
	-- Delete all data from the Aggregate Summary Schema 
	-- and insert refreshed data
	------------------------------------------------------
	Delete from tb_AggregateSummary
	where AggregationcycleID = @AggregationCycleID

	------------------------------------------------------
	-- Insert refreshed data into the Aggregate Summary
	-- schema for the aggregation cycle
	------------------------------------------------------
	insert into tb_AggregateSummary
	(
		AggregationCycleID,
		CycleStartDate,
		CycleEndDate,
		GracePeriodStartDate,
		GracePeriodEndDate,
		CommittedMinutes,
		CommittedAmount,
		IsAggregationActive,
		AggregationStartDate,
		AggregationEndDate,
		TrafficAggregatedInCycle,
		TrafficAggregatedInGracePeriod,
		TotalTrafficAggregated,
		TotalAmountAggregated,
		TrafficAggregatedOverCommitment,
		TrafficAggregatedInPercent,
		ShortfallMinutes,
		PenaltyAmount,
		AggregationBlendedRate,
		AggregationBlendedRateAfterPenalty,
		ModifiedDate,
		ModifiedByID
	)
	values
	(
		@AggregationCycleID,
		@CycleStartDate,
		@CycleEndDate,
		@GracePeriodStartDate,
		@GracePeriodEndDate,
		@CommittedMinutes,
		@CommittedAmount,
		@IsAggregationActiveFlag,
		@AggregationStartDate,
		Case
			When @TotalTrafficAggregated  = 0 then NULL
			Else @AggregationEndDate
		End,
		isnull(@TrafficAggregatedInCycle , 0),
		@TrafficAggregatedInGracePeriod,
		@TotalTrafficAggregated,
		@TotalAmountAggregated,
		@TrafficAggregatedOverCommitment,
		100, -- This will be defaulted to 100 always since there is no committment in Simple Aggregations
		0, -- No shortfall since its simple Aggregation
		0, -- No penalty since its simple Aggregation
		@AggregationBlendedRate,
		Case
			When @TotalTrafficAggregated > 0 Then 0
			Else NULL
		End, -- -- No Penalty since its simple Aggregation
		getdate(),
		-1
	)

	------------------------------------------------
	-- Delete all Rate tier allocation data for 
	-- aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryRateTierAllocation
	where AggregationCycleID = @AggregationCycleID

	-------------------------------------------------
	-- Insert refreshed data for Rate Tier Allocation
	-- for the Aggregation cycle
	-------------------------------------------------
	Insert into tb_AggregateSummaryRateTierAllocation
	(
			AggregationCycleID,
			StartDate,
			EndDate,
			TierID,
			AggregatedMinutes,
			RatedAmount,
			Rate,
			ModifiedDate,
			ModifiedByID
	)
	Select @AggregationCycleID,
		   min(CallDate),
		   max(CallDate),
		   TierID,
		   sum(AggregatedMinutes),
		   sum(RatedAmount),
		   Rate,
		   getdate(),
		   -1
	from #TempTierAllocationDetails
	Group By TierID , Rate
	order by TierID

	------------------------------------------------
	-- Delete all Rate details data for aggregated
	-- summary schema for aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryRateDetails
	where AggregationCycleID = @AggregationCycleID

	-------------------------------------------------
	-- Insert refreshed rate details data for the 
	-- Aggregation cycle
	-------------------------------------------------
	Insert into tb_AggregateSummaryRateDetails
	(
		AggregationCycleID,
		CallDate,
		TierID,
		AggregatedMinutes,
		RatedAmount,
		Rate,
		ModifiedDate,
		ModifiedByID
	)
	Select @AggregationCycleID,
		   CallDate,
		   TierID,
		   AggregatedMinutes,
		   RatedAmount,
		   Rate,
		   getdate(),
		   -1
	from #TempTierAllocationDetails
	Order By CallDate

	------------------------------------------------
	-- Delete all traffic details data for aggregated
	-- summary schema for aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryTraffic
	where AggregationCycleID = @AggregationCycleID;

	-------------------------------------------------
	-- Insert refreshed traffic details data for the 
	-- Aggregation cycle in summary schema
	-------------------------------------------------
	With CTE_BlendedRatePerCallDate As
	(
		Select CallDate , 
			   Count(TierID) as NumTiers ,
			   convert(Decimal(19,6) , Sum(RatedAmount)/Sum(AggregatedMinutes)) as BlendedRate
		From #TempTierAllocationDetails
		group by CallDate
	),
	CTE_BlendedRatePerCallDate2 As
	(
		Select Distinct tbl1.CallDate , 
					   Case
							When tbl2.NumTiers > 1 Then tbl2.BlendedRate
							Else tbl1.Rate
					   End as Rate		
		from #TempTierAllocationDetails tbl1
		inner join CTE_BlendedRatePerCallDate tbl2 on tbl1.Calldate = tbl2.CallDate
	)
	insert into tb_AggregateSummaryTraffic
	(
		AggregationCycleID,
		CallDate,
		CommercialTrunkID,
		ServiceLevelID,
		DestinationID,
		CallTypeID,
		Minutes,
		Rate,
		Amount,
		ModifiedDate,
		ModifiedByID
	)
	Select @AggregationCycleID ,
	       tbl1.CallDate,
		   tbl1.CommercialTrunkID,
		   tbl1.ServiceLevelID,
		   tbl1.DestinationID,
		   tbl1.CallTypeID,
		   tbl1.Minutes,
		   tbl2.Rate , 
		   convert(Decimal(19,4) , tbl1.Minutes * tbl2.Rate),
		   getdate(),
		   -1
	from #TempDailyTrafficSummaryDetail tbl1
	inner join CTE_BlendedRatePerCallDate2 tbl2 on tbl1.CallDate = tbl2.CallDate
	Where tbl1.CallDate between @AggregationStartDate and @AggregationEndDate


End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! Deleting and Refreshing Aggregation Summary Report for Aggregation cycle : (' + 
						    convert(varchar(10) , @AggregationcycleID) + ').' + ERROR_MESSAGE()

	set @ResultFlag = 1

	Rollback Transaction UpdateAggregationSummary

	GOTO ENDPROCESS

End Catch

Commit Transaction UpdateAggregationSummary

---- DEBUG STATEMENT
--Select * from tb_AggregateSummary
--where AggregationCycleID = @AggregationCycleID

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryRateTierAllocation
--where AggregationCycleID = @AggregationCycleID
--order by TierID

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryRateDetails
--where AggregationCycleID = @AggregationCycleID
--order by CallDate

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryTraffic
--where AggregationCycleID = @AggregationCycleID
--order by calldate


	  
ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGrouping') )
	Drop table #TempAggregationGrouping

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficSummaryDetail') )
	Drop table #TempDailyTrafficSummaryDetail

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailySummaryCummulative') )
	Drop table #TempDailySummaryCummulative

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRatingDetails') )
	Drop table #TempAggregationRatingDetails

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempTierAllocationDetails') )
	Drop table #TempTierAllocationDetails

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyINUnionOutFinancial') )
	Drop table #TempDailyINUnionOutFinancial
GO
/****** Object:  StoredProcedure [dbo].[SP_BSAggregateTrafficForCommitmentNoGraceTypeCycle]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE  Procedure [dbo].[SP_BSAggregateTrafficForCommitmentNoGraceTypeCycle]
(
    @AggregationCycleID int,
    @ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As


set @ErrorDescription = NULL
set @ResultFlag = 0

--------------------------------------------------------------------
-- This logic is for commitment based aggregations where the Traffic
-- minutes are aggregated based on the aggregation cycle and grace
-- period.
-- Ensure that the aggregation passed is of the following two types:
-- 1. "Amount Swap"
-- 2. "Volume Swap"
--------------------------------------------------------------------
if not exists (select 1 from Tb_AggregationCycle where AggregationCycleID = @AggregationCycleID and AggregationtypeID in (-3,-4 , -8 , -9))
Begin

		set @ErrorDescription = 'ERROR !!!! The Aggregation either does not exist in the system or is not of the type Amount or Volume Commitment without Grace Period'
		set @ResultFlag = 1
		GOTO ENDPROCESS

End

-------------------------------------------------------------------
-- Get essential details regarding cycle period , grace period,
-- Aggregation type and Commitment Minutes
-------------------------------------------------------------------
Declare @CycleStartDate date,
        @CycleEndDate date,
		@GracePeriodEndDate date,
		@GracePeriodStartDate date,
		@Commitment Decimal(19,4),
		@AggregationTypeID int,
		@AggregationCriteriaID int,
		@AggregationID int,
		@AccountID int,
		@CommittedMinutes Decimal(19,4),
		@CommittedAmount Decimal(19,4),
		@ShortfallMinutes Decimal(19,4) = 0,
        @PenaltyAmount Decimal(19,4) = 0,
		@AggregationRunDate date,
		@AggregationBlendedRate Decimal(19,6) = 0,
		@AggregationBlendedRateAfterPenalty Decimal(19,6) = 0

-- Set the Aggregation Run Date to Current - 1
set @AggregationRunDate = DateAdd(dd , -1 , convert(date , getdate()))

Select @CycleStartDate = StartDate,
       @CycleEndDate = EndDate,
	   @Commitment = Commitment,
	   @AggregationTypeID = AggregationtypeID,
	   @AggregationCriteriaID = AggregationCriteriaID,
	   @AggregationID = AggregationID
from Tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

------------------------------------------------------
-- Get the Direction for the aggregation to decide
-- if we need to aggregate on settlement (inbound)
-- or routing (outbound) destinations
------------------------------------------------------
Declare @DirectionID int

Select @DirectionID = tbl1.directionID,
       @AccountID = tbl2.AccountID
from Tb_Aggregation tbl1
inner join tb_Agreement tbl2 on tbl1.AgreementID = tbl2.AgreementID
where AggregationID = @AggregationID

-----------------------------------------------------
-- There is no Grace Period since this is a swap
-- based aggregation
-----------------------------------------------------
set @GracePeriodStartDate = NULL
set @GracePeriodStartDate = NULL

------------------------------------------------------------
-- If the Aggregation is of the type "Amount Swap"
-- then we need to find the Minutes based on the Amount 
-- and Rate Structure
------------------------------------------------------------
if (@AggregationTypeID in (-4 , -9) )
Begin

		Begin Try

					set @ErrorDescription = NULL
					set @ResultFlag = 0

					Exec SP_BSGetAggregationCommitmentMinutes @AggregationCycleID , @CommittedMinutes Output,
					                                          @ErrorDescription Output, @ResultFlag Output

					if (@ResultFlag = 1) -- In case of exception, abort any further processing
					Begin
							set @ErrorDescription = @ErrorDescription + '. ' + 
							                       'Exception happened when calculating Commitment Minutes for Amount based aggregation'
							GOTO ENDPROCESS
					End

		End Try

		Begin Catch

					set @ErrorDescription = 'ERROR !!!! When extracting Committed Minutes for Amount based Aggregation. ' + ERROR_MESSAGE()
					set @ResultFlag = 1
					GOTO ENDPROCESS

		End Catch

		set @CommittedAmount = @Commitment

End

------------------------------------------------------------
-- If the Aggregation is of the type "Volume Swap"
-- then we need to find the Amount based on the Minutes 
-- and Rate Structure
------------------------------------------------------------
if (@AggregationTypeID in (-3,-8) )
Begin

		Begin Try

					set @ErrorDescription = NULL
					set @ResultFlag = 0

					Exec SP_BSGetAggregationCommitmentAmount @AggregationCycleID , @CommittedAmount Output,
					                                          @ErrorDescription Output, @ResultFlag Output

					if (@ResultFlag = 1) -- In case of exception, abort any further processing
					Begin
							set @ErrorDescription = @ErrorDescription + '. ' + 
							                       'Exception happened when calculating Commitment Amount for Minutes based aggregation'
							GOTO ENDPROCESS
					End

		End Try

		Begin Catch

					set @ErrorDescription = 'ERROR !!!! When extracting Committed Amount for Minutes based Aggregation. ' + ERROR_MESSAGE()
					set @ResultFlag = 1
					GOTO ENDPROCESS

		End Catch

		set @CommittedMinutes = @Commitment

End

----------------------------------------------------
-- Check whether this is an ACTIVE or an INACTIVE
-- aggregation based on the Aggregation Run Date (T - 1) 
-- and the Cycle End Date
-----------------------------------------------------
Declare @IsAggregationActiveFlag int = 1 -- Default to Active

-- For Commitment based aggregations we compare current date to cycle end date for swap based aggregations
if (@CycleEndDate < @AggregationRunDate)
	set @IsAggregationActiveFlag = 0 -- Aggregation is no longer active


-- DEBUG STATEMENT
--Select @CycleStartDate CycleStartDate , 
--       @CycleEndDate CycleEndDate,
--       @GracePeriodStartDate GracePeriodStartDate , 
--	   @GracePeriodEndDate GracePeriodEndDate, 
--	   @AggregationTypeID AggregationTypeID,
--	   @AggregationCriteriaID AggregationCriteriaID, 
--	   @CommittedMinutes CommittedMinutes,
--	   @CommittedAmount CommittedAmount,
--	   @IsAggregationActiveFlag IsAggregationActiveFlag

-----------------------------------------------------------
-- Get the aggregation grouping for the cycle in a temp
-- table
-----------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGrouping') )
	Drop table #TempAggregationGrouping

Select agrp.*
into #TempAggregationGrouping
from Tb_AggregationGrouping agrp
inner join Tb_AggregationCycle agcyc on agrp.AggregationID = agcyc.AggregationID
where agcyc.AggregationCycleID = @AggregationCycleID

----------------------------------------------------------------------------
-- Get the traffic from Summarization mart at call date level, based on the 
-- aggregation grouping criteria
----------------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyINUnionOutFinancial') )
	Drop table #TempDailyINUnionOutFinancial

Select CallDate , CommercialTrunkID , INServiceLevelID,CallDuration,
       SettlementDestinationID , RoutingDestinationID, CallTypeID
into #TempDailyINUnionOutFinancial
From ReportServer.Uc_Report.dbo.tb_DailyINUnionOutFinancial
Where AccountID = @AccountID
and DirectionID = @DirectionID
and CallDate Between @CycleStartDate and Case 
                                                When @IsAggregationActiveFlag = 1 Then @AggregationRunDate 
												Else @CycleEndDate
										 End
and CallDuration > 0	   


if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficSummaryDetail') )
	Drop table #TempDailyTrafficSummaryDetail

Select convert(date ,CallDate) as CallDate , summ.CommercialTrunkID , agrp.ServiceLevelID,
       agrp.DestinationID , agrp.CalltypeID , convert(Decimal(19,4) ,sum(Callduration)/60.0) as Minutes
into #TempDailyTrafficSummaryDetail
from #TempDailyINUnionOutFinancial summ
inner join #TempAggregationGrouping agrp on
									summ.CommercialTrunkID = agrp.CommercialTrunkID
								and
									summ.INServiceLevelID = agrp.ServiceLevelID
								and
									Case
										When @DirectionID = 1 Then summ.SettlementDestinationID
										When @DirectionID = 2 Then summ.RoutingDestinationID
									End  = agrp.DestinationID
								and
									summ.CalltypeID = agrp.CallTypeID
Where summ.CallDate between agrp.StartDate and isnull(agrp.EndDate , summ.CallDate)
Group by CallDate , summ.CommercialTrunkID , agrp.ServiceLevelID,
       agrp.DestinationID , agrp.CalltypeID;

------------------------------------------------------------------------
-- Remove records for CallDate, where the grouping of Commercial Trunk,
-- Service Level , Destination and Call Type have been used for satisfying
-- different aggregation cycle
------------------------------------------------------------------------
Delete tbl1
from #TempDailyTrafficSummaryDetail tbl1
inner join tb_AggregateSummaryTraffic tbl2 on tbl1.CallDate = tbl2.CallDate
											and
											  tbl1.CommercialTrunkID = tbl2.CommercialTrunkID
											and
											  tbl1.ServiceLevelID = tbl2.ServiceLevelID
											and
											  tbl1.DestinationID = tbl2.DestinationID
											and
											  tbl1.CallTypeID =  tbl2.CalltypeID
where tbl2.AggregationCycleID <> @AggregationCycleID -- Important to ensure that records of same aggregation cycle are not considered

------------------------------------------------------------------------
-- Summarize the data by Call Data for qualifying records and calculate 
-- the cumulative traffic minutes for each call date
------------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailySummaryCummulative') )
	Drop table #TempDailySummaryCummulative;

With CTE_SummaryByCallDate As
(
	Select CallDate , convert(Decimal(19,4) ,sum(Minutes)) as Minutes
	from #TempDailyTrafficSummaryDetail
	Group by CallDate
)
Select  CallDate , Minutes , sum(Minutes) over (order by CallDate) as CummulativeMinutes
into #TempDailySummaryCummulative
from CTE_SummaryByCallDate

-- DEBUG STATEMENT
-- Select * from #TempDailySummaryCummulative

--------------------------------------------------------
-- Check the aggregation criteria and on basis of that
-- decide whether we need to aggregate till end of
-- commitment or end of cycle
--------------------------------------------------------
Declare @AggregationEndDate date,
        @AggregationStartDate date,
        @TrafficAggregatedInGracePeriod Decimal(19,4) = 0,
		@TrafficAggregatedInCycle Decimal(19,4) = 0,
		@TrafficAggregatedOverCommitment Decimal(19,4) = 0,
		@TotalTrafficAggregated Decimal(19,4) = 0,
		@TotalAmountAggregated Decimal(19,4) = 0

------------------------------------------------------------
--  Get the actual date when aggregation of traffic started
------------------------------------------------------------
Select @AggregationStartDate = min(CallDate)
from #TempDailySummaryCummulative

-------------------------------------------------------------
-- ******************** SCENARIO 1 **************************
-------------------------------------------------------------
-- Process flow when the committed minutes for the aggregation 
-- are met 
-------------------------------------------------------------
if ( (select count(*) from #TempDailySummaryCummulative where CummulativeMinutes >= @CommittedMinutes) > 0 )
Begin

			-------------------------------------------------------------
			-- Get the Call Date where the Cumulative Minutes went above 
			-- the Committed minutes
			-------------------------------------------------------------
			Select @AggregationEndDate = CallDate 
			from #TempDailySummaryCummulative
			where CallDate = (
								Select min(CallDate)
								from #TempDailySummaryCummulative
								where CummulativeMinutes >= @CommittedMinutes
				             )

			-- DEBUG STATEMENT
			-- Select @AggregationEndDate as AggregationEndDate

			-- Set the Traffic Aggregated in Grace Period to 0
			set @TrafficAggregatedInGracePeriod = 0

			--------------------------------------------------------------------
			-- Get the Traffic Minutes Aggregated within the cycle period
			-- Based on the aggregation criteria we decide whether to take the
			-- Aggregation End Date or Cycle End Date as the termination date for
			-- aggregation
			--------------------------------------------------------------------
			-- "Till Completion of Commitment" means that we take Aggregation End Date
			-- "Till Completion of Aggregation Cycle" means that we take Cycle End Date
			--------------------------------------------------------------------
			Select @TrafficAggregatedInCycle = isnull(sum(Minutes),0)
			from #TempDailyTrafficSummaryDetail
			where CallDate between @AggregationStartDate and 
					Case
						When @AggregationCriteriaID = -1 Then @AggregationEndDate
						When @AggregationCriteriaID = -2 Then @CycleEndDate
					End

			---------------------------------------------
			-- Get the Total Traffic Minutes Aggregated 
			----------------------------------------------
			set @TotalTrafficAggregated = @TrafficAggregatedInCycle + @TrafficAggregatedInGracePeriod

			-------------------------------------------------------------------------
			-- If the aggregation criteria is "Till Completion of Aggregation Cycle"
			-- then we should change the Aggregation end Date to Cycle End Date
			-- for expired aggregations else set it to Aggregation Run Date
			-------------------------------------------------------------------------
			if (@AggregationCriteriaID = -2 ) -- "Till Completion of Aggregation Cycle"
				set @AggregationEndDate =  Case
												When @IsAggregationActiveFlag = 1 Then @AggregationRunDate
												When @IsAggregationActiveFlag = 0 Then @CycleEndDate
											End 

End

-------------------------------------------------------------
-- ******************** SCENARIO 2 **************************
-------------------------------------------------------------
-- Process flow when the committed minutes for the aggregation 
-- are not met and there is still shortfall
-------------------------------------------------------------
Else
Begin

		-------------------------------------------------
		-- Set the Aggregation End Date based on whether
		-- This is an expired aggregation or an active
		-- aggregation
		-------------------------------------------------
		set @AggregationEndDate = Case 
		                                -- Its an expired aggregation so we will aggregate till end of cycle
										When @IsAggregationActiveFlag = 0 Then @CycleEndDate
										-- Its as active aggregation so we will aggregate till Aggregate Run Date
										When @IsAggregationActiveFlag =  1 Then @AggregationRunDate
								  End

			-----------------------------------------------------------------------------------------
			--           ************************ IMPORTANT NOTE *********************
			-----------------------------------------------------------------------------------------					     
			-- ***** The following logic should work for both active and expired aggregations ******
			-----------------------------------------------------------------------------------------

			-- Set the Traffic Aggregated in Grace Period to 0
			set @TrafficAggregatedInGracePeriod = 0

			--------------------------------------------------------------------
			-- Calculate the Total Minutes aggregated in the cycle based on 
			-- Aggregation Start Date and Aggregation End Date
			--------------------------------------------------------------------

			------------------------------------------------------------------------------------
			-- NOTE : IMPORTANT
			-- In scenarios when no traffic has been aggregated for the cycle, there will be no
			-- aggregation start date. In such cases we need to set the Traffic Aggregated in cycle
			-- to 0
			------------------------------------------------------------------------------------
			if (@AggregationEndDate is NULL)
			Begin

					set @TrafficAggregatedInCycle = 0

			End

			Else
			Begin

					Select @TrafficAggregatedInCycle = isnull(sum(Minutes) , 0)
					from #TempDailyTrafficSummaryDetail
					where CallDate between @AggregationStartDate and @AggregationEndDate

			End

			---------------------------------------------
			-- Get the Total Traffic Minutes Aggregated 
			----------------------------------------------
			set @TotalTrafficAggregated = @TrafficAggregatedInCycle + @TrafficAggregatedInGracePeriod
			

End		

----------------------------------------------------------
-- Get the total traffic aggregated over the commitement
----------------------------------------------------------
set @TrafficAggregatedOverCommitment = Case
											When @TotalTrafficAggregated >= @CommittedMinutes Then (@TotalTrafficAggregated - @CommittedMinutes)
											Else 0
									   End


----------------------------------------------------------------------------------------------
-- ***************** LOGIC TO GET THE RATING DETAILS FOR AGGREGATED AMOUNT *******************
----------------------------------------------------------------------------------------------

---------------------------------------------------------
-- Create the temp table to store all the Rating details
-- for the aggregated amount, based on the rate structure
---------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRatingDetails') )
	Drop table #TempAggregationRatingDetails

Create Table #TempAggregationRatingDetails
(
	TierID int,
	TierMinutes Decimal(19,4),
	TierAmount Decimal(19,4),
	TierRate Decimal(19,6)
)

------------------------------------------------------------------------
-- Call the Procedure the get the Rating details for Aggregated Amount
------------------------------------------------------------------------
Begin Try

	Exec SP_BSGetRatingDetailsForAggregatedAmount @AggregationCycleID , @TotalTrafficAggregated

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!!! While getting Rating details for Aggregated Traffic amount for Aggregation cycle :' +
							convert(varchar(10) ,@AggregationCycleID) + '. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

-- DEBUG STATEMENT
--Select sum(TierMinutes) as TotalTierMinutes , 
--       sum(TierAmount) as TotalTierAmount
--from #TempAggregationRatingDetails

-- DEBUG STATEMENT
--select * from #TempAggregationRatingDetails

------------------------------------------------------------------------------------------------------
-- ******** LOGIC TO ALLOCATE TIER OR BLENDED RATE FOR EACH CALL DATE BASED ON RATING DETAILS *******
------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------
-- Get the traffic from the Cumulative Summary schema that is actually
-- part of the aggregation for the cycle
----------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficMinutes') )
	Drop table #TempDailyTrafficMinutes

select *
into #TempDailyTrafficMinutes
from #TempDailySummaryCummulative
where CallDate between @AggregationStartDate and @AggregationEndDate

-- DEBUG
-- Select * from #TempDailyTrafficMinutes

--------------------------------------------------------------------
-- Create a Temporary schema to store the details related to 
-- Rate Tier and Rate allocation for traffic minutes on each
-- qualifying Call Date for aggregation
--------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempTierAllocationDetails') )
	Drop table #TempTierAllocationDetails

Create Table #TempTierAllocationDetails
(
	CallDate date,
	TierID int,
	Rate Decimal(19,6),
	AggregatedMinutes Decimal(19,4),
	RatedAmount Decimal(19,4)

)

------------------------------------------------------------------------
-- Call the Procedure to allocate rate tier to each aggregate record
-- in the summary schema
------------------------------------------------------------------------
Begin Try

	Exec SP_BSGetRateTierAllocationForAggregation 

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!!! While allocating rate tiers to aggregated traffic for Aggregation cycle :' +
							convert(varchar(10) ,@AggregationCycleID) + '. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

---------------------------------------------------
-- Get the total amount for the aggregated minutes
---------------------------------------------------
Select @TotalAmountAggregated  = isnull(sum(RatedAmount) ,0)
from #TempTierAllocationDetails

-------------------------------------------------------
-- Check if the aggregation has a shortfall and get
-- the shortfall minutes and amount. We would need to
-- only do this for aggregations that have reached their
-- end and are no longer active
-------------------------------------------------------
-- This check needs to be perfromed for aggregations which
-- have Penalty configured in case of shortfall
-------------------------------------------------------
if (@IsAggregationActiveFlag = 0)
Begin

		if ( (@AggregationTypeID in (-3,-8)) and (@TotalTrafficAggregated < @CommittedMinutes)) -- Volume Committment with shortfall
		Begin

				set @ShortfallMinutes = @CommittedMinutes - @TotalTrafficAggregated
				set @PenaltyAmount = Case
										When @AggregationTypeID = -8 Then @CommittedAmount - @TotalAmountAggregated
										Else 0
									 End
				

		End

		if ( (@AggregationTypeID in (-4,-9)) and (@TotalAmountAggregated < @CommittedAmount)) -- Amount Committment with shortfall
		Begin

				set @ShortfallMinutes = @CommittedMinutes - @TotalTrafficAggregated
				set @PenaltyAmount = Case
										When @AggregationTypeID = -9 then @CommittedAmount - @TotalAmountAggregated
										Else 0
									 End
				

		End

End


-----------------------------------------------------------
-- Calculate the Aggregation Blended Rate and Aggregation 
-- Blended Rate After Penalty. The Penalty rate will only be
-- calculated for those aggregations that have expired and
-- have not met their committment
-----------------------------------------------------------

--------------------------------
-- AGGREGATION BLENDED RATE
--------------------------------
set @AggregationBlendedRate = Case
									When @TotalTrafficAggregated > 0  Then convert(Decimal(19,6) , @TotalAmountAggregated/@TotalTrafficAggregated)
									Else NULL
							  End


------------------------------------------
-- AGGREGATION BLENDED RATE AFTER PENALTY
------------------------------------------
if ( (@IsAggregationActiveFlag = 0)) -- Expired Aggregation so we calculate the Blended Penalty Rate based on whether the penalty amount is > 0 or not
Begin

	set @AggregationBlendedRateAfterPenalty = Case
													When @PenaltyAmount > 0 then 
														Case
															When @TotalTrafficAggregated > 0  Then convert(Decimal(19,6) , @CommittedAmount/@TotalTrafficAggregated)
															Else NULL
														End 													
													Else @AggregationBlendedRate
											  End
End

-- DEBUG STATEMENT
--Select @AggregationStartDate AggregationStartDate,
--       @AggregationEndDate AggregationEndDate,
--	   @TrafficAggregatedInCycle TrafficAggregatedInCycle,
--	   @TrafficAggregatedInGracePeriod TrafficAggregatedInGracePeriod,
--	   @TotalTrafficAggregated TotalTrafficAggregated,
--	   @TotalAmountAggregated TotalAmountAggregated,
--	   @TrafficAggregatedOverCommitment TrafficAggregatedOverCommitment,
--	   convert(Decimal(19,2) ,(@TotalTrafficAggregated/@CommittedMinutes) * 100) as TrafficAggregatedInPercent,
--	   @AggregationBlendedRate AggregationBlendedRate

---------------------------------------------------------------------------
-- ***************** REFRESH DATA IN THE AGGREGATE SCHEMA *****************
---------------------------------------------------------------------------
Begin Transaction UpdateAggregationSummary

Begin Try

	------------------------------------------------------
	-- Delete all data from the Aggregate Summary Schema 
	-- and insert refreshed data
	------------------------------------------------------
	Delete from tb_AggregateSummary
	where AggregationcycleID = @AggregationCycleID

	------------------------------------------------------
	-- Insert refreshed data into the Aggregate Summary
	-- schema for the aggregation cycle
	------------------------------------------------------
	insert into tb_AggregateSummary
	(
		AggregationCycleID,
		CycleStartDate,
		CycleEndDate,
		GracePeriodStartDate,
		GracePeriodEndDate,
		CommittedMinutes,
		CommittedAmount,
		IsAggregationActive,
		AggregationStartDate,
		AggregationEndDate,
		TrafficAggregatedInCycle,
		TrafficAggregatedInGracePeriod,
		TotalTrafficAggregated,
		TotalAmountAggregated,
		TrafficAggregatedOverCommitment,
		TrafficAggregatedInPercent,
		ShortfallMinutes,
		PenaltyAmount,
		AggregationBlendedRate,
		AggregationBlendedRateAfterPenalty,
		ModifiedDate,
		ModifiedByID
	)
	values
	(
		@AggregationCycleID,
		@CycleStartDate,
		@CycleEndDate,
		@GracePeriodStartDate,
		@GracePeriodEndDate,
		@CommittedMinutes,
		@CommittedAmount,
		@IsAggregationActiveFlag,
		@AggregationStartDate,
		Case
			When @TotalTrafficAggregated = 0 Then NULL
			Else @AggregationEndDate	
		End,		
		@TrafficAggregatedInCycle,
		@TrafficAggregatedInGracePeriod,
		@TotalTrafficAggregated,
		@TotalAmountAggregated,
		@TrafficAggregatedOverCommitment,
		convert(Decimal(19,2) ,(@TotalTrafficAggregated/@CommittedMinutes) * 100),
		@ShortfallMinutes,
		@PenaltyAmount,
		@AggregationBlendedRate,
		@AggregationBlendedRateAfterPenalty,
		getdate(),
		-1
	)

	------------------------------------------------
	-- Delete all Rate tier allocation data for 
	-- aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryRateTierAllocation
	where AggregationCycleID = @AggregationCycleID

	-------------------------------------------------
	-- Insert refreshed data for Rate Tier Allocation
	-- for the Aggregation cycle
	-------------------------------------------------
	Insert into tb_AggregateSummaryRateTierAllocation
	(
			AggregationCycleID,
			StartDate,
			EndDate,
			TierID,
			AggregatedMinutes,
			RatedAmount,
			Rate,
			ModifiedDate,
			ModifiedByID
	)
	Select @AggregationCycleID,
		   min(CallDate),
		   max(CallDate),
		   TierID,
		   sum(AggregatedMinutes),
		   sum(RatedAmount),
		   Rate,
		   getdate(),
		   -1
	from #TempTierAllocationDetails
	Group By TierID , Rate
	order by TierID

	------------------------------------------------
	-- Delete all Rate details data for aggregated
	-- summary schema for aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryRateDetails
	where AggregationCycleID = @AggregationCycleID

	-------------------------------------------------
	-- Insert refreshed rate details data for the 
	-- Aggregation cycle
	-------------------------------------------------
	Insert into tb_AggregateSummaryRateDetails
	(
		AggregationCycleID,
		CallDate,
		TierID,
		AggregatedMinutes,
		RatedAmount,
		Rate,
		ModifiedDate,
		ModifiedByID
	)
	Select @AggregationCycleID,
		   CallDate,
		   TierID,
		   AggregatedMinutes,
		   RatedAmount,
		   Rate,
		   getdate(),
		   -1
	from #TempTierAllocationDetails
	Order By CallDate

	------------------------------------------------
	-- Delete all traffic details data for aggregated
	-- summary schema for aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryTraffic
	where AggregationCycleID = @AggregationCycleID;

	-------------------------------------------------
	-- Insert refreshed traffic details data for the 
	-- Aggregation cycle in summary schema
	-------------------------------------------------
	With CTE_BlendedRatePerCallDate As
	(
		Select CallDate , 
			   Count(TierID) as NumTiers ,
			   convert(Decimal(19,6) , Sum(RatedAmount)/Sum(AggregatedMinutes)) as BlendedRate
		From #TempTierAllocationDetails
		group by CallDate
	),
	CTE_BlendedRatePerCallDate2 As
	(
		Select Distinct tbl1.CallDate , 
					   Case
							When tbl2.NumTiers > 1 Then tbl2.BlendedRate
							Else tbl1.Rate
					   End as Rate		
		from #TempTierAllocationDetails tbl1
		inner join CTE_BlendedRatePerCallDate tbl2 on tbl1.Calldate = tbl2.CallDate
	)
	insert into tb_AggregateSummaryTraffic
	(
		AggregationCycleID,
		CallDate,
		CommercialTrunkID,
		ServiceLevelID,
		DestinationID,
		CallTypeID,
		Minutes,
		Rate,
		Amount,
		ModifiedDate,
		ModifiedByID
	)
	Select @AggregationCycleID ,
	       tbl1.CallDate,
		   tbl1.CommercialTrunkID,
		   tbl1.ServiceLevelID,
		   tbl1.DestinationID,
		   tbl1.CallTypeID,
		   tbl1.Minutes,
		   tbl2.Rate , 
		   convert(Decimal(19,4) , tbl1.Minutes * tbl2.Rate),
		   getdate(),
		   -1
	from #TempDailyTrafficSummaryDetail tbl1
	inner join CTE_BlendedRatePerCallDate2 tbl2 on tbl1.CallDate = tbl2.CallDate
	Where tbl1.CallDate between @AggregationStartDate and @AggregationEndDate


End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! Deleting and Refreshing Aggregation Summary Report for Aggregation cycle : (' + 
						    convert(varchar(10) , @AggregationcycleID) + ').' + ERROR_MESSAGE()

	set @ResultFlag = 1

	Rollback Transaction UpdateAggregationSummary

	GOTO ENDPROCESS

End Catch

Commit Transaction UpdateAggregationSummary

---- DEBUG STATEMENT
--Select * from tb_AggregateSummary
--where AggregationCycleID = @AggregationCycleID

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryRateTierAllocation
--where AggregationCycleID = @AggregationCycleID
--order by TierID

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryRateDetails
--where AggregationCycleID = @AggregationCycleID
--order by CallDate

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryTraffic
--where AggregationCycleID = @AggregationCycleID
--order by calldate


	  
ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGrouping') )
	Drop table #TempAggregationGrouping

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficSummaryDetail') )
	Drop table #TempDailyTrafficSummaryDetail

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailySummaryCummulative') )
	Drop table #TempDailySummaryCummulative

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRatingDetails') )
	Drop table #TempAggregationRatingDetails

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempTierAllocationDetails') )
	Drop table #TempTierAllocationDetails

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyINUnionOutFinancial') )
	Drop table #TempDailyINUnionOutFinancial
GO
/****** Object:  StoredProcedure [dbo].[SP_BSAggregateTrafficForCommitmentWithGraceTypeCycle]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_BSAggregateTrafficForCommitmentWithGraceTypeCycle]
(
    @AggregationCycleID int,
    @ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As


set @ErrorDescription = NULL
set @ResultFlag = 0

--------------------------------------------------------------------
-- This logic is for commitment based aggregations where the Traffic
-- minutes are aggregated based on the aggregation cycle and grace
-- period.
-- Ensure that the aggregation passed is of the following two types:
-- 1. "Amount Commitment"
-- 2. "Volume Commitment"
--------------------------------------------------------------------
if not exists (select 1 from Tb_AggregationCycle where AggregationCycleID = @AggregationCycleID and AggregationtypeID in (-1,-2 ,-6,-7))
Begin

		set @ErrorDescription = 'ERROR !!!! The Aggregation either does not exist in the system or is not of the type Amount Commitment or Volume Commitment with Grace Period'
		set @ResultFlag = 1
		GOTO ENDPROCESS

End

-------------------------------------------------------------------
-- Get essential details regarding cycle period , grace period,
-- Aggregation type and Commitment Minutes
-------------------------------------------------------------------
Declare @CycleStartDate date,
        @CycleEndDate date,
		@GracePeriodEndDate date,
		@GracePeriodStartDate date,
		@Commitment Decimal(19,4),
		@AggregationTypeID int,
		@AggregationCriteriaID int,
		@AggregationID int,
		@AccountID int,
		@CommittedMinutes Decimal(19,4),
		@CommittedAmount Decimal(19,4),
		@AggregationRunDate date,
		@ShortfallMinutes Decimal(19,4) = 0,
        @PenaltyAmount Decimal(19,4) = 0,
		@AggregationBlendedRate Decimal(19,6) = 0,
		@AggregationBlendedRateAfterPenalty Decimal(19,6) = 0

-- Set the Aggregation Run Date to Current - 1
set @AggregationRunDate = DateAdd(dd , -1 , convert(date , getdate()))

Select @CycleStartDate = StartDate,
       @CycleEndDate = EndDate,
	   @GracePeriodEndDate = GracePeriodEndDate,
	   @Commitment = Commitment,
	   @AggregationTypeID = AggregationtypeID,
	   @AggregationCriteriaID = AggregationCriteriaID,
	    @AggregationID = AggregationID
from Tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

------------------------------------------------------
-- Get the Direction for the aggregation to decide
-- if we need to aggregate on settlement (inbound)
-- or routing (outbound) destinations
------------------------------------------------------
Declare @DirectionID int

Select @DirectionID = tbl1.directionID,
       @AccountID = tbl2.AccountID
from Tb_Aggregation tbl1
inner join tb_Agreement tbl2 on tbl1.AgreementID = tbl2.AgreementID
where AggregationID = @AggregationID

-----------------------------------------------------
-- Set the grace period start date as one day after
-- Aggregation cycle end date
-----------------------------------------------------
set @GracePeriodStartDate = dateAdd(dd ,1 , @CycleEndDate)

------------------------------------------------------------
-- If the Aggregation is of the type "Amount Commitment"
-- then we need to find the Minutes based on the Amount 
-- and Rate Structure
------------------------------------------------------------
if (@AggregationTypeID in (-2 , -7 ))
Begin

		Begin Try

					set @ErrorDescription = NULL
					set @ResultFlag = 0

					Exec SP_BSGetAggregationCommitmentMinutes @AggregationCycleID , @CommittedMinutes Output,
					                                          @ErrorDescription Output, @ResultFlag Output

					if (@ResultFlag = 1) -- In case of exception, abort any further processing
					Begin
							set @ErrorDescription = @ErrorDescription + '. ' + 
							                       'Exception happened when calculating Commitment Minutes for Amount based aggregation'
							GOTO ENDPROCESS
					End

		End Try

		Begin Catch

					set @ErrorDescription = 'ERROR !!!! When extracting Committed Minutes for Amount based Aggregation. ' + ERROR_MESSAGE()
					set @ResultFlag = 1
					GOTO ENDPROCESS

		End Catch

		set @CommittedAmount = @Commitment

End

------------------------------------------------------------
-- If the Aggregation is of the type "Volume Commitment"
-- then we need to find the Amount based on the Minutes 
-- and Rate Structure
------------------------------------------------------------
if (@AggregationTypeID in ( -1 , -6) )
Begin

		Begin Try

					set @ErrorDescription = NULL
					set @ResultFlag = 0

					Exec SP_BSGetAggregationCommitmentAmount @AggregationCycleID , @CommittedAmount Output,
					                                          @ErrorDescription Output, @ResultFlag Output

					if (@ResultFlag = 1) -- In case of exception, abort any further processing
					Begin
							set @ErrorDescription = @ErrorDescription + '. ' + 
							                       'Exception happened when calculating Commitment Amount for Minutes based aggregation'
							GOTO ENDPROCESS
					End

		End Try

		Begin Catch

					set @ErrorDescription = 'ERROR !!!! When extracting Committed Amount for Minutes based Aggregation. ' + ERROR_MESSAGE()
					set @ResultFlag = 1
					GOTO ENDPROCESS

		End Catch

		set @CommittedMinutes = @Commitment

End

----------------------------------------------------
-- Check whether this is an ACTIVE or an INACTIVE
-- aggregation based on the Aggregation Run Date (T - 1) 
-- and the Aggregation + Grace Period
-----------------------------------------------------
Declare @IsAggregationActiveFlag int = 1 -- Default to Active

-- For Commitment based aggregations we compare current date to grace period end date
if (@GracePeriodEndDate < @AggregationRunDate)
	set @IsAggregationActiveFlag = 0 -- Aggregation is no longer active


-- DEBUG STATEMENT
--Select @CycleStartDate CycleStartDate , 
--       @CycleEndDate CycleEndDate,
--       @GracePeriodStartDate GracePeriodStartDate , 
--	   @GracePeriodEndDate GracePeriodEndDate, 
--	   @AggregationTypeID AggregationTypeID,
--	   @AggregationCriteriaID AggregationCriteriaID, 
--	   @CommittedMinutes CommittedMinutes,
--	   @CommittedAmount CommittedAmount,
--	   @IsAggregationActiveFlag IsAggregationActiveFlag

-----------------------------------------------------------
-- Get the aggregation grouping for the cycle in a temp
-- table
-----------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGrouping') )
	Drop table #TempAggregationGrouping

Select agrp.*
into #TempAggregationGrouping
from Tb_AggregationGrouping agrp
inner join Tb_AggregationCycle agcyc on agrp.AggregationID = agcyc.AggregationID
where agcyc.AggregationCycleID = @AggregationCycleID

----------------------------------------------------------------------------
-- Get the traffic from Summarization mart at call date level, based on the 
-- aggregation grouping criteria
----------------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyINUnionOutFinancial') )
	Drop table #TempDailyINUnionOutFinancial

Select CallDate , CommercialTrunkID , INServiceLevelID,CallDuration,
       SettlementDestinationID , RoutingDestinationID, CallTypeID
into #TempDailyINUnionOutFinancial
From ReportServer.Uc_Report.dbo.tb_DailyINUnionOutFinancial
Where AccountID = @AccountID
and DirectionID = @DirectionID
and CallDate Between @CycleStartDate and Case 
                                            When @IsAggregationActiveFlag = 1 Then @AggregationRunDate 
											Else @GracePeriodEndDate
										 End
and CallDuration > 0

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficSummaryDetail') )
	Drop table #TempDailyTrafficSummaryDetail

Select convert(date ,CallDate) as CallDate , summ.CommercialTrunkID , agrp.ServiceLevelID,
       agrp.DestinationID , agrp.CalltypeID , convert(Decimal(19,4) ,sum(Callduration)/60.0) as Minutes
into #TempDailyTrafficSummaryDetail
from #TempDailyINUnionOutFinancial summ
inner join #TempAggregationGrouping agrp on
									summ.CommercialTrunkID = agrp.CommercialTrunkID
								and
									summ.INServiceLevelID = agrp.ServiceLevelID
								and
									Case
										When @DirectionID = 1 Then summ.SettlementDestinationID
										When @DirectionID = 2 Then summ.RoutingDestinationID
									End  = agrp.DestinationID
								and
									summ.CalltypeID = agrp.CallTypeID
Where summ.CallDate between agrp.StartDate and isnull(agrp.EndDate , summ.CallDate)

Group by CallDate , summ.CommercialTrunkID , agrp.ServiceLevelID,
       agrp.DestinationID , agrp.CalltypeID;

------------------------------------------------------------------------
-- Remove records for CallDate, where the grouping of Commercial Trunk,
-- Service Level , Destination and Call Type have been used for satisfying
-- different aggregation cycle
------------------------------------------------------------------------
Delete tbl1
from #TempDailyTrafficSummaryDetail tbl1
inner join tb_AggregateSummaryTraffic tbl2 on tbl1.CallDate = tbl2.CallDate
											and
											  tbl1.CommercialTrunkID = tbl2.CommercialTrunkID
											and
											  tbl1.ServiceLevelID = tbl2.ServiceLevelID
											and
											  tbl1.DestinationID = tbl2.DestinationID
											and
											  tbl1.CallTypeID =  tbl2.CalltypeID
where tbl2.AggregationCycleID <> @AggregationCycleID -- Important to ensure that records of same aggregation cycle are not considered

------------------------------------------------------------------------
-- Summarize the data by Call Data for qualifying records and calculate 
-- the cumulative traffic minutes for each call date
------------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailySummaryCummulative') )
	Drop table #TempDailySummaryCummulative;

With CTE_SummaryByCallDate As
(
	Select CallDate , convert(Decimal(19,4) ,sum(Minutes)) as Minutes
	from #TempDailyTrafficSummaryDetail
	Group by CallDate
)
Select  CallDate , Minutes , sum(Minutes) over (order by CallDate) as CummulativeMinutes
into #TempDailySummaryCummulative
from CTE_SummaryByCallDate

-- DEBUG STATEMENT
--Select * from #TempDailySummaryCummulative

--------------------------------------------------------
-- Check the aggregation criteria and on basis of that
-- decide whether we need to aggregate till end of
-- commitment or end of cycle
--------------------------------------------------------
Declare @AggregationEndDate date,
        @AggregationStartDate date,
        @TrafficAggregatedInGracePeriod Decimal(19,4) = 0,
		@TrafficAggregatedInCycle Decimal(19,4) = 0,
		@TrafficAggregatedOverCommitment Decimal(19,4) = 0,
		@TotalTrafficAggregated Decimal(19,4) = 0,
		@TotalAmountAggregated Decimal(19,4) = 0

------------------------------------------------------------
--  Get the actual date when aggregation of traffic started
------------------------------------------------------------
Select @AggregationStartDate = min(CallDate)
from #TempDailySummaryCummulative

-------------------------------------------------------------
-- ******************** SCENARIO 1 **************************
-------------------------------------------------------------
-- Process flow when the committed minutes for the aggregation 
-- are met 
-------------------------------------------------------------
if ( (select count(*) from #TempDailySummaryCummulative where CummulativeMinutes >= @CommittedMinutes) > 0 )
Begin

			-------------------------------------------------------------
			-- Get the Call Date where the Cumulative Minutes went above 
			-- the Committed minutes
			-------------------------------------------------------------
			Select @AggregationEndDate = CallDate 
			from #TempDailySummaryCummulative
			where CallDate = (
								Select min(CallDate)
								from #TempDailySummaryCummulative
								where CummulativeMinutes >= @CommittedMinutes
				             )

			-- DEBUG STATEMENT
			--Select @AggregationEndDate as AggregationEndDate
			-----------------------------------------------------------------
			-- Check if the aggregation completion date is greater than
			-- Aggregation cycle end date. If its greater then it means that
			-- aggregation was completed in the grace period
			-----------------------------------------------------------------
			if (@AggregationEndDate > @CycleEndDate )
			Begin

					----------------------------------------------------------------
					-- Calculate the total minutes that have been aggregated in the
					-- grace period
					----------------------------------------------------------------
					Select @TrafficAggregatedInGracePeriod = sum(Minutes)
					from #TempDailyTrafficSummaryDetail
					where CallDate between @GracePeriodStartDate and @AggregationEndDate

					----------------------------------------------------------------
					-- The total minutes aggregated in the cycle would be from
					-- Aggregation Start Date to Cycle end Date
					----------------------------------------------------------------
					if (@AggregationStartDate > @CycleEndDate ) -- Indicative that no Traffic was aggregated in the cycle period
					Begin

							set @TrafficAggregatedInCycle = 0

					End

					Else -- Indicative that traffic was aggregated in the cycle period also
					Begin

							Select @TrafficAggregatedInCycle = sum(Minutes)
							from #TempDailyTrafficSummaryDetail
							where CallDate between @AggregationStartDate and @CycleEndDate

					End

					---------------------------------------------
					-- Get the Total Traffic Minutes Aggregated 
					----------------------------------------------
					Select @TotalTrafficAggregated = CummulativeMinutes 
					from #TempDailySummaryCummulative
					where CallDate = @AggregationEndDate

			End

			---------------------------------------------------------
			-- This means that the aggregation was completed in the
			-- cycle period and none of the GP was utilized
			---------------------------------------------------------
			Else
			Begin

					-- Set the Traffic Aggregated in Grace Period to 0
					set @TrafficAggregatedInGracePeriod = 0

					--------------------------------------------------------------------
					-- Get the Traffic Minutes Aggregated within the cycle period
					-- Based on the aggregation criteria we decide whether to take the
					-- Aggregation End Date or Cycle End Date as the termination date for
					-- aggregation
					--------------------------------------------------------------------
					-- "Till Completion of Commitment" means that we take Aggregation End Date
					-- "Till Completion of Aggregation Cycle" means that we take Cycle End Date
					--------------------------------------------------------------------
					Select @TrafficAggregatedInCycle = sum(Minutes)
					from #TempDailyTrafficSummaryDetail
					where CallDate between @AggregationStartDate and 
					      Case
								When @AggregationCriteriaID = -1 Then @AggregationEndDate
								When @AggregationCriteriaID = -2 Then @CycleEndDate
						  End

					---------------------------------------------
					-- Get the Total Traffic Minutes Aggregated 
					----------------------------------------------
					set @TotalTrafficAggregated = @TrafficAggregatedInCycle + @TrafficAggregatedInGracePeriod

			End

			-------------------------------------------------------------------------
			-- If the aggregation criteria is "Till Completion of Aggregation Cycle"
			-- then we should :
			-- 1. Change to Aggregation Run Date if aggregation is Active
			-- 2. Change to Cycle End Date if traffic committment was met without
			--    grace period
			-- 3. Leave as unchanged if traffic committment was met in Grace Period
			-------------------------------------------------------------------------
			if (@AggregationCriteriaID = -2 and @TrafficAggregatedInGracePeriod = 0 ) -- "Till Completion of Aggregation Cycle"
				set @AggregationEndDate =  Case
												When @IsAggregationActiveFlag = 1 Then @AggregationRunDate
												When @IsAggregationActiveFlag = 0 Then @CycleEndDate
											End 

End

-------------------------------------------------------------
-- ******************** SCENARIO 2 **************************
-------------------------------------------------------------
-- Process flow when the committed minutes for the aggregation 
-- are not met and there is still shortfall
-------------------------------------------------------------
Else
Begin

		-------------------------------------------------
		-- Set the Aggregation End Date based on whether
		-- This is an expired aggregation or an active
		-- aggregation
		-------------------------------------------------
		set @AggregationEndDate = Case 
		                                -- Its an expired aggregation so we will aggregate till end of Grace Period
										When @IsAggregationActiveFlag = 0 Then @GracePeriodEndDate
										-- Its as active aggregation so we will aggregate till Aggregate Run Date
										When @IsAggregationActiveFlag =  1 Then @AggregationRunDate
								  End

			-----------------------------------------------------------------------------------------
			--           ************************ IMPORTANT NOTE *********************
			-----------------------------------------------------------------------------------------					     
			-- ***** The following logic should work for both active and expired aggregations ******
			-----------------------------------------------------------------------------------------

			-----------------------------------------------------------------
			-- Check if the aggregation End date is greater than
			-- Aggregation cycle end date. 
			-- If its greater then it means that
			-- 1.  For Active aggregation it means it is currently running 
			--     in grace period
			-- 2.  For Expired Aggregations it means grace period has also
			--     been utilized
			-----------------------------------------------------------------
			if (@AggregationEndDate > @CycleEndDate )
			Begin

					----------------------------------------------------------------
					-- Calculate the total minutes that have been aggregated in the
					-- grace period
					----------------------------------------------------------------
					Select @TrafficAggregatedInGracePeriod = isnull(sum(Minutes),0)
					from #TempDailyTrafficSummaryDetail
					where CallDate between @GracePeriodStartDate and @AggregationEndDate

					----------------------------------------------------------------
					-- The total minutes aggregated in the cycle would be from
					-- Aggregation Start Date to Cycle end Date
					----------------------------------------------------------------
					------------------------------------------------------------------------------------
					-- NOTE : IMPORTANT
					-- In scenarios when no traffic has been aggregated for the cycle, there will be no
					-- aggregation start date. In such cases we need to set the Traffic Aggregated in cycle
					-- to 0
					------------------------------------------------------------------------------------
					if (@AggregationStartDate is NULL)
					Begin

							set @TrafficAggregatedInCycle = 0

					End

					Else
					Begin
							if (@AggregationStartDate > @CycleEndDate ) -- Indicative that no Traffic was aggregated in the cycle period
							Begin

									set @TrafficAggregatedInCycle = 0

							End

							Else -- Indicative that traffic was aggregated in the cycle period also
							Begin

									Select @TrafficAggregatedInCycle = sum(Minutes)
									from #TempDailyTrafficSummaryDetail
									where CallDate between @AggregationStartDate and @CycleEndDate

							End
					End

			End

			----------------------------------------------------------------------------
			-- This means that:
			-- 1. For active aggregations we are still in the aggregation cycle period
			-- 2. For expired aggregations we will never come to this section
			----------------------------------------------------------------------------
			Else
			Begin

					-- Set the Traffic Aggregated in Grace Period to 0
					set @TrafficAggregatedInGracePeriod = 0

					--------------------------------------------------------------------
					-- For active aggregations calculate the Total Minutes aggregated in 
					-- the cycle based on Aggregation Start Date and Aggregation End Date
					--------------------------------------------------------------------
					Select @TrafficAggregatedInCycle = isnull(sum(Minutes),0)
					from #TempDailyTrafficSummaryDetail
					where CallDate between @AggregationStartDate and @AggregationEndDate

			End

			---------------------------------------------
			-- Get the Total Traffic Minutes Aggregated 
			----------------------------------------------
			set @TotalTrafficAggregated = @TrafficAggregatedInCycle + @TrafficAggregatedInGracePeriod
			

End		

----------------------------------------------------------
-- Get the total traffic aggregated over the commitement
----------------------------------------------------------
set @TrafficAggregatedOverCommitment = Case
											When @TotalTrafficAggregated >= @CommittedMinutes Then (@TotalTrafficAggregated - @CommittedMinutes)
											Else 0
									   End


----------------------------------------------------------------------------------------------
-- ***************** LOGIC TO GET THE RATING DETAILS FOR AGGREGATED AMOUNT *******************
----------------------------------------------------------------------------------------------

---------------------------------------------------------
-- Create the temp table to store all the Rating details
-- for the aggregated amount, based on the rate structure
---------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRatingDetails') )
	Drop table #TempAggregationRatingDetails

Create Table #TempAggregationRatingDetails
(
	TierID int,
	TierMinutes Decimal(19,4),
	TierAmount Decimal(19,4),
	TierRate Decimal(19,6)
)

------------------------------------------------------------------------
-- Call the Procedure the get the Rating details for Aggregated Amount
------------------------------------------------------------------------
Begin Try

	Exec SP_BSGetRatingDetailsForAggregatedAmount @AggregationCycleID , @TotalTrafficAggregated

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!!! While getting Rating details for Aggregated Traffic amount for Aggregation cycle :' +
							convert(varchar(10) ,@AggregationCycleID) + '. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

-- DEBUG STATEMENT
--Select sum(TierMinutes) as TotalTierMinutes , 
--       sum(TierAmount) as TotalTierAmount
--from #TempAggregationRatingDetails

-- DEBUG STATEMENT
--select * from #TempAggregationRatingDetails

------------------------------------------------------------------------------------------------------
-- ******** LOGIC TO ALLOCATE TIER OR BLENDED RATE FOR EACH CALL DATE BASED ON RATING DETAILS *******
------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------
-- Get the traffic from the Cumulative Summary schema that is actually
-- part of the aggregation for the cycle
----------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficMinutes') )
	Drop table #TempDailyTrafficMinutes

select *
into #TempDailyTrafficMinutes
from #TempDailySummaryCummulative
where CallDate between @AggregationStartDate and @AggregationEndDate

-- DEBUG
-- Select * from #TempDailyTrafficMinutes

--------------------------------------------------------------------
-- Create a Temporary schema to store the details related to 
-- Rate Tier and Rate allocation for traffic minutes on each
-- qualifying Call Date for aggregation
--------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempTierAllocationDetails') )
	Drop table #TempTierAllocationDetails

Create Table #TempTierAllocationDetails
(
	CallDate date,
	TierID int,
	Rate Decimal(19,6),
	AggregatedMinutes Decimal(19,4),
	RatedAmount Decimal(19,4)

)

------------------------------------------------------------------------
-- Call the Procedure to allocate rate tier to each aggregate record
-- in the summary schema
------------------------------------------------------------------------
Begin Try

	Exec SP_BSGetRateTierAllocationForAggregation 

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!!! While allocating rate tiers to aggregated traffic for Aggregation cycle :' +
							convert(varchar(10) ,@AggregationCycleID) + '. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

---------------------------------------------------
-- Get the total amount for the aggregated minutes
---------------------------------------------------
Select @TotalAmountAggregated  = isnull(sum(RatedAmount) ,0)
from #TempTierAllocationDetails

-------------------------------------------------------
-- Check if the aggregation has a shortfall and get
-- the shortfall minutes and amount. We would need to
-- only do this for aggregations that have reached their
-- end and are no longer active
-------------------------------------------------------
if (@IsAggregationActiveFlag = 0)
Begin

		if ( (@AggregationTypeID in (-1,-6)) and (@TotalTrafficAggregated < @CommittedMinutes)) -- Volume Committment with shortfall
		Begin

				set @ShortfallMinutes = @CommittedMinutes - @TotalTrafficAggregated
				set @PenaltyAmount = Case
										When @AggregationTypeID = -1 Then @CommittedAmount - @TotalAmountAggregated
										Else 0
									 End
				

		End

		if ( (@AggregationTypeID in (-2,-7)) and (@TotalAmountAggregated < @CommittedAmount)) -- Amount Committment with shortfall
		Begin

				set @ShortfallMinutes = @CommittedMinutes - @TotalTrafficAggregated
				set @PenaltyAmount = Case
										When @AggregationTypeID = -2 then @CommittedAmount - @TotalAmountAggregated
										Else 0
									 End
				

		End

End

-----------------------------------------------------------
-- Calculate the Aggregation Blended Rate and Aggregation 
-- Blended Rate After Penalty. The Penalty rate will only be
-- calculated for those aggregations that have expired and
-- have not met their committment
-----------------------------------------------------------

--------------------------------
-- AGGREGATION BLENDED RATE
--------------------------------
set @AggregationBlendedRate = Case
									When @TotalTrafficAggregated > 0  Then convert(Decimal(19,6) , @TotalAmountAggregated/@TotalTrafficAggregated)
									Else NULL
							  End



------------------------------------------
-- AGGREGATION BLENDED RATE AFTER PENALTY
------------------------------------------
if ( (@IsAggregationActiveFlag = 0)) -- Expired Aggregation so we calculate the Blended Penalty Rate based on whether the penalty amount is > 0 or not
Begin

	set @AggregationBlendedRateAfterPenalty = Case
													When @PenaltyAmount > 0 then 
														Case
															When @TotalTrafficAggregated > 0  Then convert(Decimal(19,6) , @CommittedAmount/@TotalTrafficAggregated)
															Else NULL
														End 													
													Else @AggregationBlendedRate
											  End
End

-- DEBUG STATEMENT
--Select @AggregationStartDate AggregationStartDate,
--       @AggregationEndDate AggregationEndDate,
--	   @TrafficAggregatedInCycle TrafficAggregatedInCycle,
--	   @TrafficAggregatedInGracePeriod TrafficAggregatedInGracePeriod,
--	   @TotalTrafficAggregated TotalTrafficAggregated,
--	   @TotalAmountAggregated TotalAmountAggregated,
--	   @TrafficAggregatedOverCommitment TrafficAggregatedOverCommitment,
--	   convert(Decimal(19,2) ,(@TotalTrafficAggregated/@CommittedMinutes) * 100) as TrafficAggregatedInPercent,
--	   @ShortfallMinutes ShortfallMinutes,
--	   @PenaltyAmount PenaltyAmount,
--	   @AggregationBlendedRate AggregationBlendedRate,
--	   @AggregationBlendedRateAfterPenalty AggregationBlendedRateAfterPenalty

---------------------------------------------------------------------------
-- ***************** REFRESH DATA IN THE AGGREGATE SCHEMA *****************
---------------------------------------------------------------------------
Begin Transaction UpdateAggregationSummary

Begin Try

	------------------------------------------------------
	-- Delete all data from the Aggregate Summary Schema 
	-- and insert refreshed data
	------------------------------------------------------
	Delete from tb_AggregateSummary
	where AggregationcycleID = @AggregationCycleID

	------------------------------------------------------
	-- Insert refreshed data into the Aggregate Summary
	-- schema for the aggregation cycle
	------------------------------------------------------
	insert into tb_AggregateSummary
	(
		AggregationCycleID,
		CycleStartDate,
		CycleEndDate,
		GracePeriodStartDate,
		GracePeriodEndDate,
		CommittedMinutes,
		CommittedAmount,
		IsAggregationActive,
		AggregationStartDate,
		AggregationEndDate,
		TrafficAggregatedInCycle,
		TrafficAggregatedInGracePeriod,
		TotalTrafficAggregated,
		TotalAmountAggregated,
		TrafficAggregatedOverCommitment,
		TrafficAggregatedInPercent,
		ShortfallMinutes,
		PenaltyAmount,
		AggregationBlendedRate,
		AggregationBlendedRateAfterPenalty,
		ModifiedDate,
		ModifiedByID
	)
	values
	(
		@AggregationCycleID,
		@CycleStartDate,
		@CycleEndDate,
		@GracePeriodStartDate,
		@GracePeriodEndDate,
		@CommittedMinutes,
		@CommittedAmount,
		@IsAggregationActiveFlag,
		@AggregationStartDate,
		Case
		    When @TotalTrafficAggregated = 0 then NULL
			Else @AggregationEndDate
		End,
		isnull(@TrafficAggregatedInCycle , 0),
		isnull(@TrafficAggregatedInGracePeriod,0),
		@TotalTrafficAggregated,
		@TotalAmountAggregated,
		@TrafficAggregatedOverCommitment,
		convert(Decimal(19,2) ,(@TotalTrafficAggregated/@CommittedMinutes) * 100),
		@ShortfallMinutes,
		@PenaltyAmount,
		@AggregationBlendedRate,
		@AggregationBlendedRateAfterPenalty,
		getdate(),
		-1
	)

	------------------------------------------------
	-- Delete all Rate tier allocation data for 
	-- aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryRateTierAllocation
	where AggregationCycleID = @AggregationCycleID

	-------------------------------------------------
	-- Insert refreshed data for Rate Tier Allocation
	-- for the Aggregation cycle
	-------------------------------------------------
	Insert into tb_AggregateSummaryRateTierAllocation
	(
			AggregationCycleID,
			StartDate,
			EndDate,
			TierID,
			AggregatedMinutes,
			RatedAmount,
			Rate,
			ModifiedDate,
			ModifiedByID
	)
	Select @AggregationCycleID,
		   min(CallDate),
		   max(CallDate),
		   TierID,
		   sum(AggregatedMinutes),
		   sum(RatedAmount),
		   Rate,
		   getdate(),
		   -1
	from #TempTierAllocationDetails
	Group By TierID , Rate
	order by TierID

	------------------------------------------------
	-- Delete all Rate details data for aggregated
	-- summary schema for aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryRateDetails
	where AggregationCycleID = @AggregationCycleID

	-------------------------------------------------
	-- Insert refreshed rate details data for the 
	-- Aggregation cycle
	-------------------------------------------------
	Insert into tb_AggregateSummaryRateDetails
	(
		AggregationCycleID,
		CallDate,
		TierID,
		AggregatedMinutes,
		RatedAmount,
		Rate,
		ModifiedDate,
		ModifiedByID
	)
	Select @AggregationCycleID,
		   CallDate,
		   TierID,
		   AggregatedMinutes,
		   RatedAmount,
		   Rate,
		   getdate(),
		   -1
	from #TempTierAllocationDetails
	Order By CallDate

	------------------------------------------------
	-- Delete all traffic details data for aggregated
	-- summary schema for aggregation cycle
	------------------------------------------------
	Delete from tb_AggregateSummaryTraffic
	where AggregationCycleID = @AggregationCycleID;

	-------------------------------------------------
	-- Insert refreshed traffic details data for the 
	-- Aggregation cycle in summary schema
	-------------------------------------------------
	With CTE_BlendedRatePerCallDate As
	(
		Select CallDate , 
			   Count(TierID) as NumTiers ,
			   convert(Decimal(19,6) , Sum(RatedAmount)/Sum(AggregatedMinutes)) as BlendedRate
		From #TempTierAllocationDetails
		group by CallDate
	),
	CTE_BlendedRatePerCallDate2 As
	(
		Select Distinct tbl1.CallDate , 
					   Case
							When tbl2.NumTiers > 1 Then tbl2.BlendedRate
							Else tbl1.Rate
					   End as Rate		
		from #TempTierAllocationDetails tbl1
		inner join CTE_BlendedRatePerCallDate tbl2 on tbl1.Calldate = tbl2.CallDate
	)
	insert into tb_AggregateSummaryTraffic
	(
		AggregationCycleID,
		CallDate,
		CommercialTrunkID,
		ServiceLevelID,
		DestinationID,
		CallTypeID,
		Minutes,
		Rate,
		Amount,
		ModifiedDate,
		ModifiedByID
	)
	Select @AggregationCycleID ,
	       tbl1.CallDate,
		   tbl1.CommercialTrunkID,
		   tbl1.ServiceLevelID,
		   tbl1.DestinationID,
		   tbl1.CallTypeID,
		   tbl1.Minutes,
		   tbl2.Rate , 
		   convert(Decimal(19,4) , tbl1.Minutes * tbl2.Rate),
		   getdate(),
		   -1
	from #TempDailyTrafficSummaryDetail tbl1
	inner join CTE_BlendedRatePerCallDate2 tbl2 on tbl1.CallDate = tbl2.CallDate
	Where tbl1.CallDate between @AggregationStartDate and @AggregationEndDate


End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! Deleting and Refreshing Aggregation Summary Report for Aggregation cycle : (' + 
						    convert(varchar(10) , @AggregationcycleID) + ').' + ERROR_MESSAGE()

	set @ResultFlag = 1

	Rollback Transaction UpdateAggregationSummary

	GOTO ENDPROCESS

End Catch

Commit Transaction UpdateAggregationSummary

---- DEBUG STATEMENT
--Select * from tb_AggregateSummary
--where AggregationCycleID = @AggregationCycleID

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryRateTierAllocation
--where AggregationCycleID = @AggregationCycleID
--order by TierID

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryRateDetails
--where AggregationCycleID = @AggregationCycleID
--order by CallDate

---- DEBUG STATEMENT
--Select * from tb_AggregateSummaryTraffic
--where AggregationCycleID = @AggregationCycleID
--order by calldate


	  
ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGrouping') )
	Drop table #TempAggregationGrouping

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyTrafficSummaryDetail') )
	Drop table #TempDailyTrafficSummaryDetail

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailySummaryCummulative') )
	Drop table #TempDailySummaryCummulative

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRatingDetails') )
	Drop table #TempAggregationRatingDetails

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempTierAllocationDetails') )
	Drop table #TempTierAllocationDetails

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDailyINUnionOutFinancial') )
	Drop table #TempDailyINUnionOutFinancial
GO
/****** Object:  StoredProcedure [dbo].[SP_BSFindVolumeForAmountCommitmentAggregationType]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_BSFindVolumeForAmountCommitmentAggregationType]
(
	@AggregationCycleID int,
	@CommittedTrafficMinutes Decimal(19,4) output,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0
set @CommittedTrafficMinutes = 0

Declare @CommitmentAmount Decimal(19,4)

Declare @TierVolumeRange Decimal(19,4) = 0,
		@TierRequiredMinutesForCommitment Decimal(19,4) = 0,
		@TierAmount Decimal(19,4) = 0,
        @TierID int = 1,
        @TierRate Decimal(19,4) = 0,
		@TierFrom int,
		@TierTo int,
		@ApplyFrom int,
		@RemainingAmount Decimal(19,4),
		@MaxTierID int

--------------------------------------------------------
-- Check to see if the Aggregation type for the cycle 
-- is either :
-- Amount Commitment
-- Amount Soft Swap
--------------------------------------------------------
if not exists (Select 1 from tb_AggregationCycle where AggregationCycleID = @AggregationCycleID and AggregationtypeID in (-2,-4))
Begin

	set @ErrorDescription = 'ERROR !!!! The Aggregation cycle does no exist or is not of the type Amount Commitment or Amount Soft Swap'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

--------------------------------------------------------
-- Find the Committed Amount for the Aggregation cycle
--------------------------------------------------------
Select @CommitmentAmount = Commitment
from tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

--Select 'Debug...' , @CommitmentAmount as Commitmentamount

----------------------------------------------------------
-- Get the Aggregation Rate Structure associated with 
-- the concerned Aggregation cycle
----------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRateDetail') )
		Drop table #TempAggregationRateDetail

select tbl1.*
into #TempAggregationRateDetail
from tb_AggregationRateDetail tbl1
inner join tb_AggregationRate tbl2 on tbl1.AggregationRateID = tbl2.AggregationRateID
Where tbl2.AggregationcycleID = @AggregationCycleID

--Select 'Debug...'
--Select * from #TempAggregationRateDetail

--------------------------------------------------------------------------------------------
-- **************** LOGIC TO FIND MINUTES CORRESPONDING TO COMMITTED AMOUNT ****************
--------------------------------------------------------------------------------------------

-------------------------------------------------------
-- Remaining Amount tracks the total committed amount
-- left for which corresponding minutes have not
-- been found
-------------------------------------------------------
set @RemainingAmount = @CommitmentAmount -- Initialize to Commitment Amount

---------------------------------------------------------------
-- Find out the maximum number of Tiers in the rate structure
---------------------------------------------------------------
Select @MaxTierID = max(TierID)
from #TempAggregationRateDetail

--------------------------------------------------------------------
-- Loop through the rate structure tiers till all the Tiers are not
-- processed or the Remaining amount becomes 0
--------------------------------------------------------------------
Begin Try

	While ( (@TierID <= @MaxTierID) and (@RemainingAmount <> 0) )
	Begin

				set @TierVolumeRange = 0 -- initialize it to 0 and later reset it based on whether its last tier or not
				set @TierRequiredMinutesForCommitment = 0
				set @TierAmount = 0

				Select @ApplyFrom = ApplyFrom,
					   @TierRate = Rate,
					   @TierFrom = [From],
					   @TierTo = [To]
				from #TempAggregationRateDetail
				where TierID = @TierID

				--------------------------------------------------
				-- Initialize the running paramters based on the
				-- value of ApplyFrom flag
				--------------------------------------------------

				if(@ApplyFrom = 0 ) -- Apply from the beginning
				Begin

					set @RemainingAmount = @CommitmentAmount
					set @CommittedTrafficMinutes = 0

				End

				if(@TierID <> @MaxTierID) -- This is not the last Tier, so the range will not be open ended
				Begin

						if (@ApplyFrom = 0) -- Apply from beginning
						Begin

							Select @TierVolumeRange = sum([To] - [From])
							from #TempAggregationRateDetail
							where TierID <= @TierID

						End

						if (@ApplyFrom = 1) -- Apply Incremental
						Begin

							set @TierVolumeRange = @TierTo - @TierFrom

						End

				End

				------------------------------------------------------------------------------
				-- Calculate the volume required from the Tier to meet the Amount Committment
				------------------------------------------------------------------------------

				set @TierRequiredMinutesForCommitment = @RemainingAmount/@TierRate

				if(@TierID = @MaxTierID) -- This is the last tier of the rate structure
				Begin

						if(@ApplyFrom = 0)
						Begin

							set @CommittedTrafficMinutes = @TierRequiredMinutesForCommitment
							set @RemainingAmount = 0

						End

						Else
						Begin

							set @CommittedTrafficMinutes = @CommittedTrafficMinutes + @TierRequiredMinutesForCommitment
							set @RemainingAmount = 0

						End

				End

				Else -- This is not the last tier of the rate structure
				Begin

						set @TierAmount = @TierVolumeRange * @TierRate

						---------------------------------------------------------------
						-- If the calculated volume is more than the Tier range then
						-- its indicative that the tier cannot suffice all the volume
						-- for amount committed
						---------------------------------------------------------------

						if(@TierRequiredMinutesForCommitment > @TierVolumeRange)
						Begin

								set @RemainingAmount = @RemainingAmount - @TierAmount
								set @CommittedTrafficMinutes = Case 
																  When @ApplyFrom = 0 Then @TierVolumeRange
																  When @ApplyFrom = 1 Then @CommittedTrafficMinutes + @TierVolumeRange
															   End

						End

						Else
						Begin

								set @RemainingAmount = 0
								set @CommittedTrafficMinutes =  Case 
																  When @ApplyFrom = 0 Then @TierRequiredMinutesForCommitment
																  When @ApplyFrom = 1 Then @CommittedTrafficMinutes + @TierRequiredMinutesForCommitment
															   End

						End

				End

				--Select 'Debug.....'

				--Select @TierID as TierID , @RemainingAmount as Remainingamount,
				--	   @CommittedTrafficMinutes as CommittedTrafficMinutes,
				--	   @TierRequiredMinutesForCommitment as TierRequiredMinutesForCommitment,
				--	   @TierVolumeRange as TierVolumeRange,
				--	   @TierRate as TierRate,
				--	   @TierAmount as TierAmount,
				--	   Case
				--			When @ApplyFrom = 0 then 'Beginning'
				--			When @ApplyFrom = 1 then 'Incremental'
				--	   End as ApplyFrom

				set @TierID = @TierID + 1

	End

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! During calculation of corresponding Minutes for Amount Committed. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	set @CommittedTrafficMinutes = 0
	GOTO ENDPROCESS

End Catch


ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRateDetail') )
		Drop table #TempAggregationRateDetail
GO
/****** Object:  StoredProcedure [dbo].[SP_BSGetAggregationCommitmentAmount]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE     Procedure [dbo].[SP_BSGetAggregationCommitmentAmount]
(
	@AggregationCycleID int,
	@CommittedAmount Decimal(19,4) Output,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

Declare @CommittedMinutes Decimal(19,4)

set @CommittedAmount = 0

---------------------------------------------------------------------
-- Check if the aggregation cycle exosts in the system and 
-- is of the type : Volume Soft Swap (-3) or Volume Commitment (-1)
---------------------------------------------------------------------
if  not exists (select 1 from Tb_AggregationCycle where AggregationCycleID = @AggregationCycleID and AggregationtypeID in (-1,-3, -6, -8) )
Begin

		set @ErrorDescription = 'ERROR !!! The Aggregation cycle ID is either not valid or not of the type Volume Commitment'
		set @ResultFlag = 1
		GOTO ENDPROCESS
End

---------------------------------------------------
-- Get the essential details related to Commitment
-- Amount and rate structure from the aggregation
-- cycle
---------------------------------------------------

Select @CommittedMinutes = Commitment
from Tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRateDetail') )
	Drop table #TempAggregationRateDetail

Create Table #TempAggregationRateDetail
(
	TierID int,
	TierFrom int,
	TierTo int,
	ApplyFrom int,
	TierRate Decimal(19,6)
)

insert into #TempAggregationRateDetail
Select rtd.TierID , rtd.[From] , rtd.[To] , rtd.ApplyFrom , rtd.Rate
from Tb_AggregationRate rt
inner join Tb_AggregationRateDetail rtd on rt.AggregationRateID = rtd.AggregationRateID
where rt.AggregationCycleID = @AggregationCycleID


-- DEBUG STATEMENT
--Select * from #TempAggregationRateDetail

-------------------------------------------------------------------
-- *********** LOGIC FOR ESTABLISHING EACH TIER RANGE ********** --
-------------------------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationTierDetails') )
	Drop table #TempAggregationTierDetails

Create Table #TempAggregationTierDetails
(
	TierID int,
	TierRange int,
	TierRate Decimal(19,6),
	TierAmount Decimal(19,4),
	ApplyFrom int
)

Declare @TotalTiers int,
        @CurrentTier int = 1,
		@VarTierFrom int,
		@VarTierTo int,
		@VarTierRate Decimal(19,6),
		@VarApplyFrom int,
		@VarTierRange int,
		@VarTierAmount Decimal(19,4),
		@LastTierRate Decimal(19,6),
		@LastTierApplyFrom int

select @TotalTiers = count(*) from #TempAggregationRateDetail

While (@CurrentTier < @TotalTiers)
Begin

		Select @VarTierFrom = TierFrom,
		       @VarTierTo = TierTo,
			   @VarTierRate = TierRate,
			   @VarApplyFrom = ApplyFrom
		from #TempAggregationRateDetail
		Where TierID = @CurrentTier

		if ( @VarApplyFrom = 1) -- Apply from the 0th minute

			set @VarTierRange = @VarTierTo

		Else
			set @VarTierRange = @VarTierTo - @VarTierFrom

		set @VarTierAmount = @VarTierRange * @VarTierRate

		insert into #TempAggregationTierDetails values
		(@CurrentTier , @VarTierRange , @VarTierRate, @VarTierAmount , @VarApplyFrom)

		set @CurrentTier = @CurrentTier + 1

End

-- DEBUG STATEMENT
-- Select * from #TempAggregationTierDetails

-------------------------------------------------------------------
-- *********** LOGIC FOR CALCULATING COMMITTED MINUTES ********** --
-------------------------------------------------------------------
Declare @LeftMinutes Decimal(19,4)

set @LeftMinutes = @CommittedMinutes
set @CurrentTier = 1
select @TotalTiers = count(*) from #TempAggregationTierDetails

-- DEBUG STATEMENT
--select @CurrentTier as 'Current Tier' , @TotalTiers as 'Total Tiers'

While( (@CurrentTier <= @TotalTiers) and (@LeftMinutes > 0 ) )
Begin

	select @VarTierRange = TierRange,
		   @VarTierRate = TierRate,
		   @VarApplyFrom = ApplyFrom,
		   @VarTierAmount = TierAmount
	from #TempAggregationTierDetails
	where TierID = @CurrentTier

	if (@VarApplyFrom = 1) -- This means that we need apply this tier rate to all traffic minutes from beginning
	Begin

		-- Initialize the Left Amount to original committed Amount
		set @LeftMinutes = @CommittedMinutes

		-- Initialize the Committed Minutes calculated till now to zero
		set @CommittedAmount = 0

	End

	if (@VarTierAmount >= @LeftMinutes) -- This means that the tier has the capacity to fulfil the left over of the committed amount
	Begin

			set @CommittedAmount = @CommittedAmount + convert(Decimal(19,4),(@LeftMinutes * @VarTierRate))
			set @LeftMinutes = 0

	End

	Else
	Begin

			set @CommittedAmount = @CommittedAmount  + @VarTierAmount
			set @LeftMinutes = @LeftMinutes - @VarTierRange

	End

	-- DEBUG STATEMENT
	-- select @LeftMinutes as 'Left Minutes' , @CommittedAmount as 'Committed Amount'

	set @CurrentTier = @CurrentTier + 1

End

--------------------------------------------------
-- Check if last rate structure tier needs to be
-- used or not. If there is leftover Amount, then it
-- means that we need to utlilize the last tier, as
-- previous tiers were not able to meet all the 
-- requirement
--------------------------------------------------

if (@LeftMinutes > 0)
Begin

	select @LastTierRate = TierRate,
	       @LastTierApplyFrom = ApplyFrom
	from #TempAggregationRateDetail
	where TierID = @CurrentTier

	-- DEBUG STATEMENT
	-- Select @LastTierRate as 'Last Tier Rate' , @LastTierApplyFrom as 'Last Apply From'

	if (@LastTierApplyFrom  = 1) -- This means that we need apply this tier rate to all traffic minutes from beginning
			set @CommittedAmount = convert(Decimal(19,4) ,(@CommittedMinutes * @LastTierRate))


	Else
			set @CommittedAmount =  @CommittedAmount + convert(Decimal(19,4),(@LeftMinutes * @LastTierRate))

	set @LeftMinutes = 0

End
        
-- DEBUG STATEMENT
-- select @CommittedAmount as 'Committed Amount'     

ENDPROCESS: 

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRateDetail') )
	Drop table #TempAggregationRateDetail

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationTierDetails') )
	Drop table #TempAggregationTierDetails
GO
/****** Object:  StoredProcedure [dbo].[SP_BSGetAggregationCommitmentMinutes]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_BSGetAggregationCommitmentMinutes]
(
	@AggregationCycleID int,
	@CommittedMinutes Decimal(19,2) Output,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

Declare @CommittedAmount Decimal(19,4)

set @CommittedMinutes = 0

---------------------------------------------------------------------
-- Check if the aggregation cycle exosts in the system and 
-- is of the type : Amount Soft Swap (-4) or Amount Commitment (-2)
---------------------------------------------------------------------
if  not exists (select 1 from Tb_AggregationCycle where AggregationCycleID = @AggregationCycleID and AggregationtypeID in (-2,-4 ,-7, -9) )
Begin

		set @ErrorDescription = 'ERROR !!! The Aggregation cycle ID is either not valid or not of the type Amount Commitment'
		set @ResultFlag = 1
		GOTO ENDPROCESS
End

---------------------------------------------------
-- Get the essential details related to Commitment
-- Amount and rate structure from the aggregation
-- cycle
---------------------------------------------------

Select @CommittedAmount = Commitment
from Tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRateDetail') )
	Drop table #TempAggregationRateDetail

Create Table #TempAggregationRateDetail
(
	TierID int,
	TierFrom int,
	TierTo int,
	ApplyFrom int,
	TierRate Decimal(19,6)
)

insert into #TempAggregationRateDetail
Select rtd.TierID , rtd.[From] , rtd.[To] , rtd.ApplyFrom , rtd.Rate
from Tb_AggregationRate rt
inner join Tb_AggregationRateDetail rtd on rt.AggregationRateID = rtd.AggregationRateID
where rt.AggregationCycleID = @AggregationCycleID

-- DEBUG STATEMENT
-- Select * from #TempAggregationRateDetail

-------------------------------------------------------------------
-- *********** LOGIC FOR ESTABLISHING EACH TIER RANGE ********** --
-------------------------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationTierDetails') )
	Drop table #TempAggregationTierDetails

Create Table #TempAggregationTierDetails
(
	TierID int,
	TierRange int,
	TierRate Decimal(19,6),
	TierAmount Decimal(19,4),
	ApplyFrom int
)

Declare @TotalTiers int,
        @CurrentTier int = 1,
		@VarTierFrom int,
		@VarTierTo int,
		@VarTierRate Decimal(19,6),
		@VarApplyFrom int,
		@VarTierRange int,
		@VarTierAmount Decimal(19,4),
		@LastTierRate Decimal(19,6),
		@LastTierApplyFrom int

select @TotalTiers = count(*) from #TempAggregationRateDetail

While (@CurrentTier < @TotalTiers)
Begin

		Select @VarTierFrom = TierFrom,
		       @VarTierTo = TierTo,
			   @VarTierRate = TierRate,
			   @VarApplyFrom = ApplyFrom
		from #TempAggregationRateDetail
		Where TierID = @CurrentTier

		if ( @VarApplyFrom = 1) -- Apply from the 0th minute

			set @VarTierRange = @VarTierTo

		Else
			set @VarTierRange = @VarTierTo - @VarTierFrom

		set @VarTierAmount = @VarTierRange * @VarTierRate

		insert into #TempAggregationTierDetails values
		(@CurrentTier , @VarTierRange , @VarTierRate, @VarTierAmount , @VarApplyFrom)

		set @CurrentTier = @CurrentTier + 1

End

-- DEBUG STATEMENT
-- Select * from #TempAggregationTierDetails

-------------------------------------------------------------------
-- *********** LOGIC FOR CALCULATING COMMITTED MINUTES ********** --
-------------------------------------------------------------------
Declare @LeftAmount Decimal(19,4)

set @LeftAmount = @CommittedAmount
set @CurrentTier = 1
select @TotalTiers = count(*) from #TempAggregationTierDetails

-- DEBUG STATEMENT
--select @CurrentTier as 'Current Tier' , @TotalTiers as 'Total Tiers'

While( (@CurrentTier <= @TotalTiers) and (@LeftAmount > 0 ) )
Begin

	select @VarTierRange = TierRange,
		   @VarTierRate = TierRate,
		   @VarApplyFrom = ApplyFrom,
		   @VarTierAmount = TierAmount
	from #TempAggregationTierDetails
	where TierID = @CurrentTier

	if (@VarApplyFrom = 1) -- This means that we need apply this tier rate to all traffic minutes from beginning
	Begin

		-- Initialize the Left Amount to original committed Amount
		set @LeftAmount = @CommittedAmount

		-- Initialize the Committed Minutes calculated till now to zero
		set @CommittedMinutes = 0

	End

	if (@VarTierAmount >= @LeftAmount) -- This means that the tier has the capacity to fulfil the left over of the committed amount
	Begin

			set @CommittedMinutes = @CommittedMinutes + convert(Decimal(19,2),(@LeftAmount/@VarTierRate))
			set @LeftAmount = 0

	End

	Else
	Begin

			set @CommittedMinutes = @CommittedMinutes + @VarTierRange
			set @LeftAmount = @LeftAmount - @VarTierAmount

	End

	-- DEBUG STATEMENT
	-- select @LeftAmount as 'Left Amount' , @CommittedMinutes as 'Committed Minutes'

	set @CurrentTier = @CurrentTier + 1

End

--------------------------------------------------
-- Check if last rate structure tier needs to be
-- used or not. If there is leftover Amount, then it
-- means that we need to utlilize the last tier, as
-- previous tiers were not able to meet all the 
-- requirement
--------------------------------------------------

if (@LeftAmount > 0)
Begin

	select @LastTierRate = TierRate,
	       @LastTierApplyFrom = ApplyFrom
	from #TempAggregationRateDetail
	where TierID = @CurrentTier

	-- DEBUG STATEMENT
	-- Select @LastTierRate as 'Last Tier Rate' , @LastTierApplyFrom as 'Last Apply From'

	if (@LastTierApplyFrom  = 1) -- This means that we need apply this tier rate to all traffic minutes from beginning
			set @CommittedMinutes = convert(Decimal(19,2) ,(@CommittedAmount/@LastTierRate))


	Else
			set @CommittedMinutes =  @CommittedMinutes + convert(Decimal(19,2),(@LeftAmount/@LastTierRate))

	set @LeftAmount = 0

End
        
-- DEBUG STATEMENT
-- select @CommittedMinutes as 'Committed Minutes'     

ENDPROCESS: 

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRateDetail') )
	Drop table #TempAggregationRateDetail

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationTierDetails') )
	Drop table #TempAggregationTierDetails
GO
/****** Object:  StoredProcedure [dbo].[SP_BSGetRateTierAllocationForAggregation]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE    Procedure [dbo].[SP_BSGetRateTierAllocationForAggregation]
As

Declare @VarTierID int,
        @VarTierMinutes Decimal(19,4),
		@VarTierAmount Decimal(19,4),
		@VarTierRate Decimal(19,6),
		@EndCallDate date,
		@StartCallDate date,
		@AggregatedMinutes Decimal(19,4)

-------------------------------------------------------------------
-- Open a Cursor to loop through all the tiers of Rating Detail
-- and allocate rate to each call date based on the aggregated
-- amount on that date
-------------------------------------------------------------------

DECLARE db_RatingDetail_Cur CURSOR FOR 
select TierID , TierMinutes , TierAmount, TierRate
from #TempAggregationRatingDetails
order by TierID

OPEN db_RatingDetail_Cur
FETCH NEXT FROM db_RatingDetail_Cur 
INTO @VarTierID , @VarTierMinutes , @VarTierAmount , @VarTierRate

While @@FETCH_STATUS = 0   
Begin

	-------------------------------------------------------
	-- Find the CallDates and Aggregated Minutes which are
	-- less than the Rating Tier Minutes
	-------------------------------------------------------		
    Select @EndCallDate = max(CallDate),
		   @StartCallDate = min(CallDate),
		   @AggregatedMinutes = isnull(sum(Minutes),0)
	from #TempDailyTrafficMinutes
	where CummulativeMinutes < @VarTierMinutes

	-- DEBUG STATEMENT
	-- Select @StartCallDate , @EndCallDate , @AggregatedMinutes , @VarTierMinutes
	
	-- DEBUG STATEMENT
	-- Select * from #TempDailyTrafficMinutes

	----------------------------------------------------------
	-- Insert the Call Date Range along with Tier Rate and
	-- Aggregated Minutes into the Temporary Allocation details
	-- schema for later processing
	----------------------------------------------------------
	insert into #TempTierAllocationDetails 
	Select CallDate , 
	       @VarTierID , 
		   @VarTierRate , 
		   Minutes,
		   Minutes * @VarTierRate
	From #TempDailyTrafficMinutes
	where CallDate between @StartCallDate and  @EndCallDate

	---------------------------------------------------------
	-- Delete the records for the call date range from the
	-- Daily Traffic minutes schema as they have been allocated
	-- Tier for rating
	---------------------------------------------------------
	Delete from #TempDailyTrafficMinutes
	where CallDate between @StartCallDate and @EndCallDate

	-----------------------------------------------------------
	-- Get the Call Date where the aggregated minutes go over
	-- the Tier Minutes threshold and get the remaining
	-- traffic minutes required to complete the Tier Threshold
	-----------------------------------------------------------
    Select @StartCallDate = CallDate
	from #TempDailyTrafficMinutes
	where CallDate = ( 
						Select min(CallDate)
						from #TempDailyTrafficMinutes
						where CummulativeMinutes >= @VarTierMinutes
					 )

	insert into #TempTierAllocationDetails Values
	(
		@StartCallDate,
		@VarTierID , 
		@VarTierRate,	
        @VarTierMinutes - @AggregatedMinutes,
		(@VarTierMinutes - @AggregatedMinutes) * @VarTierRate
	)

	--------------------------------------------------------------
	-- Update the Minutes for the Call Date after removing the 
	-- required minutes for completing the Tier Threshold
	--------------------------------------------------------------
	update #TempDailyTrafficMinutes
	set Minutes = CummulativeMinutes - @VarTierMinutes
	where CallDate = @StartCallDate;

	------------------------------------------------------------
	-- Recalculate the Cumulative minutes for the available
	-- Call Date records in the Daily Traffic minutes Schema
	------------------------------------------------------------
	With CTE_RecalculateCumulativeMinutes As
	(
		Select  CallDate , Minutes , sum(Minutes) over (order by CallDate) as CummulativeMinutes
		From #TempDailyTrafficMinutes
	)
	update tbl1
	set CummulativeMinutes = tbl2.CummulativeMinutes
	from #TempDailyTrafficMinutes tbl1
	inner join CTE_RecalculateCumulativeMinutes tbl2 on tbl1.CallDate = tbl2.CallDate


 	FETCH NEXT FROM db_RatingDetail_Cur
    INTO @VarTierID , @VarTierMinutes , @VarTierAmount , @VarTierRate

End 
 
CLOSE db_RatingDetail_Cur  
DEALLOCATE db_RatingDetail_Cur

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_BSGetRatingDetailsForAggregatedAmount]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE    Procedure [dbo].[SP_BSGetRatingDetailsForAggregatedAmount]
(
	@AggregationCycleID int ,
	@AggregatedMinutes Decimal(19,4)
)
As

Declare @TotalAmount Decimal(19,4) = 0

---------------------------------------------------
-- Get the essential details related to Commitment
-- Amount and rate structure from the aggregation
-- cycle
---------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRateDetail') )
	Drop table #TempAggregationRateDetail

Create Table #TempAggregationRateDetail
(
	TierID int,
	TierFrom int,
	TierTo int,
	ApplyFrom int,
	TierRate Decimal(19,6)
)

insert into #TempAggregationRateDetail
Select rtd.TierID , rtd.[From] , rtd.[To] , rtd.ApplyFrom , rtd.Rate
from Tb_AggregationRate rt
inner join Tb_AggregationRateDetail rtd on rt.AggregationRateID = rtd.AggregationRateID
where rt.AggregationCycleID = @AggregationCycleID


-- DEBUG STATEMENT
--Select * from #TempAggregationRateDetail

-------------------------------------------------------------------
-- *********** LOGIC FOR ESTABLISHING EACH TIER RANGE ********** --
-------------------------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationTierDetails') )
	Drop table #TempAggregationTierDetails

Create Table #TempAggregationTierDetails
(
	TierID int,
	TierRange int,
	TierRate Decimal(19,6),
	TierAmount Decimal(19,4),
	ApplyFrom int
)

Declare @TotalTiers int,
        @CurrentTier int = 1,
		@VarTierFrom int,
		@VarTierTo int,
		@VarTierRate Decimal(19,6),
		@VarApplyFrom int,
		@VarTierRange int,
		@VarTierAmount Decimal(19,4),
		@LastTierRate Decimal(19,6),
		@LastTierApplyFrom int

select @TotalTiers = count(*) from #TempAggregationRateDetail

While (@CurrentTier < @TotalTiers)
Begin

		Select @VarTierFrom = TierFrom,
		       @VarTierTo = TierTo,
			   @VarTierRate = TierRate,
			   @VarApplyFrom = ApplyFrom
		from #TempAggregationRateDetail
		Where TierID = @CurrentTier

		if ( @VarApplyFrom = 1) -- Apply from the 0th minute

			set @VarTierRange = @VarTierTo

		Else
			set @VarTierRange = @VarTierTo - @VarTierFrom

		set @VarTierAmount = @VarTierRange * @VarTierRate

		insert into #TempAggregationTierDetails values
		(@CurrentTier , @VarTierRange , @VarTierRate, @VarTierAmount , @VarApplyFrom)

		set @CurrentTier = @CurrentTier + 1

End

-- DEBUG STATEMENT
--Select * from #TempAggregationTierDetails

-----------------------------------------------------------------------------
-- *********** LOGIC FOR CALCULATING TOTAL AMOUNT FOR EACH TIER ********** --
-----------------------------------------------------------------------------
Declare @LeftMinutes Decimal(19,4)

set @LeftMinutes = @AggregatedMinutes
set @CurrentTier = 1

select @TotalTiers = count(*) from #TempAggregationTierDetails

-- DEBUG STATEMENT
--select @CurrentTier as 'Current Tier' , @TotalTiers as 'Total Tiers'

While( (@CurrentTier <= @TotalTiers) and (@LeftMinutes > 0 ) )
Begin

	select @VarTierRange = TierRange,
		   @VarTierRate = TierRate,
		   @VarApplyFrom = ApplyFrom,
		   @VarTierAmount = TierAmount
	from #TempAggregationTierDetails
	where TierID = @CurrentTier

	if (@VarApplyFrom = 1) -- This means that we need apply this tier rate to all traffic minutes from beginning
	Begin

		-- Initialize the Left Amount to original committed Amount
		set @LeftMinutes = @AggregatedMinutes

		-- Initialize the Committed Minutes calculated till now to zero
		set @TotalAmount = 0

		--------------------------------------------------
		-- Delete all the previous records stored in the
		-- rating detail table, since this tier can cater
		-- for minutes from previous tiers
		--------------------------------------------------
		Delete from #TempAggregationRatingDetails

	End

	if (@VarTierRange >= @LeftMinutes) -- This means that the tier has the capacity to fulfil the left over of the committed amount
	Begin

			set @TotalAmount = @TotalAmount + convert(Decimal(19,4),(@LeftMinutes * @VarTierRate))

			--------------------------------------------------
			-- Insert the record for new rating details
			--------------------------------------------------
			insert into #TempAggregationRatingDetails values 
			(
				@CurrentTier, 
				@LeftMinutes, 
				convert(Decimal(19,4),(@LeftMinutes * @VarTierRate)),
				@VarTierRate
			)

			set @LeftMinutes = 0

	End

	Else
	Begin

			set @TotalAmount = @TotalAmount  + @VarTierAmount
			set @LeftMinutes = @LeftMinutes - @VarTierRange

			--------------------------------------------------
			-- Insert the record for new rating details
			--------------------------------------------------
			insert into #TempAggregationRatingDetails values 
			(
				@CurrentTier, 
				@VarTierRange, 
				@VarTierAmount,
				@VarTierRate
			)

	End

	-- DEBUG STATEMENT
	-- select @LeftMinutes as 'Left Minutes' , @TotalAmount as 'Committed Amount'

	set @CurrentTier = @CurrentTier + 1

End

--------------------------------------------------
-- Check if last rate structure tier needs to be
-- used or not. If there is leftover Amount, then it
-- means that we need to utlilize the last tier, as
-- previous tiers were not able to meet all the 
-- requirement
--------------------------------------------------

if (@LeftMinutes > 0)
Begin

	select @LastTierRate = TierRate,
	       @LastTierApplyFrom = ApplyFrom
	from #TempAggregationRateDetail
	where TierID = @CurrentTier

	-- DEBUG STATEMENT
	-- Select @LastTierRate as 'Last Tier Rate' , @LastTierApplyFrom as 'Last Apply From'

	if (@LastTierApplyFrom  = 1) -- This means that we need apply this tier rate to all traffic minutes from beginning
	Begin
			set @TotalAmount = convert(Decimal(19,4) ,(@AggregatedMinutes * @LastTierRate))

			--------------------------------------------------
			-- Delete all the previous records stored in the
			-- rating detail table, since this tier can cater
			-- for minutes from previous tiers
			--------------------------------------------------
			Delete from #TempAggregationRatingDetails

	End


	Else
			set @TotalAmount =  @TotalAmount + convert(Decimal(19,4),(@LeftMinutes * @LastTierRate))

	--------------------------------------------------
	-- Insert the record for new rating details
	--------------------------------------------------
	insert into #TempAggregationRatingDetails values 
	(
		@CurrentTier, 
		Case 
			When @LastTierApplyFrom = 1 Then @AggregatedMinutes 
			Else @LeftMinutes 
		End, 
		Case 
			When @LastTierApplyFrom = 1 Then convert(Decimal(19,4) ,(@AggregatedMinutes * @LastTierRate)) 
			Else convert(Decimal(19,4),(@LeftMinutes * @LastTierRate)) 
		End, 
		@LastTierRate
	)

	set @LeftMinutes = 0

End
        
-- DEBUG STATEMENT
--select @TotalAmount as 'Total Amount'  


ENDPROCESS: 

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationRateDetail') )
	Drop table #TempAggregationRateDetail

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationTierDetails') )
	Drop table #TempAggregationTierDetails

GO
/****** Object:  StoredProcedure [dbo].[SP_BSRecalculateForAggregationCycle]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE      Procedure [dbo].[SP_BSRecalculateForAggregationCycle]
(
	@AggregationCycleID int,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

Declare @AccountID int,
        @DirectionID int,
        @AggregationRunDate date = DateAdd(dd , -1 , convert(date , getdate()));

---------------------------------------------------------------------
-- Get the Account for which the Aggregation cycle has requested
-- recalculation
---------------------------------------------------------------------
Select @AccountID = tbl3.AccountID,
       @DirectionID = tbl2.DirectionID
from Tb_AggregationCycle tbl1
inner join Tb_Aggregation tbl2 on tbl1.AggregationID =  tbl2.AggregationID
inner join tb_Agreement tbl3 on tbl2.AgreementID = tbl3.AgreementID
Where AggregationCycleID = @AggregationCycleID

--------------------------------------------------------------------
-- Get list of all the aggregation cycles in the order we want to 
-- process them based on the following paramters:
-- 1. Aggregation Cycle Start Date
-- 2. Aggregation Priority
-- 3. Aggregation Cycle Creation Date
---------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggrgegationsToRecalculate') )
	Drop table #TempAggrgegationsToRecalculate;

With CTE_AllAggregationsForAccount As
(
	select Row_Number() over (partition by tbl3.DirectionID order by tbl1.StartDate , tbl2.AggregationtypePriorityID , tbl1.CycleCreationDate) as RecordID,
		   tbl3.AggregationID,
		   tbl1.AggregationCycleID ,
		   tbl1.AggregationtypeID,
		   tbl1.StartDate, 
		   tbl1.EndDate,
		   tbl3.DirectionID,
		   Case
				When tbl1.AggregationtypeID in (-1,-2 , -6 , -7) then -- Grace Period based Agrgegations
					Case
						When tbl1.GracePeriodEndDate < @AggregationRunDate Then 0
						Else 1
					End
				Else -- Non Grace Period based Aggregations
					Case
						When tbl1.EndDate < @AggregationRunDate Then 0
						Else 1
					End			
		   End as IsAggregationActiveFlag,
		   Case
				When tbl1.AggregationtypeID in (-1,-2 , -6 , -7) then GracePeriodEndDate -- Grace Period based Agrgegations
				Else tbl1.EndDate -- Non Grace Period based Aggregations			
		   End as AggregationEndDate,
		   datediff(dd , StartDate ,@AggregationRunDate) as NumDaysPastCycleStart
	from Tb_AggregationCycle tbl1
	inner join Tb_AggregationtypePriority tbl2 on tbl1.AggregationtypeID = tbl2.AggregationtypeID
	inner join tb_Aggregation tbl3 on tbl1.AggregationID = tbl3.AggregationID
	inner join tb_Agreement tbl4 on tbl3.AgreementID = tbl4.AgreementID
	Where tbl4.AccountID = @AccountID	
	and tbl1.StartDate <= @AggregationRunDate -- Only pick up expired or currently active aggregations. Skip future aggregation cycles.
),
CTE_AllAggregationsForAccount_2 As
(
	select *
	from CTE_AllAggregationsForAccount
	where IsAggregationActiveFlag = 1 
	or
	(
		IsAggregationActiveFlag = 0
		and
		datediff(dd , StartDate , @AggregationRunDate) < 730 -- Default value since we are going to archive data in the system after 2 years
	)
)
select *
into #TempAggrgegationsToRecalculate
from CTE_AllAggregationsForAccount_2
-- This clause is essential to ensure that only the aggregation cycles which are later to
-- the one qualifying for recalculation are selected. No aggregation will run for the 
-- previous cycles
where RecordID >=
(
	Select RecordID
	from CTE_AllAggregationsForAccount_2
	where AggregationCycleID = @AggregationCycleID
)
and DirectionID = @DirectionID

-- DEBUG STATEMENT
--Select * from #TempAggrgegationsToRecalculate
--order by RecordID

-----------------------------------------------------------
-- Exit the procedure if there are no entries in the 
-- Temp table for aggregation recalculations
-----------------------------------------------------------
if ((Select count(*) from #TempAggrgegationsToRecalculate) = 0)
	GOTO ENDPROCESS

-------------------------------------------------------------
-- For all the qualifying aggregations remove the historical
-- data from the Aggregate Summary tables  
-------------------------------------------------------------
Begin Try

	-------------------------------
	-- TB_AGGREGATESUMMARY
	-------------------------------
	Delete tbl1
	from tb_AggregateSummary tbl1
	inner join #TempAggrgegationsToRecalculate tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID

	---------------------------------
	-- TB_AGGREGATESUMMARYRATEDETAILS
	---------------------------------
	Delete tbl1
	from tb_AggregateSummaryRateDetails tbl1
	inner join #TempAggrgegationsToRecalculate tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID

	-----------------------------------------
	-- TB_AGGREGATESUMMARYRATETIERALLOCATION
	-----------------------------------------
	Delete tbl1
	from tb_AggregateSummaryRateTierAllocation tbl1
	inner join #TempAggrgegationsToRecalculate tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID

	---------------------------------
	-- TB_AGGREGATESUMMARYTRAFFIC
	---------------------------------
	Delete tbl1
	from tb_AggregateSummaryTraffic tbl1
	inner join #TempAggrgegationsToRecalculate tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While Deleting historcal data from summary tables for Aggregations of Account : (' + 
	                        convert(varchar(10) ,@AccountID) + '). ' + ERROR_MESSAGE()

	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

----------------------------------------------------------------
-- Loop through each of the aggregations and recalculate based
-- on the aggregation type
----------------------------------------------------------------
Declare @VarAggregationCycleID int,
        @VarAggregationTypeID int

DECLARE db_RecalculateAggregation_Cur CURSOR FOR 
select AggregationCycleID , AggregationtypeID
from #TempAggrgegationsToRecalculate
order by RecordID

OPEN db_RecalculateAggregation_Cur
FETCH NEXT FROM db_RecalculateAggregation_Cur 
INTO @VarAggregationCycleID , @VarAggregationTypeID 

While @@FETCH_STATUS = 0   
Begin

	Begin Try

		set @ErrorDescription = NULL
		set @ResultFlag = 0

		-----------------------------------------------------------------
		-- Call the Appropriate procedure based on the Aggregation type
		-- of the qualifying cycle
		-----------------------------------------------------------------

		if (@VarAggregationTypeID in (-1,-2 , -6, -7))
			Exec SP_BSAggregateTrafficForCommitmentWithGraceTypeCycle @VarAggregationCycleID , @ErrorDescription Output , @ResultFlag Output

		if (@VarAggregationTypeID in (-3,-4 , -8 , -9))
			Exec SP_BSAggregateTrafficForCommitmentNoGraceTypeCycle @VarAggregationCycleID , @ErrorDescription Output , @ResultFlag Output

		if (@VarAggregationTypeID  = -5)
			Exec SP_BSAggregateTrafficForAggregateTypeCycle @VarAggregationCycleID , @ErrorDescription Output , @ResultFlag Output

		------------------------------------------------------------------
		-- Check the Result flag to establish if there was any exception
		------------------------------------------------------------------
		if (@ResultFlag = 1)
		Begin

				set @ResultFlag = 1

				CLOSE db_RecalculateAggregation_Cur  
				DEALLOCATE db_RecalculateAggregation_Cur

				GOTO ENDPROCESS

		End


	End Try

	Begin Catch

			set @ErrorDescription = 'ERROR !!! While calculating summary for aggregation cycle :(' + 
									convert(varchar(20) , @VarAggregationcycleID) + '). ' + ERROR_MESSAGE()
	          
			set @ResultFlag = 1

			CLOSE db_RecalculateAggregation_Cur  
			DEALLOCATE db_RecalculateAggregation_Cur

			GOTO ENDPROCESS

	End Catch

 	FETCH NEXT FROM db_RecalculateAggregation_Cur
    INTO @VarAggregationCycleID , @VarAggregationTypeID 

End 
 
CLOSE db_RecalculateAggregation_Cur  
DEALLOCATE db_RecalculateAggregation_Cur

ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggrgegationsToRecalculate') )
	Drop table #TempAggrgegationsToRecalculate

Return 0







GO
/****** Object:  StoredProcedure [dbo].[SP_BSValidateAggregateRateStructureData]    Script Date: 7/9/2021 10:36:54 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_BSValidateAggregateRateStructureData]
(
	@ErrorDescription varchar(2000) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0


if not exists (select 1 from #TempRateStructure)
Begin

	set @ErrorDescription = 'ERROR !!!! No rate structure details have been passd for validation or exception during upload of provided information'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

-----------------------------------------------------------
-- CHECK: Ensure that all the values in the [FROM] field
-- are non negative numbers and not NULL
-----------------------------------------------------------
if exists ( select 1 from #TempRateStructure where ISNUMERIC([FROM]) = 0 or [FROM] is NULL or [FROM] < 0 )
Begin

	set @ErrorDescription = 'ERROR !!!! The FROM column cannot be NULL and should have a positive numerical value'
	set @ResultFlag = 1

	GOTO ENDPROCESS


End

-----------------------------------------------------------
-- CHECK: Ensure that all the values in the [APPLY FROM] field
-- are NOT NULL and either 0 or 1
-----------------------------------------------------------
if exists ( select 1 from #TempRateStructure where isnull(ApplyFrom , 999) not in (0,1) )
Begin

	set @ErrorDescription = 'ERROR !!!! The APPLY FROM column can only take values 0 or 1'
	set @ResultFlag = 1

	GOTO ENDPROCESS


End

-----------------------------------------------------------
-- CHECK: Ensure that all the values in the [RATE] field
-- are NOT NULL and positive values
-----------------------------------------------------------
if exists ( select 1 from #TempRateStructure where ISNUMERIC(Rate) = 0 or Rate is NULL or Rate < 0 )
Begin

	set @ErrorDescription = 'ERROR !!!! The RATE column cannot be NULL and should have a positive numerical value'
	set @ResultFlag = 1

	GOTO ENDPROCESS


End

-----------------------------------------------------------
-- CHECK: Ensure that all the values in the [TO] field
-- are positive numerical values
-----------------------------------------------------------
if exists ( select 1 from #TempRateStructure where ISNUMERIC(isnull([To] , 999)) = 0 or isnull([To] , 999) < 0 )
Begin

	set @ErrorDescription = 'ERROR !!!! The TO column should have a positive numerical value for all the closed Rate Tiers'
	set @ResultFlag = 1

	GOTO ENDPROCESS


End

------------------------------------------------------------
-- Check to ensure [FROM] and [TO] columns have unique values
------------------------------------------------------------
-- [FROM] Column
if exists (
				Select 1 from 
				(
					Select [From] , count(*) as TotalRecords
					from #TempRateStructure
					group by [From]
					having count(2) > 1
				) tbl1
          )
Begin

	set @ErrorDescription = 'ERROR !!!! Values in the [FROM] column are not distinct. Two or more rate tiers have the same value'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

-- [FROM] Column
if exists (
				Select 1 from 
				(
					Select [To] , count(*) as TotalRecords
					from #TempRateStructure
					where [To] is not null
					group by [To]
					having count(2) > 1
				) tbl1
          )
Begin

	set @ErrorDescription = 'ERROR !!!! Values in the [To] column are not distinct. Two or more rate tiers have the same value'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

-----------------------------------------------------------
-- Check to ensure that the lowest rate tier always starts
-- from 0
-----------------------------------------------------------
if (( select min([From]) from #TempRateStructure) <> 0)
Begin

	set @ErrorDescription = 'ERROR !!!! The rate tier with the smallest [FROM] value shoud start from Zero.'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

-----------------------------------------------------------
-- CHECK: There should be no gaps in the tiers of the 
-- rate structure
-----------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempMissingRateTier') )
	Drop table #TempMissingRateTier;

With CTE_RateStructureWithTierID as
(
	select * , ROW_NUMBER() Over (order by [From]) as TierID
	from #TempRateStructure
)
Select tbl1.*
into #TempMissingRateTier
from CTE_RateStructureWithTierID tbl1
inner join CTE_RateStructureWithTierID tbl2 on tbl1.TierID = (tbl2.TierID + 1)
Where tbl1.[From] <> tbl2.[To]

if exists (Select 1 from #TempMissingRateTier)
Begin

	set @ErrorDescription = 'ERROR !!!! Missing rate tier in the configuration. Check the [FROM] and [TO] range of tiers.'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

-----------------------------------------------------------
-- CHECK: There should be only one and only one open tier
-----------------------------------------------------------
Declare @NumOpenTiers int = 0

Select @NumOpenTiers = count(*)
from #TempRateStructure
where [To] is NULL

if (@NumOpenTiers > 1 )
Begin

	set @ErrorDescription = 'ERROR !!!! There cannot be more than one open tier in the rate structure (To = NULL)'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

if (@NumOpenTiers = 0 )
Begin

	set @ErrorDescription = 'ERROR !!!! The last tier in the rate structure needs to be open (To = NULL)'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

----------------------------------------------------------
-- CHECK: Ensure that the open tier in the rate structure
-- is the last one with the highest [FROM] minutes
----------------------------------------------------------
if not exists (
				select 1 
				from #TempRateStructure
				where [From] = (select max([From]) from #TempRateStructure)
				and [To] is NULL
			  )
Begin

	set @ErrorDescription = 'ERROR !!!! The last tier in the rate structure is not an open tier'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempMissingRateTier') )
	Drop table #TempMissingRateTier
GO


-------UI scripts



Create    Procedure [dbo].[SP_UIAggregationCommitmentTypeList]
As

select ID , Name
from
(
	Select 0 as ID , 'None' as Name
	union
	Select 1 as ID , 'Volume' as Name
	union
	Select 2 as ID , 'Amount' as Name
) tbl1
order by ID
GO
/****** Object:  StoredProcedure [dbo].[SP_UIAggregationCriteriaList]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_UIAggregationCriteriaList]
As

Select AggregationCriteriaID as ID , AggregationCriteria as Name
from tb_AggregationCriteria
order by AggregationCriteriaID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIAggregationCriteriaListBasedOnAggregationTypeAttributes]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create    Procedure [dbo].[SP_UIAggregationCriteriaListBasedOnAggregationTypeAttributes]
(
	@CommitmentType int,
	@GracePeriod int,
	@Penalty int
)
as

Select distinct tbl3.AggregationCriteriaID as ID , tbl3.AggregationCriteria as Name
from Tb_AggregationTypeAndCriteriaMapping tbl1
inner join tb_AggregationType tbl2 on tbl1.AggregationTypeID = tbl2.AggregationTypeID
inner join Tb_AggregationCriteria tbl3 on tbl1.AggregationCriteriaID = tbl3.AggregationCriteriaID
Where tbl2.CommitmentType = @CommitmentType
and tbl2.GracePeriod = @GracePeriod
and tbl2.Penalty = @Penalty

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIAggregationCycleSearch]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create     Procedure [dbo].[SP_UIAggregationCycleSearch]
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
--  @AggregationName varchar(100) = 'Indo%'
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
-- Get all the relevant Aggregation cycles along with
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

		set @SQLStr1 = 'Select tbl1.AccountID , tbl1.Account, tbl1.AggregationID , tbl1.AggregationName ,' + char(10) +
		               'tbl1.DirectionID , tbl1.Direction, tbl1.AggregationCycleID , tbl1.AggregationCycle' + char(10)

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
					   When (@AggregationName = '_') then ' and tbl1.AggregationName like '  + '''' + '%' + '[_]' + '%' + '''' + char(10)
					   When ( ( Len(@AggregationName) =  1 ) and ( @AggregationName = '%') ) then ''
					   When ( right(@AggregationName ,1) = '%' ) then ' and tbl1.AggregationName like ' + '''' + substring(@AggregationName,1 , len(@AggregationName) - 1) + '%' + '''' + char(10)
					   Else ' and tbl1.AggregationName like ' + '''' + @AggregationName + '%' + '''' + char(10)
				End
		
		set @SQLStr5 = 'Order by  tbl1.Account , tbl1.Direction , tbl1.AggregationName , tbl1.CycleStartDate'

		set @SQLStr = @SQLStr1 + @SQLStr2 + @SQLStr3 + @SQLStr4 + @SQLStr5

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


GO
/****** Object:  StoredProcedure [dbo].[SP_UIAggregationGracePeriodList]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_UIAggregationGracePeriodList]
As

select ID , Name
from
(
	Select 0 as ID , 'No' as Name
	union
	Select 1 as ID , 'Yes' as Name
) tbl1
order by ID
GO
/****** Object:  StoredProcedure [dbo].[SP_UIAggregationGroupingDelete]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_UIAggregationGroupingDelete]
(
	@AggregationGroupingIDList varchar(max),
	@UserID int,
	@ErrorDescription varchar(2000) output,
	@ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

----------------------------------------------------
-- Parse the Aggregation Grouping ID List and store 
-- in a Temp table
----------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGroupingIDTable') )
		Drop table #TempAggregationGroupingIDTable

Create table #TempAggregationGroupingIDTable (AggregationGroupingID varchar(100) )


insert into #TempAggregationGroupingIDTable
select * from FN_ParseValueList ( @AggregationGroupingIDList )

----------------------------------------------------------------
-- Check to ensure that none of the values are non numeric
----------------------------------------------------------------

if exists ( select 1 from #TempAggregationGroupingIDTable where ISNUMERIC(AggregationGroupingID) = 0 )
Begin

	set @ErrorDescription = 'ERROR !!! List of Aggregation Grouping IDs passed contain a non numeric value'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End
		
---------------------------------------------------------------------------------
-- Check to ensure that all the Aggregation Grouping IDs passed are valid values
---------------------------------------------------------------------------------
		
if exists ( 
				select 1 
				from #TempAggregationGroupingIDTable 
				where AggregationGroupingID not in
				(
					Select AggregationGroupingID
					from Tb_AggregationGrouping
				)
			)
Begin

	set @ErrorDescription = 'ERROR !!! List of Aggregation Grouping IDs passed contain value(s) which are not valid or do not exist'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

----------------------------------------------------------
-- Check to ensure that all the aggregation groupings
-- belong to the same aggregation
----------------------------------------------------------
Declare @TotalDistinctAggregations int = 0

Select @TotalDistinctAggregations = count(distinct tbl2.AggregationID)
from #TempAggregationGroupingIDTable tbl1
inner join Tb_AggregationGrouping tbl2 on tbl1.AggregationGroupingID = tbl2.AggregationGroupingID

if (@TotalDistinctAggregations >  1 )
Begin

	set @ErrorDescription = 'ERROR !!! The aggregation groupings provided to delete belong to multiple aggregations.'
	set @ResultFlag = 1
	GOTO ENDPROCESS


End

-----------------------------------------------
-- Delete all the passed Aggregation Groupings
-- from the system
-----------------------------------------------
Begin Try

	Delete from tbl1
	from Tb_AggregationGrouping tbl1
	inner join #TempAggregationGroupingIDTable tbl2 on tbl1.AggregationGroupingID = tbl2.AggregationGroupingID

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! During deletion of Aggregation Groupings. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	GOTO ENDPROCESS

End Catch


ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGroupingIDTable') )
		Drop table #TempAggregationGroupingIDTable

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIAggregationGroupingUpdate]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create     Procedure [dbo].[SP_UIAggregationGroupingUpdate]
(
    @AggregationGroupingID int,
	@BeginDate date,
	@EndDate date,
	@UserID int,
	@ErrorDescription varchar(2000) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @DirectionID int,
		@AccountID int,
		@TableName varchar(200),
		@SQLStr varchar(max)


--------------------------------------------------------------
-- Check to see if the aggregation Grouping ID is valid or not
--------------------------------------------------------------
if not exists (select 1 from Tb_AggregationGrouping where AggregationGroupingID = @AggregationGroupingID)
Begin

	set @ErrorDescription = 'ERROR !!! Aggregation Grouping ID is either not valid or it does not exist in the system.'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

---------------------------------------------------------
-- If End Date is not NULL, then ensure that Start Date is
-- less than End Date
---------------------------------------------------------
if ((@EndDate is not NULL) and (@BeginDate > @EndDate))
Begin

		set @ErrorDescription = 'ERROR !!! The Begin Date of Aggregation group cannot be later than the End Date'
		set @ResultFlag = 1
		GOTO ENDPROCESS

End

----------------------------------------------------------
-- Get all the essential details for the aggregation
-- whihc will be used later for date overlap verification
----------------------------------------------------------
Declare @VarCommercialTrunkID int,
		@VarServiceLevelID int,
		@VarCalltypeID int,
		@VarDestinationID int,
		@VarStartDate date,
		@VarEndDate date,
		@ResultFlag2 int = 0

Select @VarCommercialTrunkID = CommercialTrunkID,
	   @VarServiceLevelID = ServiceLevelID,
	   @VarCalltypeID = CallTypeID,
	   @VarDestinationID = DestinationID,
	   @AccountID = AccountID,
	   @DirectionID = DirectionID
from Tb_AggregationGrouping tbl1
inner join Tb_Aggregation tbl2 on tbl1.AggregationID = tbl2.AggregationID
inner join Tb_Agreement tbl3 on tbl2.AgreementID = tbl3.AgreementID
where AggregationGroupingID = @AggregationGroupingID

-----------------------------------------------------
-- Get a list of existing Aggregation Grouping 
-- combinations for the account to see if there is any
-- overlapping of the new aggregation groupings with
-- the existing ones
-----------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempExistingAggregationGroupingCombinations') )
		Drop table #TempExistingAggregationGroupingCombinations

Select tbl1.CommercialTrunkID , tbl1.ServiceLevelID, 
       tbl1.CallTypeID , tbl1.DestinationID,
	   tbl1.StartDate , tbl1.EndDate
into #TempExistingAggregationGroupingCombinations
from Tb_AggregationGrouping tbl1
inner join Tb_Aggregation tbl2 on tbl1.AggregationID = tbl2.AggregationID
inner join tb_Agreement tbl3 on tbl2.AgreementID = tbl3.AgreementID
where tbl2.DirectionID = @DirectionID
and tbl3.AccountID = @AccountID
and tbl1.CommercialTrunkID = @VarCommercialTrunkID
and tbl1.ServiceLevelID = @VarServiceLevelID
and tbl1.CallTypeID = @VarCalltypeID
and tbl1.DestinationID = @VarServiceLevelID
and tbl1.AggregationGroupingID <> @AggregationGroupingID -- All Aggregation Grouping except the one we are updating

-- DEBUG STATEMENT
--Select *
--from #TempExistingAggregationGroupingCombinations

--------------------------------------------------
-- If there are no existing aggregation groupings
-- then we skip the process to check for date 
-- overlap
--------------------------------------------------
if ((Select count(*) from #TempExistingAggregationGroupingCombinations) = 0)
Begin

	GOTO UPDATEAGGREGATIONGROUPING

End

--------------------------------------------------------------------
-- Perform the date overlap check for all the aggregation groupings
-- that have an existing record in other aggregations for the account
--------------------------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDateOverlapCheck') )
		Drop table #TempDateOverlapCheck

Create table #TempDateOverlapCheck (BeginDate date , EndDate date)

-------------------------------------------------------------
-- initialize and re-populate the temp schema holding dates
-- for existing aggregation groupings
-------------------------------------------------------------
insert into #TempDateOverlapCheck
select distinct StartDate , EndDate
from #TempExistingAggregationGroupingCombinations

------------------------------------------------
-- Call the procedure to check for date overlap
------------------------------------------------
set @ResultFlag2 = 0 -- Initialize the flag

Exec SP_BSCheckDateOverlap @VarStartDate , @VarEndDate , @ResultFlag2 Output
	
if (@ResultFlag2 = 1)
Begin

		set @ErrorDescription = 'ERROR !!! Aggregation Grouping is active under another aggregation for the new date period' 
		set @ResultFlag = 1
		GOTO ENDPROCESS

End

UPDATEAGGREGATIONGROUPING:

Begin Try

	update Tb_AggregationGrouping
	set StartDate = @BeginDate,
	    EndDate = @EndDate,
		ModifiedDate = getdate(),
		ModifiedByID = @UserID
	where AggregationGroupingID = @AggregationGroupingID


End Try

Begin Catch

	set @ErrorDescription = 'ERROR !! While updating the dates for Aggregation Grouping. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	GOTO ENDPROCESS

End Catch


ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDateOverlapCheck') )
		Drop table #TempDateOverlapCheck

return 0




GO
/****** Object:  StoredProcedure [dbo].[SP_UIAggregationPenaltyList]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_UIAggregationPenaltyList]
As

select ID , Name
from
(
	Select 0 as ID , 'No' as Name
	union
	Select 1 as ID , 'Yes' as Name
) tbl1
order by ID
GO
/****** Object:  StoredProcedure [dbo].[SP_UICreateAggregation]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create     procedure [dbo].[SP_UICreateAggregation]
(
	@AggregationName varchar(100),
	@AggregationAbbrv varchar(50),
	@AgreementID int,
	@DirectionID int,
	@Notes varchar(2000) = NULL,
	@UserID int,
	@AggregationID int Output,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @AggregationID = 0
set @ErrorDescription = NULL
set @ResultFlag = 0

--------------------------------------------------------
-- Check to ensure that a valid Agreement ID has been
-- passed to create the new aggregation
--------------------------------------------------------
if not exists (Select 1 from tb_Agreement where AgreementID = @AgreementID and Flag & 1 <> 1)
Begin

	set @ErrorDescription = 'ERROR !!! The agreement passed for creation of aggregation is not valid or does not exist in the system.'
	set @ResultFlag = 1
	Return 1

End

--------------------------------------------------------
-- Check to ensure that a valid value is passed for the
-- direction of the aggregation (1 or 2)
--------------------------------------------------------
if (@DirectionID not in (1,2))
Begin

	set @ErrorDescription = 'ERROR !!! The direction passed for aggregation creation is not valid'
	set @ResultFlag = 1
	Return 1

End

-------------------------------------------------------
-- Check to see that no other aggregation exists by the
-- same name for the agreement
-------------------------------------------------------
if exists (Select 1 from tb_aggregation where AgreementID = @AgreementID and AggregationName = @AggregationName)
Begin

	set @ErrorDescription = 'ERROR !!! There already exists an aggregation in the system by the same name for Agreement'
	set @ResultFlag = 1
	Return 1

End


-----------------------------------------------------
-- Insert data into the tb_Aggregation table for the
-- new aggregation
-----------------------------------------------------
Begin Try

	insert into Tb_Aggregation
	(
		AggregationName,
		AggregationAbbrv,
		AgreementID,
		DirectionID,
		Notes,
		ModifiedDate,
		ModifiedByID
	)
	values
	(
		@AggregationName,
		@AggregationAbbrv,
		@AgreementID,
		@DirectionID,
		@Notes,
		getdate(),
		@UserID
	)
	set @AggregationID = @@IDENTITY 
End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While creating the new Aggregation in system. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	Return 1

End Catch

return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UICreateAggregationCycle]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_UICreateAggregationCycle]
(
	@AggregationID int,
	@AggregationCycle varchar(100),
	@CommitmentType int,
	@GracePeriod int,
	@Penalty int,
	@AggregationCriteriaID int,
	@StartDate date,
	@EndDate date,
	@GracePeriodEndDate date = NULL,
	@Commitment Decimal(19,4) = NULL,
	@UserID int,
	@AggregationCycleID int output,
	@ErrorDescription varchar(2000) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag  = 0

Declare @AggregationTypeID int

-------------------------------------------------------------
-- Ensure that no other record exists for combination of 
-- aggregation cycle and AgrgegationID
-------------------------------------------------------------
if exists (Select 1 from Tb_AggregationCycle where AggregationID = @AggregationID and AggregationCycle = @AggregationCycle)
Begin

	set @ErrorDescription = 'ERROR !!! Record already exists in system for Aggregation and Aggregation Cycle Name. Please select a unique Aggregation cycle name.'
	set @ResultFlag = 1
	return 1

End

--------------------------------------------------------------
-- Call the validation procedure to check if all the info
-- provided for the new Aggregation cycle is correct or not
--------------------------------------------------------------

Begin Try

		set @ErrorDescription = NULL
		set @ResultFlag = 0

		Exec SP_UIValidateAggregationCycleInfo 	@AggregationID,
												@AggregationCycle,
												@CommitmentType,
												@GracePeriod,
												@Penalty,
												@AggregationCriteriaID,
												@StartDate,
												@EndDate,
												@GracePeriodEndDate,
												@Commitment,
												@ErrorDescription output,
												@ResultFlag output

		if (@ResultFlag = 1) --  Incase of exception exit the aggregation cycle creation process
				Return 1

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! When validating information for new Aggregation cycle. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	return 1

End Catch

-----------------------------------------------------------
-- Get the aggregation type based on the parameters:
-- 1.Commitment type
-- 2. Grace Period
-- 3. Penalty
-----------------------------------------------------------

Select  @AggregationTypeID = AggregationTypeID
from Tb_AggregationType 
where CommitmentType = @CommitmentType 
and GracePeriod = @GracePeriod 
and Penalty = @Penalty

--------------------------------------------------------
-- Insert record for the new aggregation cycle if all
-- the validations are successful
--------------------------------------------------------
Begin Try

		insert into tb_AggregationCycle
		(
			AggregationCycle,
			AggregationID,
			AggregationtypeID,
			AggregationCriteriaID,
			StartDate,
			EndDate,
			GracePeriodEndDate,
			Commitment,
			CycleCreationDate,
			ModifiedDate,
			ModifiedByID
		)
		values
		(
			@AggregationCycle,
			@AggregationID,
			@AggregationTypeID,
			@AggregationCriteriaID,
			@StartDate,
			@EndDate,
			@GracePeriodEndDate,
			@Commitment,
			getdate(),
			getdate(),
			@UserID
		)

		set @AggregationCycleID = @@IDENTITY -- Output the ID value for the new Aggregation cycle created in the system
End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! Inserting record in system for new Aggregation cycle. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	return 1

End Catch
GO
/****** Object:  StoredProcedure [dbo].[SP_UICreateAggregationCycleFromWizard]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_UICreateAggregationCycleFromWizard]
(
    -- All Parameters for Aggregation cycle
	@AggregationID int,
	@AggregationCycle varchar(100),
	@CommitmentType int,
	@GracePeriod int,
	@Penalty int,
	@AggregationCriteriaID int,
	@StartDate date,
	@EndDate date,
	@GracePeriodEndDate date = NULL,
	@Commitment Decimal(19,4) = NULL,
	-- All parameters for Rate Structure
	@CurrencyID int,
	@From varchar(max),
    @To varchar(max),
	@ApplyFrom varchar(max),
	@Rate varchar(max),
	@UserID int,
	@ErrorDescription varchar(max) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AggregationCycleID int

Begin Try

	Begin Transaction Create_AggregationCycle

	------------------------------------------------------
	-- Call the Procedure to create the aggregation cycle
	------------------------------------------------------
	Exec SP_UICreateAggregationCycle 	@AggregationID ,
										@AggregationCycle ,
										@CommitmentType,
										@GracePeriod,
										@Penalty,
										@AggregationCriteriaID,
										@StartDate,
										@EndDate,
										@GracePeriodEndDate,
										@Commitment,
										@UserID,
										@AggregationCycleID output,
										@ErrorDescription output,
										@ResultFlag output

	if (@ResultFlag = 1)
	Begin

			Rollback Transaction Create_AggregationCycle
			return 1

	End

	----------------------------------------------------------------
	-- If the aggregation cycle creation is successful, then call
	-- the procedure for creating the rate structure
	----------------------------------------------------------------
	set @ErrorDescription = NULL
	set @ResultFlag = 0

	Exec SP_UICreateAggregationCycleRate 	@AggregationCycleID ,
											@CurrencyID ,
											@From ,
											@To ,
											@ApplyFrom,
											@Rate ,
											@UserID ,
											@ErrorDescription output,
											@ResultFlag output

	if (@ResultFlag = 1)
	Begin

			Rollback Transaction Create_AggregationCycle
			return 1

	End


End Try


Begin Catch

	set @ErrorDescription = 'ERROR !!! When creating aggregation cycle and rate structure from wizard. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	Rollback Transaction Create_AggregationCycle
	return 1

End Catch

-- In case no exception is there, then commit the newly inserted data
Commit Transaction Create_AggregationCycle

return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UICreateAggregationCycleRate]    Script Date: 7/10/2021 12:26:56 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_UICreateAggregationCycleRate]
(
	@AggregationCycleID int,
	@CurrencyID int,
	@From varchar(max),
    @To varchar(max),
	@ApplyFrom varchar(max),
	@Rate varchar(max),
	@UserID int,
	@ErrorDescription varchar(max) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AggregationRateID int

-------------------------------------------------------
-- Check to ensure that the aggregation cycle is valid
-- and exists in the system
-------------------------------------------------------
if not exists (select 1 from Tb_AggregationCycle where AggregationCycleID = @AggregationCycleID)
Begin

	set @ErrorDescription = 'ERROR !!! Aggregation CycleID is either not valid or it doesnot exist in the system.'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

------------------------------------------------------
-- Check to ensure that currency ID is valid and exists
-- in the system
------------------------------------------------------
if not exists (select 1 from tb_Currency where currencyID = @CurrencyID)
Begin

	set @ErrorDescription = 'ERROR !!! CurrencyID is either not valid or it doesnot exist in the system.'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

--------------------------------------------------------------
-- If we are creating an Aggregation rate detail for the cycle
-- then ensure that no previous Aggregation rate details
-- exist in the system for this cycle
--------------------------------------------------------------
if exists (select 1 from Tb_AggregationRate where AggregationCycleID = @AggregationCycleID)
Begin

	set @ErrorDescription = 'ERROR !!! Agrgegation Rate details already exist in the system for this cycle'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

--------------------------------------------------------
-- Create the Temp table to store the Rate Structure 
-- details and pass to the Validation procedure to
-- perform mandatory checks on the data
--------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureFrom') )
	Drop table #TempRateStructureFrom

Create Table #TempRateStructureFrom
(
    RecordID int identity(1,1),
	[From] int,
)

insert into #TempRateStructureFrom ([From])
Select RecordValue from FN_ParseValueListWithNULL(@From)

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureTo') )
	Drop table #TempRateStructureTo

Create Table #TempRateStructureTo
(
    RecordID int identity(1,1),
	[To] int,
)

insert into #TempRateStructureTo ([To])
Select RecordValue from FN_ParseValueListWithNULL(@To)


if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureApplyFrom') )
	Drop table #TempRateStructureApplyFrom

Create Table #TempRateStructureApplyFrom
(
    RecordID int identity(1,1),
	[ApplyFrom] int,
)

insert into #TempRateStructureApplyFrom ([ApplyFrom])
Select RecordValue from FN_ParseValueListWithNULL(@ApplyFrom)


if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureRate') )
	Drop table #TempRateStructureRate

Create Table #TempRateStructureRate
(
    RecordID int identity(1,1),
	[Rate] Decimal(19,6),
)
insert into #TempRateStructureRate ([Rate])
Select RecordValue from FN_ParseValueListWithNULL(@Rate)

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructure') )
	Drop table #TempRateStructure

select tbl1.[From] , tbl2.[To] , tbl3.ApplyFrom , tbl4.Rate
into #TempRateStructure
from #TempRateStructureFrom tbl1
left join #TempRateStructureTo tbl2 on tbl1.RecordID = tbl2.RecordID
left join #TempRateStructureApplyFrom tbl3 on tbl1.RecordID = tbl3.RecordID
left join #TempRateStructureRate tbl4 on tbl1.RecordID = tbl4.RecordID

-- DEBUG STATEMENT
select *
from #TempRateStructure

----------------------------------------------------------
-- CALL THE PROCEDURE TO VALIDATE THE RATE STRUCTURE DATA
----------------------------------------------------------

Begin Try

	Exec SP_BSValidateAggregateRateStructureData @ErrorDescription output , @ResultFlag output

	if (@ResultFlag = 1)
		GOTO ENDPROCESS
	
End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While validating the Aggregate Rate Structure date. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch


-------------------------------------------------------------------
-- Insert the data into tb_AggregateRate and tb_AggregateRateDetail
-- schema
-------------------------------------------------------------------

-----------------------
-- TB_AGGREGATERATE
-----------------------
Begin Try

		insert into Tb_AggregationRate
		(
			AggregationCycleID,
			CurrencyID,
			ModifiedDate,
			ModifiedByID			
		)
		values
		(
			@AggregationCycleID,
			@CurrencyID,
			getdate(),
			@UserID
		)

		set @AggregationRateID = @@IDENTITY

End Try

Begin Catch 

	set @ErrorDescription = 'ERROR !!! While inserting new record for Aggregate Rate. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS
	
End Catch


--------------------------
-- TB_AGGREGATERATEDETAIL
--------------------------
Begin Try

		insert into Tb_AggregationRateDetail
		(
			AggregationRateID,
			TierID,
			[From],
			[To],
			ApplyFrom,
			Rate,
			ModifiedDate,
			ModifiedByID			
		)
		Select @AggregationRateID,
			   ROW_NUMBER() Over (Order by [From]),
			   [From],
			   [To],
			   ApplyFrom,
			   Rate,
			   getdate(),
			   @UserID
		from #TempRateStructure
			   
End Try

Begin Catch 

	set @ErrorDescription = 'ERROR !!! While inserting new record(s) for Aggregate Rate Structure detail. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	-- Remove the Aggregate Rate record as the insertion for Rate Details failed
	Delete from Tb_AggregationRate 
	where AggregationRateID = @AggregationRateID

	GOTO ENDPROCESS
	
End Catch


ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureFrom') )
	Drop table #TempRateStructureFrom

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureTo') )
	Drop table #TempRateStructureTo

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureApplyFrom') )
	Drop table #TempRateStructureApplyFrom

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureRate') )
	Drop table #TempRateStructureRate

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructure') )
	Drop table #TempRateStructure

return 0
GO



Create   Procedure [dbo].[SP_UIGetAggregateSummaryForAggregationCycle]
(
	@AggregationCycleID int
)
As

Select Account, AggregationName , Direction, AggregationCycleID , AggregationCycle ,
	   CommitmentType, GracePeriod, Penalty, AggregationStatus, AggregationCriteria,
	   CycleStartDate, CycleEndDate, GracePeriodStartDate, GracePeriodEndDate, CommittedMinutes,
	   CommittedAmount, AggregationStartDate, AggregationEndDate, TrafficAggregatedIncycle,
	   TrafficAggregatedInGracePeriod, TotalTrafficAggregated, TotalAmountAggregated,TrafficAggregatedOverCommitment,
	   TrafficAggregatedInPercent, ShortfallMinutes, PenaltyAmount, AggregationBlendedRate, AggregationBlendedRateAfterPenalty
from vw_AggregateSummary
where AggregationCycleID = @AggregationCycleID

return 0

GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregateSummaryRateDetailsForAggregationCycle]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_UIGetAggregateSummaryRateDetailsForAggregationCycle]
(
	@AggregationCycleID int
)
As

select Min(CallDate) as StartDate,
       Max(CallDate) as EndDate,
	   Convert(Decimal(19,2) ,sum(Minutes)) as [Minutes],
	   Rate ,
	   convert(Decimal(19,2) ,sum(Amount)) as Amount
from tb_aggregateSummaryTraffic
where AggregationCycleID = @AggregationCycleID
group by Rate
order by 1
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregateSummaryRateTierAllocationForAggregationCycle]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create    Procedure [dbo].[SP_UIGetAggregateSummaryRateTierAllocationForAggregationCycle]
(
	@AggregationCycleID int
)
As

select StartDate , EndDate , TierID , 
       convert(Decimal(19,2) , AggregatedMinutes) as AggregatedMinutes,
	   convert(Decimal(19,2) , RatedAmount) as RatedAmount,
	   Rate
from tb_AggregateSummaryRateTierAllocation
where AggregationCycleID = @AggregationCycleID
order by StartDate, TierID
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregateSummaryTrafficForAggregationCycle]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create    Procedure [dbo].[SP_UIGetAggregateSummaryTrafficForAggregationCycle]
(
	@AggregationCycleID int
)
As

With CTE_AggregateSummaryTrafficByGrouping As
(
	select min(CallDate) as StartDate,
	       max(CallDate) as EndDate,
		   CommercialTrunkID,
		   ServiceLevelID,
		   CallTypeID,
		   DestinationID,
		   sum(minutes) as [Minutes],
		   sum(Amount) as Amount,
		   Rate
	from tb_AggregateSummaryTraffic
	where AggregationCycleID = @AggregationCycleID
	Group by CommercialTrunkID,
		     ServiceLevelID,
		     CallTypeID,
		     DestinationID,
			 Rate
)
select tbl1.StartDate , tbl1.EndDate,
	   tbl2.Trunk as CommercialTrunk,
	   tbl3.ServiceLevel as ServiceLevel,
	   tbl4.CallType as CallType,
	   tbl5.Destination as Destination,
	   convert(Decimal(19,2) ,tbl1.Minutes) as [Minutes],
	   tbl1.Rate,
	   convert(Decimal(19,2) ,tbl1.Amount) as Amount
from CTE_AggregateSummaryTrafficByGrouping tbl1
inner join tb_Trunk tbl2 on tbl1.CommercialTrunkID = tbl2.TrunkID
inner join tb_ServiceLevel tbl3 on tbl1.ServiceLevelID = tbl3.ServiceLevelID
inner join tb_CallType tbl4 on tbl1.CallTypeID = tbl4.CallTypeID
inner join tb_Destination tbl5 on tbl1.DestinationID = tbl5.DestinationID
order by tbl1.StartDate, tbl3.Servicelevel, tbl2.Trunk , tbl4.CallType,  tbl5.Destination

return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregationCycleDetails]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_UIGetAggregationCycleDetails]
(
	@AggregationCycleID int
)
As

Select tbl1.AggregationCycleID,
       tbl1.AggregationCycle,
	   tbl2.CommitmentType,
	   Case
			When tbl2.CommitmentType = 0 Then 'None'
			When tbl2.CommitmentType = 1 Then 'Volume'
			When tbl2.CommitmentType = 2 Then 'Amount'
	   End as CommitmentTypeValue,
	   tbl2.GracePeriod,
	   Case
			When tbl2.GracePeriod = 0 Then 'No'
			When tbl2.GracePeriod = 1 Then 'Yes'
	   End as GracePeriodValue,
	   tbl2.Penalty,
	   Case
			When tbl2.Penalty = 0 Then 'No'
			When tbl2.Penalty = 1 Then 'Yes'
	   End as PenaltyValue,
	   tbl1.AggregationCriteriaID,
	   tbl3.AggregationCriteria , 
	   tbl1.StartDate,
	   tbl1.EndDate,
	   Case
			When tbl2.GracePeriod = 0 Then NULL
			When tbl2.GracePeriod = 1 Then tbl1.GracePeriodEndDate
	   End as GracePeriodEndDate,
	   Case
			When tbl2.CommitmentType = 0 Then NULL
			Else tbl1.Commitment
	   End as Commitment,
	   tbl1.CycleCreationDate,
	   tbl1.ModifiedDate,
	   tbl4.Name as ModifiedBy
from Tb_AggregationCycle tbl1
inner join Tb_AggregationType tbl2 on tbl1.AggregationtypeID = tbl2.AggregationTypeID
inner join Tb_AggregationCriteria tbl3 on tbl1.AggregationCriteriaID = tbl3.AggregationCriteriaID
inner join UC_Admin.dbo.tb_Users tbl4 on tbl1.ModifiedByID = tbl4.UserID
where tbl1.AggregationCycleID = @AggregationCycleID


return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregationCycleParentDetailsByCycleId]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE  Procedure [dbo].[SP_UIGetAggregationCycleParentDetailsByCycleId]
(
	@AggregationCycleID int
)
As
select tb1.AggregationCycleID,tb1.AggregationCycle,tb2.AggregationName,tb4.Account 
from Tb_AggregationCycle tb1
inner join Tb_Aggregation tb2 on tb1.AggregationID = tb2.AggregationID
inner join tb_Agreement tb3 on tb3.AgreementID = tb2.AgreementID
inner join tb_Account tb4 on tb3.AccountID = tb4.AccountID
 where AggregationCycleID = @AggregationCycleID

return 0

 
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregationCyclesForAggregation]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   Procedure [dbo].[SP_UIGetAggregationCyclesForAggregation]
(
	@AggregationID int
)
As

Select tbl1.AggregationCycleID,
       tbl1.AggregationCycle,
	   Case
			When tbl2.CommitmentType = 0 Then 'None'
			When tbl2.CommitmentType = 1 Then 'Volume'
			When tbl2.CommitmentType = 2 Then 'Amount'
	   End as CommitmentType,
	   Case
			When tbl2.GracePeriod = 0 Then 'No'
			When tbl2.GracePeriod = 1 Then 'Yes'
	   End as GracePeriod,
	   Case
			When tbl2.Penalty = 0 Then 'No'
			When tbl2.Penalty = 1 Then 'Yes'
	   End as Penalty,
	   tbl3.AggregationCriteria , 
	   tbl1.StartDate,
	   tbl1.EndDate,
	   Case
			When tbl2.GracePeriod = 0 Then NULL
			When tbl2.GracePeriod = 1 Then tbl1.GracePeriodEndDate
	   End as GracePeriodEndDate,
	   Case
			When tbl2.CommitmentType = 0 Then NULL
			Else tbl1.Commitment
	   End as Commitment,
	   Case
			When tbl2.GracePeriod = 0 then
				Case
						When tbl1.EndDate >= convert(date , getdate()) then 'Active'
						Else 'Expired'
				End
			When tbl2.GracePeriod = 1 then 
				Case
						When tbl1.GracePeriodEndDate >= convert(date , getdate()) then 'Active'
						Else 'Expired'
				End
	   End as Status,
	   tbl1.CycleCreationDate,
	   tbl1.ModifiedDate,
	   tbl4.Name as ModifiedBy
from Tb_AggregationCycle tbl1
inner join Tb_AggregationType tbl2 on tbl1.AggregationtypeID = tbl2.AggregationTypeID
inner join Tb_AggregationCriteria tbl3 on tbl1.AggregationCriteriaID = tbl3.AggregationCriteriaID
inner join UC_Admin.dbo.tb_Users tbl4 on tbl1.ModifiedByID = tbl4.UserID
where AggregationID = @AggregationID
order by tbl1.StartDate

return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregationDetails]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_UIGetAggregationDetails]
(
	@AggregationID int
)
As

Select aggr.AggregationID,
       aggr.AggregationName,
	   aggr.AggregationAbbrv,
	   agr.Agreement,
	   dir.Direction,
	   aggr.Notes,
	   usr.Name as ModifiedBy,
	   aggr.ModifiedDate
from tb_aggregation aggr
inner join tb_agreement agr on aggr.AgreementID = agr.AgreementID
inner join tb_Direction dir on aggr.DirectionID =  dir.DirectionID
inner join UC_Admin.dbo.tb_Users usr on aggr.ModifiedByID = usr.UserID
where aggr.AggregationID = @AggregationID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregationGroupingDetails]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   Procedure [dbo].[SP_UIGetAggregationGroupingDetails]  
(  
 @AggregationGroupingID int  
)  
As  
  
select tbl1.AggregationGroupingID, tbl1.StartDate, tbl1.EndDate, tbl2.Trunk As CommercialTrunk, tbl3.ServiceLevel, tbl4.Destination, tbl5.CallType 
from tb_aggregationGrouping tbl1
Inner Join tb_Trunk tbl2 on tbl1.CommercialTrunkID = tbl2.TrunkID
Inner Join tb_ServiceLevel tbl3 on tbl1.ServiceLevelID = tbl3.ServiceLevelID
inner join tb_Destination tbl4 on tbl1.DestinationID = tbl4.DestinationID
inner Join tb_CallType tbl5 on tbl1.CallTypeID = tbl5.CallTypeID
where aggregationGroupingID = @AggregationGroupingID
  
Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregationGroupingsForAggregationSearch]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_UIGetAggregationGroupingsForAggregationSearch]
(
	@AggregationID int,
	@CommercialTrunkIDList varchar(max),
	@ServiceLevelIDList varchar(max),
	@CallTypeIDList varchar(max),
	@DestinationIDList varchar(max),
	@SelectDate date = null,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AllCallTypeFlag int = 0,
        @AllServiceLevelFlag int = 0,
		@AllDestinationFlag int = 0,
		@AllCommercialTrunkFlag int = 0,
		@SQLStr1 nvarchar(max),
		@SQLStr2 nvarchar(max),
		@SQLStr3 nvarchar(max),
		@SQLStr  nvarchar(max),
		@SQLStr4 nvarchar(max)

----------------------------------------------------------
-- Check to confirm if the aggregation ID is valid or not
----------------------------------------------------------
if not exists (Select 1 from Tb_Aggregation where AggregationID = @AggregationID)
Begin

	set @ErrorDescription = 'ERROR !!! The AggregationID passed is either invalid or does nor exist in the system.'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

---------------------------------------------------------
-- Parse the Lists for Commercial Trunk , Call type,
-- Service Level and Destinations and store in Temp
-- table
---------------------------------------------------------
Begin Try

	---------------------------
	-- SERVICE LEVEL ID LIST
	---------------------------

		-----------------------------------------------------------------
		-- Create table for list of all selected Service Levels from the 
		-- parameter passed
		-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempServiceLevelIDTable') )
				Drop table #TempServiceLevelIDTable

		Create table #TempServiceLevelIDTable (ServiceLevelID varchar(100) )


		insert into #TempServiceLevelIDTable
		select * from FN_ParseValueList ( @ServiceLevelIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempServiceLevelIDTable where ISNUMERIC(ServiceLevelID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Service Level IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the Service Levels have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempServiceLevelIDTable 
						where ServiceLevelID = 0
				  )
		Begin

				  set @AllServiceLevelFlag = 1
				  GOTO PROCESSDESTINATIONLIST
				  
		End
		
        --------------------------------------------------------------------------
		-- Check to ensure that all the Service Level IDs passed are valid values
		--------------------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempServiceLevelIDTable 
						where ServiceLevelID not in
						(
							Select distinct ServiceLevelID
							from Tb_AggregationGrouping
							where AggregationID = @AggregationID
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Service Level IDs passed contain value(s) which are not valid or do not exist for the Aggregation'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End 

PROCESSDESTINATIONLIST:
	-------------------------
	-- DESTINATION ID LIST
	-------------------------

		-----------------------------------------------------------------
		-- Create table for list of all selected Destinations from the 
		-- parameter passed
		-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationIDTable') )
				Drop table #TempDestinationIDTable

		Create table  #TempDestinationIDTable (DestinationID varchar(100) )


		insert into #TempDestinationIDTable
		select * from FN_ParseValueList ( @DestinationIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempDestinationIDTable where ISNUMERIC(DestinationID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the Detinations have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempDestinationIDTable 
						where DestinationID = 0
				  )
		Begin

				  set @AllDestinationFlag = 1
				  GOTO PROCESSCOMMERCIALTRUNKLIST
				  
		End
		
        --------------------------------------------------------------------------
		-- Check to ensure that all the Destinations passed are valid values
		--------------------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempDestinationIDTable 
						where DestinationID not in
						(
							Select distinct DestinationID
							from Tb_AggregationGrouping
							where AggregationID = @AggregationID
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination IDs passed contain value(s) which are not valid or do not exist for the aggregation'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

PROCESSCOMMERCIALTRUNKLIST:

	-------------------------------
	-- COMMERCIAL TRUNK ID LIST
	-------------------------------

		-----------------------------------------------------------------
		-- Create table for list of selected Commercial Trunks from the 
		-- parameter passed
		-----------------------------------------------------------------
		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCommercialTrunkIDTable') )
				Drop table #TempCommercialTrunkIDTable

		Create table #TempCommercialTrunkIDTable (CommercialTrunkID varchar(100) )


		insert into #TempCommercialTrunkIDTable
		select * from FN_ParseValueList ( @CommercialTrunkIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempCommercialTrunkIDTable where ISNUMERIC(CommercialTrunkID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of CommercialTrunk IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the CommercialTrunks have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempCommercialTrunkIDTable 
						where CommercialTrunkID = 0
				  )
		Begin

                  set @AllCommercialTrunkFlag = 1
				  GOTO PROCESSCALLTYPELIST
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the CommercialTrunk IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempCommercialTrunkIDTable 
						where CommercialTrunkID not in
						(
							Select distinct CommercialTrunkID
							from Tb_AggregationGrouping
							where AggregationID = @AggregationID
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of CommercialTrunk IDs passed contain value(s) which are not valid or do not exist for the aggregation'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

PROCESSCALLTYPELIST:
	-----------------------
	-- CALL TYPE ID LIST
	-----------------------

		---------------------------------------------------------
		-- Create table for list of selected Call Type from the 
		-- parameter passed
		---------------------------------------------------------
		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCallTypeIDTable') )
				Drop table #TempCallTypeIDTable

		Create table #TempCallTypeIDTable (CallTypeID varchar(100) )


		insert into #TempCallTypeIDTable
		select * from FN_ParseValueList ( @CallTypeIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempCallTypeIDTable where ISNUMERIC(CallTypeID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of CallType IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the Call types have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempCallTypeIDTable 
						where CallTypeID = 0
				  )
		Begin

                  set @AllCallTypeFlag = 1
				  GOTO GENERATERESULT
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the Call Type IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempCallTypeIDTable 
						where CallTypeID not in
						(
							Select distinct CallTypeID
							from Tb_AggregationGrouping
							where AggregationID = @AggregationID
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of CallType IDs passed contain value(s) which are not valid or do not exist for the Aggregation'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! Exception encountered when parsing attribute lists. '+ ERROR_MESSAGE()
		set @ResultFlag = 1
		GOTO ENDPROCESS

End Catch

GENERATERESULT:

---------------------------------------------------
-- Buid the dynamic Search to get the result set
---------------------------------------------------

Begin Try

		set @SQLStr1 = 'Select tbl1.AggregationGroupingID , tbl1.CommercialTrunkID , tbl2.Trunk as CommercialTrunk,' + char(10)+
					  'tbl1.ServiceLevelID , tbl3.ServiceLevel , tbl1.CallTypeID , tbl4.CallType ,' + char(10)+
					  'tbl1.DestinationID , tbl5.Destination , tbl1.StartDate , tbl1.EndDate' + char(10)

		set @SQLStr2 = 'From tb_AggregationGrouping tbl1' + char(10) +
					   'inner join tb_Trunk tbl2 on tbl1.CommercialTrunkID = tbl2.TrunkID' + char(10)+
					   'inner join tb_ServiceLevel tbl3 on tbl1.ServiceLevelID = tbl3.ServiceLevelID' + char(10)+
					   'inner join tb_Calltype tbl4 on tbl1.CallTypeID = tbl4.CallTypeID' + char(10)+
					   'inner join tb_Destination tbl5 on tbl1.DestinationID = tbl5.DestinationID' + char(10)+
					   Case
							When @AllCommercialTrunkFlag = 1 Then ''
							Else 'inner join #TempCommercialTrunkIDTable tbl6 on tbl1.CommercialTrunkID = tbl6.CommercialTrunkID' + char(10)
					   End + 
					   Case
							When @AllServiceLevelFlag = 1 Then ''
							Else 'inner join #TempServiceLevelIDTable tbl7 on tbl1.ServiceLevelID = tbl7.ServiceLevelID' + char(10)
					   End +
					   Case
							When @AllCallTypeFlag = 1 Then ''
							Else 'inner join #TempCallTypeIDTable tbl8 on tbl1.CallTypeID = tbl8.CallTypeID' + char(10)
					   End +
					   Case
							When @AllDestinationFlag = 1 Then ''
							Else 'inner join #TempDestinationIDTable tbl9 on tbl1.DestinationID = tbl9.DestinationID' + char(10)
					   End

		set @SQLStr3 = 'Where tbl1.AggregationID = ' + convert(varchar(10) , @AggregationID) +  char(10)+ 
						Case
							When @SelectDate is NULL Then ''
							Else 'and ''' + convert(varchar(10) , @SelectDate , 120) + ''' between tbl1.StartDate and isnull(tbl1.EndDate , ''' + 
								 convert(varchar(10) , @SelectDate , 120) + ''')' + char(10)					
						End



		set @SQLStr4 = 'Order by tbl2.trunk, tbl3.ServiceLevel, tbl4.CallType, tbl5.Destination'

		set @SQLStr = @SQLStr1 + @SQLStr2 +  @SQLStr3  + @SQLStr4

		-- DEBUG STATEMENT
		-- print @SQLStr

		Exec(@SQLStr)

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! While executing the search for Aggregation Group results. ' + ERROR_MESSAGE()
		set @ResultFlag = 1
		GOTO ENDPROCESS

End Catch

ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempServiceLevelIDTable') )
		Drop table #TempServiceLevelIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCommercialTrunkIDTable') )
		Drop table #TempCommercialTrunkIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationIDTable') )
		Drop table #TempDestinationIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCallTypeIDTable') )
		Drop table #TempCallTypeIDTable

return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregationRateDetailInfo]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_UIGetAggregationRateDetailInfo]
(
	@AggregationRateID int
)
As

Select tbl1.AggregationRateDetailID , tbl1.TierID,
       tbl1.[From] , tbl1.[To], 
	   cast(tbl1.ApplyFrom as int) as ApplyFromVal,
	   Case 
			When tbl1.ApplyFrom = 1 then 'Zeroth Minute'
			When tbl1.ApplyFrom = 0 then 'Next Minute'
	   End as ApplyFrom,
	   tbl1.Rate ,
	   tbl1.ModifiedDate,
	   tbl2.Name as ModifiedBy
from Tb_AggregationRateDetail tbl1
inner join UC_Admin.dbo.tb_Users tbl2 on tbl1.ModifiedByID = tbl2.UserID
where tbl1.AggregationRateID = @AggregationRateID
order by TierID
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAggregationRateInfo]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE    Procedure [dbo].[SP_UIGetAggregationRateInfo]
(
	@AggregationCycleID int
)
As

Select tbl1.AggregationRateID,
       tbl1.AggregationCycleID,
	   tbl2.AggregationCycle,
	   tbl1.CurrencyID,
	   tbl3.Currency,
	   tbl1.ModifiedDate,
	   tbl4.Name as ModifiedBy
from Tb_AggregationRate tbl1
inner join Tb_AggregationCycle tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID
inner join tb_Currency tbl3 on tbl1.CurrencyID = tbl3.CurrencyID
inner join UC_Admin.dbo.tb_Users tbl4 on tbl1.ModifiedByID = tbl4.UserID
where tbl1.AggregationCycleID = @AggregationCycleID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetAllAggregationsForAgreement]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create    Procedure [dbo].[SP_UIGetAllAggregationsForAgreement]
(
	@AgreementID int
)
As

Select tbl1.AggregationID ,tbl1.AggregationName , tbl1.DirectionID , tbl2.Direction
from tb_Aggregation tbl1
inner join tb_direction tbl2 on tbl1.DirectionID = tbl2.DirectionID
where AgreementID = @AgreementID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetCallTypeListForAggregation]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   procedure [dbo].[SP_UIGetCallTypeListForAggregation]
(
	@AggregationID int
)
As

Select Distinct tbl1.CallTypeID as ID , tbl2.CallType as Name
from tb_AggregationGrouping tbl1
inner join tb_CallType tbl2 on tbl1.CallTypeID = tbl2.CallTypeID
where tbl1.AggregationID = @AggregationID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetCommercialTrunkListForAggregation]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   procedure [dbo].[SP_UIGetCommercialTrunkListForAggregation]
(
	@AggregationID int
)
As

Select Distinct tbl1.CommercialTrunkID as ID , tbl2.Trunk as Name
from tb_AggregationGrouping tbl1
inner join tb_Trunk tbl2 on tbl1.CommercialTrunkID = tbl2.TrunkID
where tbl1.AggregationID = @AggregationID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetDestinationListForAggregation]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create     procedure [dbo].[SP_UIGetDestinationListForAggregation]
(
	@AggregationID int
)
As

Select Distinct tbl1.DestinationID as ID , tbl2.Destination as Name
from tb_AggregationGrouping tbl1
inner join tb_Destination tbl2 on tbl1.DestinationID = tbl2.DestinationID
where tbl1.AggregationID = @AggregationID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetServiceLevelListForAggregation]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   procedure [dbo].[SP_UIGetServiceLevelListForAggregation]
(
	@AggregationID int
)
As

Select Distinct tbl1.ServiceLevelID as ID , tbl2.ServiceLevel as Name
from tb_AggregationGrouping tbl1
inner join tb_ServiceLevel tbl2 on tbl1.ServiceLevelID = tbl2.ServiceLevelID
where tbl1.AggregationID = @AggregationID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetTrafficByCallDateForAggregationCycleSearch]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   Procedure [dbo].[SP_UIGetTrafficByCallDateForAggregationCycleSearch]
(
	@AggregationCycleID int,
	@StartDate date,
	@EndDate date,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

----------------------------------------------------------
-- Check to confirm if the aggregation cycle ID is valid or not
----------------------------------------------------------
if not exists (Select 1 from Tb_AggregationCycle where AggregationCycleID = @AggregationCycleID)
Begin

	set @ErrorDescription = 'ERROR !!! The Aggregation Cycle ID passed is either invalid or does nor exist in the system.'
	set @ResultFlag = 1
	Return 1

End

--------------------------------------------------------------------
-- Check to ensure that the start Date is not greater than end Date
--------------------------------------------------------------------
if (@StartDate > @EndDate)
Begin

	set @ErrorDescription = 'ERROR !!! The Start Date cannot be greater than the End Date'
	set @ResultFlag = 1
	Return 1

End

------------------------------------------
-- Get the Result set based on the dates
------------------------------------------
Select CallDate, TierID,
       convert(Decimal(19,2) , AggregatedMinutes) as AggregatedMinutes,
	   convert(Decimal(19,2) , Ratedamount) as RatedAmount,
	   Rate
from tb_AggregateSummaryRateDetails
where CallDate between @StartDate and @EndDate 
and AggregationCycleID = @AggregationCycleID

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIGetTrafficByGroupingForAggregationCycleSearch]    Script Date: 7/10/2021 12:32:41 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE      Procedure [dbo].[SP_UIGetTrafficByGroupingForAggregationCycleSearch]
(
	@AggregationCycleID int,
	@CommercialTrunkIDList varchar(max),
	@ServiceLevelIDList varchar(max),
	@CallTypeIDList varchar(max),
	@DestinationIDList varchar(max),
	@StartDate date,
	@EndDate date,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AllCallTypeFlag int = 0,
        @AllServiceLevelFlag int = 0,
		@AllDestinationFlag int = 0,
		@AllCommercialTrunkFlag int = 0,
		@SQLStr1 nvarchar(max),
		@SQLStr2 nvarchar(max),
		@SQLStr3 nvarchar(max),
		@SQLStr  nvarchar(max),
		@SQLStr4 nvarchar(max),
		@AggregationID int

----------------------------------------------------------
-- Check to confirm if the aggregation cycle ID is valid or not
----------------------------------------------------------
if not exists (Select 1 from Tb_AggregationCycle where AggregationCycleID = @AggregationCycleID)
Begin

	set @ErrorDescription = 'ERROR !!! The Aggregation Cycle ID passed is either invalid or does nor exist in the system.'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

--------------------------------------------------------------------
-- Check to ensure that the start Date is not greater than end Date
--------------------------------------------------------------------
if (@StartDate > @EndDate)
Begin

	set @ErrorDescription = 'ERROR !!! The Start Date cannot be greater than the End Date'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

--------------------------------------------------------------------
-- Get the Aggregation ID for the Aggregation Cycle so that is can
-- be used for getting the qualifying attributes from the Aggregation
-- grouping schema
--------------------------------------------------------------------
Select @AggregationID = AggregationID
from Tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

---------------------------------------------------------
-- Parse the Lists for Commercial Trunk , Call type,
-- Service Level and Destinations and store in Temp
-- table
---------------------------------------------------------
Begin Try

	---------------------------
	-- SERVICE LEVEL ID LIST
	---------------------------

		-----------------------------------------------------------------
		-- Create table for list of all selected Service Levels from the 
		-- parameter passed
		-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempServiceLevelIDTable') )
				Drop table #TempServiceLevelIDTable

		Create table #TempServiceLevelIDTable (ServiceLevelID varchar(100) )


		insert into #TempServiceLevelIDTable
		select * from FN_ParseValueList ( @ServiceLevelIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempServiceLevelIDTable where ISNUMERIC(ServiceLevelID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Service Level IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the Service Levels have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempServiceLevelIDTable 
						where ServiceLevelID = 0
				  )
		Begin

				  set @AllServiceLevelFlag = 1
				  GOTO PROCESSDESTINATIONLIST
				  
		End
		
        --------------------------------------------------------------------------
		-- Check to ensure that all the Service Level IDs passed are valid values
		--------------------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempServiceLevelIDTable 
						where ServiceLevelID not in
						(
							Select distinct ServiceLevelID
							from Tb_AggregationGrouping
							where AggregationID = @AggregationID
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Service Level IDs passed contain value(s) which are not valid or do not exist for the Aggregation'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End 

PROCESSDESTINATIONLIST:
	-------------------------
	-- DESTINATION ID LIST
	-------------------------

		-----------------------------------------------------------------
		-- Create table for list of all selected Destinations from the 
		-- parameter passed
		-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationIDTable') )
				Drop table #TempDestinationIDTable

		Create table  #TempDestinationIDTable (DestinationID varchar(100) )


		insert into #TempDestinationIDTable
		select * from FN_ParseValueList ( @DestinationIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempDestinationIDTable where ISNUMERIC(DestinationID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the Detinations have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempDestinationIDTable 
						where DestinationID = 0
				  )
		Begin

				  set @AllDestinationFlag = 1
				  GOTO PROCESSCOMMERCIALTRUNKLIST
				  
		End
		
        --------------------------------------------------------------------------
		-- Check to ensure that all the Destinations passed are valid values
		--------------------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempDestinationIDTable 
						where DestinationID not in
						(
							Select distinct DestinationID
							from Tb_AggregationGrouping
							where AggregationID = @AggregationID
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination IDs passed contain value(s) which are not valid or do not exist for the aggregation'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

PROCESSCOMMERCIALTRUNKLIST:

	-------------------------------
	-- COMMERCIAL TRUNK ID LIST
	-------------------------------

		-----------------------------------------------------------------
		-- Create table for list of selected Commercial Trunks from the 
		-- parameter passed
		-----------------------------------------------------------------
		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCommercialTrunkIDTable') )
				Drop table #TempCommercialTrunkIDTable

		Create table #TempCommercialTrunkIDTable (CommercialTrunkID varchar(100) )


		insert into #TempCommercialTrunkIDTable
		select * from FN_ParseValueList ( @CommercialTrunkIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempCommercialTrunkIDTable where ISNUMERIC(CommercialTrunkID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of CommercialTrunk IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the CommercialTrunks have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempCommercialTrunkIDTable 
						where CommercialTrunkID = 0
				  )
		Begin

                  set @AllCommercialTrunkFlag = 1
				  GOTO PROCESSCALLTYPELIST
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the CommercialTrunk IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempCommercialTrunkIDTable 
						where CommercialTrunkID not in
						(
							Select distinct CommercialTrunkID
							from Tb_AggregationGrouping
							where AggregationID = @AggregationID
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of CommercialTrunk IDs passed contain value(s) which are not valid or do not exist for the aggregation'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

PROCESSCALLTYPELIST:
	-----------------------
	-- CALL TYPE ID LIST
	-----------------------

		---------------------------------------------------------
		-- Create table for list of selected Call Type from the 
		-- parameter passed
		---------------------------------------------------------
		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCallTypeIDTable') )
				Drop table #TempCallTypeIDTable

		Create table #TempCallTypeIDTable (CallTypeID varchar(100) )


		insert into #TempCallTypeIDTable
		select * from FN_ParseValueList ( @CallTypeIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempCallTypeIDTable where ISNUMERIC(CallTypeID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of CallType IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the Call types have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempCallTypeIDTable 
						where CallTypeID = 0
				  )
		Begin

                  set @AllCallTypeFlag = 1
				  GOTO GENERATERESULT
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the Call Type IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempCallTypeIDTable 
						where CallTypeID not in
						(
							Select distinct CallTypeID
							from Tb_AggregationGrouping
							where AggregationID = @AggregationID
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of CallType IDs passed contain value(s) which are not valid or do not exist for the Aggregation'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! Exception encountered when parsing attribute lists. '+ ERROR_MESSAGE()
		set @ResultFlag = 1
		GOTO ENDPROCESS

End Catch

GENERATERESULT:

---------------------------------------------------
-- Buid the dynamic Search to get the result set
---------------------------------------------------
Begin Try

		set @SQLStr1 = 'Select tbl1.CallDate, tbl2.Trunk as CommercialTrunk, tbl3.ServiceLevel as ServiceLevel, ' + char(10)+
		               'tbl4.CallType as CallType, tbl5.Destination as Destination, convert(Decimal(19,2) ,tbl1.Minutes) as [Minutes], ' + char(10)+
					   'tbl1.Rate, convert(Decimal(19,2) ,tbl1.Amount) as Amount' + char(10)

		set @SQLStr2 = 'From tb_AggregateSummaryTraffic tbl1' + char(10) +
					   'inner join tb_Trunk tbl2 on tbl1.CommercialTrunkID = tbl2.TrunkID' + char(10)+
					   'inner join tb_ServiceLevel tbl3 on tbl1.ServiceLevelID = tbl3.ServiceLevelID' + char(10)+
					   'inner join tb_Calltype tbl4 on tbl1.CallTypeID = tbl4.CallTypeID' + char(10)+
					   'inner join tb_Destination tbl5 on tbl1.DestinationID = tbl5.DestinationID' + char(10)+
					   Case
							When @AllCommercialTrunkFlag = 1 Then ''
							Else 'inner join #TempCommercialTrunkIDTable tbl6 on tbl1.CommercialTrunkID = tbl6.CommercialTrunkID' + char(10)
					   End + 
					   Case
							When @AllServiceLevelFlag = 1 Then ''
							Else 'inner join #TempServiceLevelIDTable tbl7 on tbl1.ServiceLevelID = tbl7.ServiceLevelID' + char(10)
					   End +
					   Case
							When @AllCallTypeFlag = 1 Then ''
							Else 'inner join #TempCallTypeIDTable tbl8 on tbl1.CallTypeID = tbl8.CallTypeID' + char(10)
					   End +
					   Case
							When @AllDestinationFlag = 1 Then ''
							Else 'inner join #TempDestinationIDTable tbl9 on tbl1.DestinationID = tbl9.DestinationID' + char(10)
					   End

		set @SQLStr3 = 'Where tbl1.AggregationcycleID = ' + convert(varchar(10) , @AggregationCycleID) +  char(10)+
					   'and tbl1.CallDate between ''' + convert(varchar(100) , @StartDate) + ''' and ''' +
					   convert(varchar(100) , @EndDate) + '''' + char(10)

		set @SQLStr4 = 'order by tbl1.CallDate , tbl3.Servicelevel, tbl2.Trunk ,tbl4.CallType,  tbl5.Destination'

		set @SQLStr = @SQLStr1 + @SQLStr2 +  @SQLStr3  + @SQLStr4

		-- DEBUG STATEMENT
		print @SQLStr

		Exec(@SQLStr)

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! While executing the search for Aggregation Traffc results. ' + ERROR_MESSAGE()
		set @ResultFlag = 1
		GOTO ENDPROCESS

End Catch

ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempServiceLevelIDTable') )
		Drop table #TempServiceLevelIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCommercialTrunkIDTable') )
		Drop table #TempCommercialTrunkIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationIDTable') )
		Drop table #TempDestinationIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCallTypeIDTable') )
		Drop table #TempCallTypeIDTable

return 0
GO



CREATE   or alter Procedure [dbo].[SP_UIRecalculateForAggregationCycle]
(
    @AggregationCycleID int,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @ResultFlag = 0
set @ErrorDescription = NULL

Begin Try

	set @ErrorDescription = NULL
	set @ResultFlag = 0

	Exec SP_BSRecalculateForAggregationCycle @AggregationCycleID , @ErrorDescription Output , @ResultFlag Output

	------------------------------------------------------------------
	-- Check the Result flag to establish if there was any exception
	------------------------------------------------------------------
	if (@ResultFlag = 1)
	Begin
			Return 1

	End


End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! While re-calculating summary for aggregation cycle. ' + ERROR_MESSAGE()	          
		set @ResultFlag = 1
		Return 1

End Catch

Return 0







GO
/****** Object:  StoredProcedure [dbo].[SP_UIUpdateAggregation]    Script Date: 7/10/2021 12:36:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE    or alter  procedure [dbo].[SP_UIUpdateAggregation]
(
	@AggregationID int,
	@AggregationName varchar(100),
	@AggregationAbbrv varchar(50),
	@Notes varchar(2000),
	@UserID int,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AgreementID int

--------------------------------------------------------
-- Check to ensure that a valid Aggregation ID has been
-- passed for update
--------------------------------------------------------
if not exists (Select 1 from Tb_Aggregation where AggregationID = @AggregationID )
Begin

	set @ErrorDescription = 'ERROR !!! The Aggregation ID passed for update is not valid or does not exist in the system.'
	set @ResultFlag = 1
	Return 1

End

-------------------------------------------
-- Get the agreement ID for the aggregation
-------------------------------------------
select @AgreementID = AgreementID
from Tb_Aggregation
where AggregationID = @AggregationID

-------------------------------------------------------
-- Check to see that no other aggregation exists by the
-- same name for the agreement
-------------------------------------------------------
if exists (Select 1 from tb_aggregation where AgreementID = @AgreementID and AggregationName = @AggregationName and AggregationID != @AggregationID)
Begin

	set @ErrorDescription = 'ERROR !!! There already exists an aggregation in the system by the same name for Agreement'
	set @ResultFlag = 1
	Return 1

End


-----------------------------------------------------
-- update data into the tb_Aggregation table for the
-- existing aggregation
-----------------------------------------------------
Begin Try

	update tb_Aggregation
	set AggregationName = @AggregationName,
	    AggregationAbbrv = @AggregationAbbrv,
		Notes = @Notes,
		ModifiedDate = getdate(),
		ModifiedByID = @UserID
	where AggregationID = @AggregationID


End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While updating info for Aggregation in system. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	Return 1

End Catch

return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIUpdateAggregationCycle]    Script Date: 7/10/2021 12:36:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE    or alter  Procedure [dbo].[SP_UIUpdateAggregationCycle]
(
	@AggregationCycleID int,
	@AggregationCycle varchar(100),
	@CommitmentType int,
	@GracePeriod int,
	@Penalty int,
	@AggregationCriteriaID int,
	@StartDate date,
	@EndDate date,
	@GracePeriodEndDate date = NULL,
	@Commitment Decimal(19,4) = NULL,
	@UserID int,
	@ErrorDescription varchar(2000) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag  = 0

Declare @AggregationTypeID int,
        @OldAggregationCycle varchar(100),
        @AggregationID int

-------------------------------------------------------------
-- Check to see if the Aggregation cycle ID passed for update
-- is valid or not
-------------------------------------------------------------
if not exists (select 1 from tb_Aggregationcycle where AggregationCycleID = @AggregationCycleID)
Begin

	set @ErrorDescription = 'ERROR !!! The Aggregation cycle ID passed is either not valid or it does not exist in the system.'
	set @ResultFlag = 1
	return 1

End

--------------------------------------- 
-- Get the AggregationID for the cycle
--------------------------------------- 
select @AggregationID = AggregationID,
       @OldAggregationCycle = AggregationCycle
from Tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

-------------------------------------------------------------
-- Ensure that no other record exists for combination of 
-- aggregation cycle and AgrgegationID
-------------------------------------------------------------
if (@OldAggregationCycle <> @AggregationCycle)
Begin
		if exists (Select 1 from Tb_AggregationCycle where AggregationID = @AggregationID and AggregationCycle = @AggregationCycle)
		Begin

			set @ErrorDescription = 'ERROR !!! Record already exists in system for Aggregation and Aggregation Cycle Name. Please select a unique Aggregation cycle name.'
			set @ResultFlag = 1
			return 1

		End
End

--------------------------------------------------------------
-- Call the validation procedure to check if all the info
-- provided for the new Aggregation cycle is correct or not
--------------------------------------------------------------

Begin Try

		set @ErrorDescription = NULL
		set @ResultFlag = 0

		Exec SP_UIValidateAggregationCycleInfo 	@AggregationID,
												@AggregationCycle,
												@CommitmentType,
												@GracePeriod,
												@Penalty,
												@AggregationCriteriaID,
												@StartDate,
												@EndDate,
												@GracePeriodEndDate,
												@Commitment,
												@ErrorDescription output,
												@ResultFlag output

		if (@ResultFlag = 1) --  Incase of exception exit the aggregation cycle creation process
				Return 1

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! When validating information for new Aggregation cycle. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	return 1

End Catch

-----------------------------------------------------------
-- Get the aggregation type based on the parameters:
-- 1.Commitment type
-- 2. Grace Period
-- 3. Penalty
-----------------------------------------------------------

Select  @AggregationTypeID = AggregationTypeID
from Tb_AggregationType 
where CommitmentType = @CommitmentType 
and GracePeriod = @GracePeriod 
and Penalty = @Penalty

--------------------------------------------------------
-- Update record for the aggregation cycle if all
-- the validations are successful
--------------------------------------------------------
Begin Try

		update Tb_AggregationCycle
		set AggregationCycle = @AggregationCycle,
			AggregationtypeID = @AggregationTypeID,
			AggregationCriteriaID = @AggregationCriteriaID,
			StartDate = @StartDate,
			EndDate = @EndDate,
			GracePeriodEndDate = @GracePeriodEndDate,
			Commitment = @Commitment,
			ModifiedDate =  getdate(),
			ModifiedByID = @UserID
		where AggregationCycleID = @AggregationCycleID

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While updating information for the Aggregation cycle. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	return 1

End Catch
GO
/****** Object:  StoredProcedure [dbo].[SP_UIUpdateAggregationCycleRate]    Script Date: 7/10/2021 12:36:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   or alter Procedure [dbo].[SP_UIUpdateAggregationCycleRate]
(
	@AggregationRateID int,
	@CurrencyID int,
	@AggregationRateDetail varchar(max),
	@From varchar(max),
    @To varchar(max),
	@ApplyFrom varchar(max),
	@Rate varchar(max),
	@UserID int,
	@ErrorDescription varchar(max) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AggregationCycleID int,
        @OldCurrencyID int

-------------------------------------------------------
-- Check to ensure that the aggregation Rate is valid
-- and exists in the system
-------------------------------------------------------
if not exists (select 1 from Tb_AggregationRate where AggregationRateID = @AggregationRateID)
Begin

	set @ErrorDescription = 'ERROR !!! Aggregation Rate ID  being updated is either not valid or it doesnot exist in the system.'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

------------------------------------------------------
-- Check to ensure that currency ID is valid and exists
-- in the system
------------------------------------------------------
if not exists (select 1 from tb_Currency where currencyID = @CurrencyID)
Begin

	set @ErrorDescription = 'ERROR !!! CurrencyID is either not valid or it doesnot exist in the system.'
	set @ResultFlag = 1

	GOTO ENDPROCESS

End

---------------------------------------------------------
-- Get the old currency details from the Aggregate Rate
-- schema
---------------------------------------------------------
Select @OldCurrencyID = CurrencyID
from Tb_AggregationRate
where AggregationRateID = @AggregationRateID

--------------------------------------------------------
-- Create the Temp table to store the Rate Structure 
-- details and pass to the Validation procedure to
-- perform mandatory checks on the data
--------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureRateDetailsID') )
	Drop table #TempRateStructureRateDetailsID

Create Table #TempRateStructureRateDetailsID
(
    RecordID int identity(1,1),
	AggregationRateDetailID int,
)

insert into #TempRateStructureRateDetailsID (AggregationRateDetailID)
Select RecordValue from FN_ParseValueListWithNULL(@AggregationRateDetail)



if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureFrom') )
	Drop table #TempRateStructureFrom

Create Table #TempRateStructureFrom
(
    RecordID int identity(1,1),
	[From] int,
)

insert into #TempRateStructureFrom ([From])
Select RecordValue from FN_ParseValueListWithNULL(@From)

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureTo') )
	Drop table #TempRateStructureTo

Create Table #TempRateStructureTo
(
    RecordID int identity(1,1),
	[To] int,
)

insert into #TempRateStructureTo ([To])
Select RecordValue from FN_ParseValueListWithNULL(@To)


if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureApplyFrom') )
	Drop table #TempRateStructureApplyFrom

Create Table #TempRateStructureApplyFrom
(
    RecordID int identity(1,1),
	[ApplyFrom] int,
)

insert into #TempRateStructureApplyFrom ([ApplyFrom])
Select RecordValue from FN_ParseValueListWithNULL(@ApplyFrom)


if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureRate') )
	Drop table #TempRateStructureRate

Create Table #TempRateStructureRate
(
    RecordID int identity(1,1),
	[Rate] Decimal(19,6),
)
insert into #TempRateStructureRate ([Rate])
Select RecordValue from FN_ParseValueListWithNULL(@Rate)

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructure') )
	Drop table #TempRateStructure

select tbl5.AggregationRateDetailID , tbl1.[From] , tbl2.[To] , tbl3.ApplyFrom , tbl4.Rate
into #TempRateStructure
from #TempRateStructureFrom tbl1
left join #TempRateStructureTo tbl2 on tbl1.RecordID = tbl2.RecordID
left join #TempRateStructureApplyFrom tbl3 on tbl1.RecordID = tbl3.RecordID
left join #TempRateStructureRate tbl4 on tbl1.RecordID = tbl4.RecordID
left join #TempRateStructureRateDetailsID tbl5 on tbl1.RecordID = tbl5.RecordID

-- DEBUG STATEMENT
select *
from #TempRateStructure

----------------------------------------------------------
-- CALL THE PROCEDURE TO VALIDATE THE RATE STRUCTURE DATA
----------------------------------------------------------

Begin Try

	Exec SP_BSValidateAggregateRateStructureData @ErrorDescription output , @ResultFlag output

	if (@ResultFlag = 1)
		GOTO ENDPROCESS
	
End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While validating the Aggregate Rate Structure date. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	GOTO ENDPROCESS

End Catch

----------------------------------------------------------------
-- Get the existing Aggregate Rate Structure from the system
----------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempOldRateStructure') )
	Drop table #TempOldRateStructure

Select AggregationRateDetailID , [From] , [To] , ApplyFrom , Rate
into #TempOldRateStructure
from Tb_AggregationRateDetail
Where AggregationRateID = @AggregationRateID

-- DEBUG STATEMENT
select *
from #TempOldRateStructure

--------------------------------------------------
-- Add the TIER ID for the new rate structure as
-- its passed the validations and is the final
-- structure to be updated in the system
--------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureFinal') )
	Drop table #TempRateStructureFinal

Select * , 
       ROW_NUMBER() Over (Order by [From]) as TierID
into #TempRateStructureFinal
from #TempRateStructure

-- DEBUG STATEMENT
select *
from #TempRateStructureFinal
Order by TierID

-------------------------------------------------------------------------
-- We need to do the following:
-- 1. If Aggregation Detail is NULL in the new rate structure, then
--    insert records for these
-- 2. If Aggregation Detail is present in both old and new rate structure
--    then update the record
-- 3. If Aggregation detail is present in old structure, but missing in
--    new structure, then delete it
-------------------------------------------------------------------------
-- NOTE: PERFORM ALL THE STEPS IN A SINGLE TRANSACTION BLOCK
-------------------------------------------------------------------------
Begin Try

	Begin Transaction Commit_RateStructure

	---------------------------------------------------------------
	-- Update Currency ID for Agrgegate Rate in case it has changed
	---------------------------------------------------------------
	if (@OldCurrencyID <> @CurrencyID)
	Begin
			update tb_AggregationRate
			set CurrencyID = @CurrencyID
			where AggregationRateID = @AggregationRateID
	End

	-----------------------------------------------------------------
	-- Delete all the Rate Structures that are part of old data, but 
	-- are missing in the new data
	-----------------------------------------------------------------
	delete from Tb_AggregationRateDetail
	where AggregationRateDetailID in
	(
		Select tbl1.AggregationRateDetailID
		from #TempOldRateStructure tbl1
		left join (Select * from #TempRateStructureFinal where AggregationRateDetailID is not NULL) tbl2
				 on tbl1.AggregationRateDetailID =  tbl2.AggregationRateDetailID
		Where tbl2.AggregationRateDetailID is NULL
	)

	--------------------------------------------------------------------
	-- update all the Rate Structures that are part of old and new data
	--------------------------------------------------------------------
	update tbl3
	set [From] = tbl2.[From],
	    [To] = tbl2.[To],
		ApplyFrom = tbl2.ApplyFrom,
		Rate = tbl2.Rate,
		TierID = tbl2.TierID,
		ModifiedDate = getdate(),
		ModifiedByID = @UserID
	from #TempOldRateStructure tbl1
	inner join (Select * from #TempRateStructureFinal where AggregationRateDetailID is not NULL) tbl2
				on tbl1.AggregationRateDetailID =  tbl2.AggregationRateDetailID
	inner join Tb_AggregationRateDetail tbl3 on tbl1.AggregationRateDetailID = tbl3.AggregationRateDetailID

	----------------------------------------------------------
	-- Insert records for all the new Aggregate Detail records
	----------------------------------------------------------

	Insert into Tb_AggregationRateDetail
	(
		AggregationRateID,
		TierID,
		[From],
		[To],
		ApplyFrom,
		Rate,
		ModifiedDate,
		ModifiedByID
	)
	Select @AggregationRateID,
		   TierID,
		   [From],
		   [To],
		   ApplyFrom,
		   Rate,
		   getdate(),
		   @UserID
	from #TempRateStructureFinal
	where AggregationRateDetailID is NULL

End Try

Begin Catch 

	set @ErrorDescription = 'ERROR !!! While updatig the rate structure details for aggregation cycle. ' + ERROR_MESSAGE()
	set @ResultFlag = 1

	Rollback Transaction Commit_RateStructure

	GOTO ENDPROCESS
	
End Catch

-- In case no exceptions are encountered, commit all the changes
Commit Transaction Commit_RateStructure


ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureFrom') )
	Drop table #TempRateStructureFrom

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureTo') )
	Drop table #TempRateStructureTo

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureApplyFrom') )
	Drop table #TempRateStructureApplyFrom

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureRate') )
	Drop table #TempRateStructureRate

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructure') )
	Drop table #TempRateStructure

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempOldRateStructure') )
	Drop table #TempOldRateStructure

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRateStructureRateDetailsID') )
	Drop table #TempRateStructureRateDetailsID

return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIValidateAggregationCycleInfo]    Script Date: 7/10/2021 12:36:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE     or alter Procedure [dbo].[SP_UIValidateAggregationCycleInfo]
(
	@AggregationID int,
	@AggregationCycle varchar(100),
	@CommitmentType int,
	@GracePeriod int,
	@Penalty int,
	@AggregationCriteriaID int,
	@StartDate date,
	@EndDate date,
	@GracePeriodEndDate date = NULL,
	@Commitment Decimal(19,4) = NULL,
	@ErrorDescription varchar(2000) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag  = 0

Declare @AggregationTypeID int,
        @AggregationType varchar(100),
		@AggregationCriteria varchar(100)

---------------------------------------------------------
-- Check to confirm if the aggregation ID passed for
-- creation of aggregation cycle is valid or not
---------------------------------------------------------
if not exists (select 1 from tb_Aggregation where AggregationID = @AggregationID)
Begin

	set @ErrorDescription = 'ERROR !!! The Aggregation ID passed for creation of cycle is not valid and does not exist in system.'
	set @ResultFlag = 1
	return 1

End

-------------------------------------------------------------
-- Check to ensure that the cycle start date is earlier than
-- the cycle end date
-------------------------------------------------------------
if (@StartDate >= @EndDate)
Begin

	set @ErrorDescription = 'ERROR !!! Start Date for aggregation cycle cannot be greater or equal to End Date.'
	set @ResultFlag = 1
	return 1

End

------------------------------------------------------------
-- Check to ensure that valid values are passed in the
-- fields:
-- 1. CommitmentType  (0,1,2)
-- 2. GracePeriod (0,1)
-- 3. Penalty (0,1)
------------------------------------------------------------
if (@CommitmentType not in (0,1,2))
Begin

	set @ErrorDescription = 'ERROR !!! The value passed for Commitmenttype is not valid'
	set @ResultFlag = 1
	return 1

End

if (@GracePeriod not in (0,1))
Begin

	set @ErrorDescription = 'ERROR !!! The value passed for Grace Period is not valid'
	set @ResultFlag = 1
	return 1

End

if (@Penalty not in (0,1))
Begin

	set @ErrorDescription = 'ERROR !!! The value passed for Penalty is not valid'
	set @ResultFlag = 1
	return 1

End

-------------------------------------------------------------
-- Check to ensure that Aggregation criteria is a valid value
-------------------------------------------------------------
if not exists (select 1 from Tb_AggregationCriteria where AggregationCriteriaID = @AggregationCriteriaID)
Begin

	set @ErrorDescription = 'ERROR !!! The value passed for Aggregation Criteria is not valid'
	set @ResultFlag = 1
	return 1

End

---------------------------------------------------------
-- If the Commitment type is 0, then Commitment parameter
-- should have NULL value
---------------------------------------------------------
if (@CommitmentType = 0 and @Commitment is NOT NULL)
Begin

	set @ErrorDescription = 'ERROR !!! There cannot be a commitment amount or volume, if the commitment type is set to NONE.'
	set @ResultFlag = 1
	return 1

End

---------------------------------------------------------
-- If the Commitment type is not 0, then Commitment parameter
-- should have a valid value
---------------------------------------------------------
if (@CommitmentType in (1,2) and @Commitment is NULL)
Begin

	set @ErrorDescription = 'ERROR !!! There needs to be a commitment amount or volume defined, if the commitment type is set to AMOUNT/VOLUME.'
	set @ResultFlag = 1
	return 1

End

------------------------------------------------------
-- if the Grace Period is set to 0 then there can be
-- no Grace period End Date
------------------------------------------------------
if (@GracePeriod = 0 and @GracePeriodEndDate is NOT NULL )
Begin

	set @ErrorDescription = 'ERROR !!! There cannot be a Grace Period End Date defined if option of NO grace period is selected.'
	set @ResultFlag = 1
	return 1

End

------------------------------------------------------
-- if the Grace Period is set to 1 then there has to
-- be a Grace period End Date
------------------------------------------------------
if (@GracePeriod = 1 and @GracePeriodEndDate is NULL )
Begin

	set @ErrorDescription = 'ERROR !!! There needs to be a Grace Period End Date defined if option of YES grace period is selected.'
	set @ResultFlag = 1
	return 1

End

---------------------------------------------------------
-- check if a valid aggregation type exists in the system
-- for the combination of:
-- 1. Commitment type
-- 2. Grace Period
-- 3. Penalty
---------------------------------------------------------
Select  @AggregationTypeID = AggregationTypeID,
        @AggregationType = AggregationType
from Tb_AggregationType 
where CommitmentType = @CommitmentType 
and GracePeriod = @GracePeriod 
and Penalty = @Penalty

if (@AggregationTypeID is NULL)
Begin

	set @ErrorDescription = 'ERROR !!! Combination of attributes ' + 
							'Commitment Type : ' + Case 
														When @CommitmentType = 0 then 'NONE'
														When @CommitmentType = 1 then 'VOLUME'
														When @CommitmentType = 2 then 'AMOUNT'
							                      End + ' and '+
							'Grace Period : '  +  Case 
														When @GracePeriod = 0 then 'NO'
														When @GracePeriod = 1 then 'YES'
							                        End + ' and '+
							'Penalty : '  +       Case 
														When @Penalty = 0 then 'NO'
														When @Penalty = 1 then 'YES'
							                      End + ' not valid for configuring aggregation cycle. Please change to a valid combination.'
	set @ResultFlag = 1
	return 1

End

---------------------------------------------------
-- Check to ensure that the Grace Period End Date
-- is greater than the Cycle End Date
---------------------------------------------------
if (@GracePeriod = 1 and @GracePeriodEndDate <= @EndDate)
Begin

	set @ErrorDescription = 'ERROR !!! The Grace Period End Date has to be greater than the Aggregation cycle End Date.'
	set @ResultFlag = 1
	return 1

End

----------------------------------------------------------
-- check to ensure that the combination of Aggregation type
-- and Aggregation criteria are valid
----------------------------------------------------------
Select @AggregationCriteria = AggregationCriteria
From Tb_AggregationCriteria
where AggregationCriteriaID = @AggregationCriteriaID

if not exists (Select 1 from tb_AggregationTypeAndCriteriaMapping where AggregationTypeID = @AggregationTypeID and AggregationCriteriaID = @AggregationCriteriaID)
Begin

	set @ErrorDescription = 'ERROR !!! An aggreation cycle of type : (' + @AggregationType + ') cannot have aggregation criteria as : (' + @AggregationCriteria + ').'
	set @ResultFlag = 1
	return 1

End


Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIWizardAggregationGroupingAddNew]    Script Date: 7/10/2021 12:36:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   or alter Procedure [dbo].[SP_UIWizardAggregationGroupingAddNew]
(
	@SessionID varchar(200),
	@AggregationGroupingIDList varchar(Max),
	@UserID int,
	@ErrorDescription varchar(2000) Output,
	@ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @TableName varchar(200),
        @SQLStr varchar(max)

--------------------------------------------------------
-- Create the name of the temp table for this session
-- using the session ID and check if it exists or not
--------------------------------------------------------
set @TableName = 'wtb_AggregationGrouping_' + @SessionID 

if not exists (select 1 from sysobjects where name = @TableName and xtype = 'u')
Begin

	set @ErrorDescription = 'ERROR !!!! The schema for qualifying aggregation groupings does not exist'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

----------------------------------------------------------------
-- Move the data from session table to a Temp table so that 
-- we can use direct queries instead of dynamic ones
----------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGroupings') )
		Drop table #TempAggregationGroupings

Create table #TempAggregationGroupings
(
    AggregationID int,
	RecordID int,
	CommercialTrunkID int,
	ServiceLevelID int,
	CallTypeID int,
	DestinationID int,
	StartDate date,
	EndDate date
)

set @SQLStr = 'Insert into #TempAggregationGroupings '  + char(10) +
              '(AggregationID , RecordID ,CommercialTrunkID, ServiceLevelID, CallTypeID, DestinationID, StartDate, EndDate)' + char(10)+
			  'Select AggregationID ,RecordID ,CommercialTrunkID, ServiceLevelID, CallTypeID, DestinationID, StartDate, EndDate' + char(10)+
			  'From ' + @TableName

print @SQLStr

Exec(@SQLStr)

----------------------------------------------------------
-- Parse the list of qualifying Aggregation Groupings
-- and check that they exist in the temp table for session
----------------------------------------------------------
-----------------------------------------------------------------
-- Create table for list of all selected aggregation Groupings from
-- the parameter passed
-----------------------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGroupingIDTable') )
		Drop table #TempAggregationGroupingIDTable

Create table #TempAggregationGroupingIDTable (AggregationGroupingID varchar(100) )


insert into #TempAggregationGroupingIDTable
select * from FN_ParseValueList ( @AggregationGroupingIDList )

----------------------------------------------------------------
-- Check to ensure that none of the values are non numeric
----------------------------------------------------------------

if exists ( select 1 from #TempAggregationGroupingIDTable where ISNUMERIC(AggregationGroupingID) = 0 )
Begin

	set @ErrorDescription = 'ERROR !!! List of Aggregation Grouping IDs passed contain a non numeric value'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End
		
---------------------------------------------------------------------------------
-- Check to ensure that all the Aggregation Grouping IDs passed are valid values
---------------------------------------------------------------------------------
		
if exists ( 
				select 1 
				from #TempAggregationGroupingIDTable 
				where AggregationGroupingID not in
				(
					Select RecordID
					from #TempAggregationGroupings
				)
			)
Begin

	set @ErrorDescription = 'ERROR !!! List of Aggregation Grouping IDs passed contain value(s) which are not valid or do not exist'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

----------------------------------------------------------------------
-- Insert data into the tb_AggregationGrouping table for the qualifying
-- records
----------------------------------------------------------------------
Begin Try

	insert into Tb_AggregationGrouping
	(
		AggregationID,
		CommercialTrunkID,
		ServiceLevelID,
		DestinationID,
		CallTypeID,
		StartDate,
		EndDate,
		ModifiedDate,
		ModifiedByID
	)
	Select 	AggregationID,
			CommercialTrunkID,
			ServiceLevelID,
			DestinationID,
			CallTypeID,
			StartDate,
			EndDate,
			getdate(),
			@UserID
	From #TempAggregationGroupings tbl1
	inner join #TempAggregationGroupingIDTable tbl2 on tbl1.RecordID = tbl2.AggregationGroupingID

End Try

Begin Catch

	set @ErrorDescription = 'ERROR !!! While adding new Aggregation Groupings. ' + ERROR_MESSAGE()
	set @ResultFlag = 1
	GOTO ENDPROCESS

End Catch


ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGroupings') )
		Drop table #TempAggregationGroupings
GO
/****** Object:  StoredProcedure [dbo].[SP_UIWizardAggregationGroupingDeleteSessionSchema]    Script Date: 7/10/2021 12:36:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
Create   or alter Procedure [dbo].[SP_UIWizardAggregationGroupingDeleteSessionSchema]
(
	@SessionID varchar(200)
)
As

Declare @TableName varchar(200)

set @TableName = 'wtb_AggregationGrouping_' + @SessionID 

if exists (select 1 from sysobjects where name = @TableName and xtype = 'u')
	Exec('Drop table ' + @TableName)

Return 0
GO
/****** Object:  StoredProcedure [dbo].[SP_UIWizardAggregationGroupingValidate]    Script Date: 7/10/2021 12:36:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   or alter Procedure [dbo].[SP_UIWizardAggregationGroupingValidate]
(
	@AggregationID int,
	@BeginDate date,
	@EndDate date,
	@CommercialTrunkIDList varchar(max),
	@CallTypeIDList varchar(max),
	@ServiceLevelIDList varchar(max),
	@DestinationIDList varchar(max),
	@SessionID varchar(200) output,
	@ErrorDescription varchar(2000) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @DirectionID int,
        @NumberPlanID int,
		@AccountID int,
		@TableName varchar(200),
		@SQLStr varchar(max)

------------------------------------------------------
-- Check to see if the aggregation ID is valid or not
------------------------------------------------------
if not exists (select 1 from Tb_Aggregation where AggregationID = @AggregationID)
Begin

	set @ErrorDescription = 'ERROR !!! Aggregation ID is either not valid or it does not exist in the system.'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

-------------------------------------------------------
-- Get the direction for the aggregation, as it will
-- be used to verify that destinations are picked from
-- appropriate number plan
-------------------------------------------------------
Select @DirectionID = directionID,
       @AccountID = AccountID
from Tb_Aggregation tbl1
inner join tb_Agreement tbl2 on tbl1.AgreementID = tbl2.AgreementID
where AggregationID = @AggregationID

set @NumberPlanID = Case 
						When @DirectionID = 1 then -2 
						When @DirectionID = 2 then -1
					End

-------------------------------------------------------------
-- If the End Date is not NULL, then ensure that Start Date
-- should be less than the End Date
-------------------------------------------------------------
if ((@EndDate is not NULL) and (@BeginDate >= @EndDate))
Begin

	set @ErrorDescription = 'ERROR !!! Begin Date for the Aggregation Grouping should be less than the End Date'
	set @ResultFlag = 1
	GOTO ENDPROCESS

End

---------------------------------------------------------
-- Parse the Lists for Commercial Trunk , Call type,
-- Service Level and Destinations and store in Temp
-- table
---------------------------------------------------------
Begin Try

	---------------------------
	-- SERVICE LEVEL ID LIST
	---------------------------

		-----------------------------------------------------------------
		-- Create table for list of all selected Service Levels from the 
		-- parameter passed
		-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempServiceLevelIDTable') )
				Drop table #TempServiceLevelIDTable

		Create table #TempServiceLevelIDTable (ServiceLevelID varchar(100) )


		insert into #TempServiceLevelIDTable
		select * from FN_ParseValueList ( @ServiceLevelIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempServiceLevelIDTable where ISNUMERIC(ServiceLevelID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Service Level IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End
		
        --------------------------------------------------------------------------
		-- Check to ensure that all the Service Level IDs passed are valid values
		--------------------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempServiceLevelIDTable 
						where ServiceLevelID not in
						(
							Select ServiceLevelID
							from ReferenceServer.UC_Reference.dbo.tb_ServiceLevel
							where DirectionID = 1 -- All INBOUND Service Levels
							and flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Service Level IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End 


	-------------------------
	-- DESTINATION ID LIST
	-------------------------

		-----------------------------------------------------------------
		-- Create table for list of all selected Destinations from the 
		-- parameter passed
		-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationIDTable') )
				Drop table #TempDestinationIDTable

		Create table  #TempDestinationIDTable (DestinationID varchar(100) )


		insert into #TempDestinationIDTable
		select * from FN_ParseValueList ( @DestinationIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempDestinationIDTable where ISNUMERIC(DestinationID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End
		
        --------------------------------------------------------------------------
		-- Check to ensure that all the Destinations passed are valid values
		--------------------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempDestinationIDTable 
						where DestinationID not in
						(
							Select DestinationID
							from ReferenceServer.UC_Reference.dbo.tb_Destination
							where numberplanID = @NumberPlanID -- Can be selling or routing number plan based on the direction of aggregation
							and flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

	-------------------------------
	-- COMMERCIAL TRUNK ID LIST
	-------------------------------

		-----------------------------------------------------------------
		-- Create table for list of selected Commercial Trunks from the 
		-- parameter passed
		-----------------------------------------------------------------
		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCommercialTrunkIDTable') )
				Drop table #TempCommercialTrunkIDTable

		Create table #TempCommercialTrunkIDTable (CommercialTrunkID varchar(100) )


		insert into #TempCommercialTrunkIDTable
		select * from FN_ParseValueList ( @CommercialTrunkIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempCommercialTrunkIDTable where ISNUMERIC(CommercialTrunkID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of CommercialTrunk IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the CommercialTrunk IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempCommercialTrunkIDTable 
						where CommercialTrunkID not in
						(
							Select TrunkID
							from ReferenceServer.UC_Reference.dbo.tb_Trunk
							where trunktypeID = 9 -- Commercial trunk
							and accountid = @AccountID -- Only look at the Commercial Trunks for the account which has the aggregation 
							and flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of CommercialTrunk IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End


	-----------------------
	-- CALL TYPE ID LIST
	-----------------------

		---------------------------------------------------------
		-- Create table for list of selected Call Type from the 
		-- parameter passed
		---------------------------------------------------------
		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCallTypeIDTable') )
				Drop table #TempCallTypeIDTable

		Create table #TempCallTypeIDTable (CallTypeID varchar(100) )


		insert into #TempCallTypeIDTable
		select * from FN_ParseValueList ( @CallTypeIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempCallTypeIDTable where ISNUMERIC(CallTypeID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of CallType IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the Call Type IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempCallTypeIDTable 
						where CallTypeID not in
						(
							Select CallTypeID
							from ReferenceServer.UC_Reference.dbo.tb_CallType
							where flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of CallType IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! Exception encountered when parsing attribute lists. '+ ERROR_MESSAGE()
		set @ResultFlag = 1
		GOTO ENDPROCESS

End Catch

-------------------------------------------------------------------
-- Build a combination of the all the attributes and validate
-- all the qualifying Aggregation Groupings
-------------------------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGroupingCombinations') )
		Drop table #TempAggregationGroupingCombinations

Select @AggregationID as AggregationID, CommercialTrunkID, 
       ServiceLevelID , CallTypeID, DestinationID,
	   @BeginDate as StartDate , @EndDate as EndDate
into #TempAggregationGroupingCombinations
from #TempCommercialTrunkIDTable tbl1
cross join #TempDestinationIDTable tbl2
cross join #TempServiceLevelIDTable tbl3
cross join #TempCallTypeIDTable

-----------------------------------------------
-- Alter the table to add RecordID and Error
-- Description columns to the schema
-----------------------------------------------
Alter Table #TempAggregationGroupingCombinations Add RecordID int identity(1,1)
Alter Table #TempAggregationGroupingCombinations Add ErrorDescription varchar(200)

-- DEBUG STATEMENT
--Select *
--from #TempAggregationGroupingCombinations

-----------------------------------------------------
-- Get a list of existing Aggregation Grouping 
-- combinations for the account to see if there is any
-- overlapping of the new aggregation groupings with
-- the existing ones
-----------------------------------------------------
if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempExistingAggregationGroupingCombinations') )
		Drop table #TempExistingAggregationGroupingCombinations

Select tbl1.CommercialTrunkID , tbl1.ServiceLevelID, 
       tbl1.CallTypeID , tbl1.DestinationID,
	   tbl1.StartDate , tbl1.EndDate
into #TempExistingAggregationGroupingCombinations
from Tb_AggregationGrouping tbl1
inner join Tb_Aggregation tbl2 on tbl1.AggregationID = tbl2.AggregationID
inner join tb_Agreement tbl3 on tbl2.AgreementID = tbl3.AgreementID
inner join #TempAggregationGroupingCombinations tbl4 on tbl1.CommercialTrunkID = tbl4.CommercialTrunkID
                                                    and tbl1.ServiceLevelID = tbl4.ServiceLevelID
													and tbl1.CallTypeID = tbl4.CallTypeID
													and tbl1.DestinationID = tbl4.DestinationID
where tbl2.DirectionID = @DirectionID

-- DEBUG STATEMENT
--Select *
--from #TempExistingAggregationGroupingCombinations

--------------------------------------------------
-- If there are no existing aggregation groupings
-- then we skip the process to check for date 
-- overlap
--------------------------------------------------
if ((Select count(*) from #TempExistingAggregationGroupingCombinations) = 0)
Begin

	GOTO DISPLAYRESULTSET

End

--------------------------------------------------------------------
-- Perform the date overlap check for all the aggregation groupings
-- that have an existing record in other aggregations for the account
--------------------------------------------------------------------
Declare @VarRecordID int,
        @VarCommercialTrunkID int,
		@VarServiceLevelID int,
		@VarCalltypeID int,
		@VarDestinationID int,
		@VarStartDate date,
		@VarEndDate date,
		@ResultFlag2 int = 0

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDateOverlapCheck') )
		Drop table #TempDateOverlapCheck

Create table #TempDateOverlapCheck (BeginDate date , EndDate date)

Declare Validate_AggregateGrouping_Cur cursor for
Select tbl1.RecordID , tbl1.CommercialTrunkID , tbl1.ServiceLevelID,
       tbl1.CallTypeID , tbl1.DestinationID , tbl1.StartDate , tbl1.EndDate
from #TempAggregationGroupingCombinations tbl1
inner join #TempExistingAggregationGroupingCombinations tbl2 on tbl1.CommercialTrunkID = tbl2.CommercialTrunkID
															and tbl1.ServiceLevelID = tbl2.ServiceLevelID
															and tbl1.CallTypeID = tbl2.CallTypeID
															and tbl1.DestinationID = tbl2.DestinationID

Open Validate_AggregateGrouping_Cur
Fetch Next from Validate_AggregateGrouping_Cur
Into @VarRecordID, @VarCommercialTrunkID ,@VarServiceLevelID ,
	 @VarCalltypeID ,@VarDestinationID ,@VarStartDate,
	 @VarEndDate

While @@FETCH_STATUS = 0
Begin

    -------------------------------------------------------------
	-- initialize and re-populate the temp schema holding dates
	-- for existing aggregation groupings
    -------------------------------------------------------------
	delete from #TempDateOverlapCheck

	insert into #TempDateOverlapCheck
	select distinct StartDate , EndDate
	from #TempExistingAggregationGroupingCombinations
	where CommercialTrunkID = @VarCommercialTrunkID
	and ServiceLevelID = @VarServiceLevelID
	and CallTypeID = @VarCalltypeID
	and DestinationID = @VarDestinationID

	------------------------------------------------
	-- Call the procedure to check for date overlap
	------------------------------------------------
	set @ResultFlag2 = 0 -- Initialize the flag

	Exec SP_BSCheckDateOverlap @VarStartDate , @VarEndDate , @ResultFlag2 Output
	
	-- If the aggregation grouping has a date overlpa, then update the record
	-- with the error description

	if (@ResultFlag2 = 1)
	Begin

			update #TempAggregationGroupingCombinations
			set ErrorDescription = 'ERROR !!! Aggregation Grouping active under another aggregation.' 
			where RecordID = @VarRecordID

	End

	Fetch Next from Validate_AggregateGrouping_Cur
	Into @VarRecordID, @VarCommercialTrunkID ,@VarServiceLevelID ,
		 @VarCalltypeID ,@VarDestinationID ,@VarStartDate,
		 @VarEndDate

End

Close Validate_AggregateGrouping_Cur
DeAllocate Validate_AggregateGrouping_Cur


DISPLAYRESULTSET:

Begin Try

		--------------------------------------------------------------
		-- Call the procedure to get the unique session ID for the
		-- table in which data will be stored
		--------------------------------------------------------------
		Exec SP_UIWizardGetUniqueSessionID @SessionID Output

		----------------------------------------------
		-- Create a unique table name, in which the 
		-- data will be stored
		----------------------------------------------
		set @SessionID = replace(@SessionID , '-' , '_')
		set @TableName = 'wtb_AggregationGrouping_' + @SessionID 

		-------------------------------------------
		-- Store the Data in the Temp table for UI
		-------------------------------------------
		set @SQLStr = 'Select ' + convert(varchar(100) , @AggregationID) + ' as AggregationID , tbl1.RecordID , ' + char(10) +
		              'tbl1.CommercialTrunkID , tbl2.Trunk as CommercialTrunk, ' + char(10) +
					  'tbl1.ServiceLevelID , tbl3.ServiceLevel, tbl1.CallTypeID, tbl4.CallType ,' + char(10) +
					  'tbl1.DestinationID , tbl5.Destination , tbl1.StartDate, tbl1.EndDate , tbl1.ErrorDescription' + char(10)+
					  'into ' + @TableName + char(10) +
					  'from #TempAggregationGroupingCombinations tbl1 ' + char(10)+
					  'inner join tb_Trunk tbl2 on tbl1.CommercialTrunkID = tbl2.TrunkID' + char(10) +
					  'inner join tb_ServiceLevel tbl3 on tbl1.ServiceLevelID = tbl3.ServiceLevelID' + char(10) +
					  'inner join tb_Calltype tbl4 on tbl1.CallTypeID = tbl4.CallTypeID' + char(10) +
					  'inner join tb_Destination tbl5 on tbl1.DestinationID = tbl5.DestinationID'
			  
		--print @SQLStr
		Exec(@SQLStr)

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! When storing the validated Aggregation groupings in session specific schema.' + ERROR_MESSAGE()
		set @ResultFlag = 1

		if exists (Select 1 from sysobjects where name = @TableName and xtype = 'u')
			Exec('Drop table ' + @TableName)

		GOTO ENDPROCESS

End Catch

--------------------------------------------
-- Display the result set for UI to publish
--------------------------------------------
Exec('Select * from ' + @TableName)


ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempServiceLevelIDTable') )
		Drop table #TempServiceLevelIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCommercialTrunkIDTable') )
		Drop table #TempCommercialTrunkIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationIDTable') )
		Drop table #TempDestinationIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCallTypeIDTable') )
		Drop table #TempCallTypeIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempAggregationGroupingCombinations') )
		Drop table #TempAggregationGroupingCombinations

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempExistingAggregationGroupingCombinations') )
		Drop table #TempExistingAggregationGroupingCombinations

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDateOverlapCheck') )
		Drop table #TempDateOverlapCheck





GO
/****** Object:  StoredProcedure [dbo].[SP_UIWizardGetDestinationByCountryForAggregationGrouping]    Script Date: 7/10/2021 12:36:35 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   or alter Procedure [dbo].[SP_UIWizardGetDestinationByCountryForAggregationGrouping]
(
	@CountryIDList nvarchar(max),
	@AggregationStartDate date,
	@AggregationEndDate date = NULL,
	@AggregationID int,
	@UserID int,
	@ErrorDescription varchar(2000) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @CountryIDTable table (CountryID varchar(100) )

Declare @NumberPlanID int,
		@DirectionID int

-----------------------------------------------------
-- Get the Direction for the aggregation and on
-- basis of that select the appropriate numberplan
-----------------------------------------------------
Select @DirectionID = directionID
from Tb_Aggregation
where AggregationID = @AggregationID

set @NumberPlanID = Case 
						When @DirectionID = 1 Then -2 
						When @DirectionID = 2 then -1 
					End

Begin Try

		insert into @CountryIDTable
		select * from FN_ParseValueList ( @CountryIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from @CountryIDTable where ISNUMERIC(CountryID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Country IDs passed contain a non numeric value'
			set @ResultFlag = 1
			Return 1

		End

		------------------------------------------------------
		-- Check if the All the countries have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from @CountryIDTable 
						where CountryID = 0
				  )
		Begin

				  Delete from @CountryIDTable -- Remove all records

				  insert into @CountryIDTable (  CountryID )
				  Select countryID
				  from tb_country
				  where flag & 1  <> 1 -- Insert all the countries into the temp table

				  GOTO PROCESSRESULT
				  
		End
		
        -------------------------------------------------------------------
		-- Check to ensure that all the Country IDs passed are valid values
		-------------------------------------------------------------------
		
		if exists ( 
						select 1 
						from @CountryIDTable 
						where CountryID not in
						(
							Select CountryID
							from tb_Country
							where flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Country IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			Return 1

		End

PROCESSRESULT:

		Select tbl1.DestinationID as ID , 
		       tbl1.Destination + ' ' + '(' +replace(CONVERT(varchar(10) , tbl1.BeginDate , 120 ) , '-' , '/') + ' - '+ 
							Case
									When tbl1.EndDate is not NULL then replace(CONVERT(varchar(10) , tbl1.EndDate , 120 ) , '-' , '/')
									Else 'Open'
							End  + ')'as Name
		from tb_Destination tbl1
		inner join @CountryIDTable tbl2 on tbl1.CountryID =  tbl2.CountryID
		where tbl1.numberplanid = @NumberPlanID
		and tbl1.flag & 1 <> 1
		and 
		(
		   @AggregationStartDate between tbl1.BeginDate and isnull(tbl1.EndDate , @AggregationStartDate)
		   or
		   (
				@AggregationStartDate <= tbl1.BeginDate
				and
				(
					@AggregationEndDate is NULL 
					or
					@AggregationEndDate >= tbl1.BeginDate

				)
		   )
		)
		order by tbl1.Destination

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! Returning List of Destinations for Wizard.' + ERROR_MESSAGE()
		set @ResultFlag = 1
		Return 1

End Catch
GO
