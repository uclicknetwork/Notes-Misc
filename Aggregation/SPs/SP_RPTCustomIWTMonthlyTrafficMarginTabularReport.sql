USE [UC_Report]
GO

/****** Object:  StoredProcedure [dbo].[SP_RPTCustomIWTMonthlyTrafficMarginTabularReport]    Script Date: 7/15/2021 3:23:24 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE Procedure [dbo].[SP_RPTCustomIWTMonthlyTrafficMarginTabularReport]
(
     @ReportID int,
	 @StartMonth int,
	 @EndMonth int, 
	 @CallTypeID int,
	 @INAccountIDList nvarchar(max), 
	 @OUTAccountIDList nvarchar(max),
	 @INCommercialTrunkIDList nvarchar(max),
	 @OUTCommercialTrunkIDList nvarchar(max),
	 @CountryIDList nvarchar(max),
	 @DestinationIDList nvarchar(max),
 	 @ServiceLevelIDList nvarchar(max),
	 @DestinationGroupIDList nvarchar(max),
	 @TotalResult nvarchar(max) Output,
 	 @ErrorDescription varchar(2000) Output,
	 @ResultFlag int Output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AllCountryFlag int = 0,
        @AllServiceLevelFlag int = 0,
		@AllDestinationFlag int = 0,
		@INAllAccountFlag int = 0,
		@OUTAllAccountFlag int = 0,
		@AllDestinationGroupFlag int = 0,
		@INAllCommercialTrunkFlag int = 0,
		@OUTAllCommercialTrunkFlag int = 0,
		@SQLStr1 nvarchar(max),
		@SQLStr2 nvarchar(max),
		@SQLStr3 nvarchar(max),
		@SQLStr  nvarchar(max)


---------------------------------------------------------
-- Check if the Report is valid and exists in he system
---------------------------------------------------------

if not exists ( select 1 from tb_Report where ReportID = @ReportID and Flag & 1 <> 1 )
Begin

		set @ErrorDescription = 'ERROR !!! Report ID is not valid or is not active (flag <> 0)'
		set @ResultFlag = 1
		GOTO ENDPROCESS

End


-------------------------------------------------------------
-- Set the CALL TYPE to NULL in case the value passed is 0
-- indicating that all CALL TYPES need to be considered
-------------------------------------------------------------

if ( @CallTypeID = 0 )
	set @CallTypeID = NULL

Begin Try

-----------------------------------------------------------------
-- Create table for list of selected IN Accounts from the parameter
-- passed
-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempINAccountIDTable') )
				Drop table #TempINAccountIDTable

		Create Table #TempINAccountIDTable (AccountID varchar(100) )


		insert into #TempINAccountIDTable
		select * from FN_ParseValueList ( @INAccountIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempINAccountIDTable where ISNUMERIC(AccountID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of IN Account IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the Accounts have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempINAccountIDTable 
						where AccountID = 0
				  )
		Begin

                  set @INAllAccountFlag = 1
				  GOTO PROCESSOUTACCOUNT
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the Account IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempINAccountIDTable 
						where AccountID not in
						(
							Select AccountID
							from ReferenceServer.UC_Reference.dbo.tb_Account
							where flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of IN Account IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

PROCESSOUTACCOUNT:

-----------------------------------------------------------------
-- Create table for list of selected OUT Accounts from the parameter
-- passed
-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempOUTAccountIDTable') )
				Drop table #TempOUTAccountIDTable

		Create Table #TempOUTAccountIDTable (AccountID varchar(100) )


		insert into #TempOUTAccountIDTable
		select * from FN_ParseValueList ( @OUTAccountIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempOUTAccountIDTable where ISNUMERIC(AccountID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of OUT Account IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the Accounts have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempOUTAccountIDTable 
						where AccountID = 0
				  )
		Begin

                  set @OUTAllAccountFlag = 1
				  GOTO PROCESSCOUNTRY
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the Account IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempOUTAccountIDTable 
						where AccountID not in
						(
							Select AccountID
							from ReferenceServer.UC_Reference.dbo.tb_Account
							where flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of OUT Account IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

PROCESSCOUNTRY:

		-----------------------------------------------------------------
		-- Create table for list of all selected Countries from the 
		-- parameter passed
		-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCountryIDTable') )
				Drop table #TempCountryIDTable


		Create table #TempCountryIDTable (CountryID varchar(100) )
		
		insert into #TempCountryIDTable
		select * from FN_ParseValueList ( @CountryIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempCountryIDTable where ISNUMERIC(CountryID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Country IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the countries have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempCountryIDTable 
						where CountryID = 0
				  )
		Begin

                  set @AllCountryFlag = 1
				  GOTO PROCESSSERVICELEVEL
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the Country IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempCountryIDTable 
						where CountryID not in
						(
							Select CountryID
							from ReferenceServer.UC_Reference.dbo.tb_country
							where flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Country IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End 

PROCESSSERVICELEVEL:

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

PROCESSDESTINATIONLIST:

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
			Return 1

		End

		------------------------------------------------------
		-- Check if the All the Detinations have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempDestinationIDTable 
						where DEstinationID = 0
				  )
		Begin

				  set @AllDestinationFlag = 1
				  GOTO PROCESSDESTINATIONGROUPLIST
				  
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
							where numberplanID = -1 -- All Routing Destinations
							and flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			Return 1

		End 

PROCESSDESTINATIONGROUPLIST:

		-----------------------------------------------------------------
		-- Create table for list of all selected Destinations groups from 
		-- the parameter passed
		-----------------------------------------------------------------

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationGroupIDTable') )
				Drop table #TempDestinationGroupIDTable

		Create table  #TempDestinationGroupIDTable (DestinationGroupID varchar(100) )


		insert into #TempDestinationGroupIDTable
		select * from FN_ParseValueList ( @DestinationGroupIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempDestinationGroupIDTable where ISNUMERIC(DestinationGroupID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination Group IDs passed contain a non numeric value'
			set @ResultFlag = 1
			Return 1

		End

		------------------------------------------------------
		-- Check if the All the Detinations have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempDestinationGroupIDTable 
						where DEstinationGroupID = 0
				  )
		Begin

				  set @AllDestinationGroupFlag = 1
				  GOTO PROCESSINCOMMERCIALTRUNK
				  
		End
		
        --------------------------------------------------------------------------
		-- Check to ensure that all the Destination Groups passed are valid values
		--------------------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempDestinationGroupIDTable 
						where DestinationGroupID not in
						(
							select distinct tbl1.EntityGroupID
							from Referenceserver.UC_Reference.dbo.tb_EntityGroup tbl1
							inner join Referenceserver.UC_Reference.dbo.tb_EntityGroupMember tbl2 on tbl1.EntityGroupID = tbl2.EntityGroupID
							inner join Referenceserver.UC_Reference.dbo.tb_Destination tbl3 on tbl2.InstanceID = tbl3.DestinationID
							where EntityGroupTypeID = -2
							and tbl3.NumberPlanID = -1 -- Only Routing number plan grouping
							and tbl1.Flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of Destination Group IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			Return 1

		End


PROCESSINCOMMERCIALTRUNK:

		-----------------------------------------------------------------
		-- Create table for list of selected IN Commercial Trunks from the 
		-- parameter passed
		-----------------------------------------------------------------
		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempINCommercialTrunkIDTable') )
				Drop table #TempINCommercialTrunkIDTable

		Create table #TempINCommercialTrunkIDTable (CommercialTrunkID varchar(100) )


		insert into #TempINCommercialTrunkIDTable
		select * from FN_ParseValueList ( @INCommercialTrunkIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempINCommercialTrunkIDTable where ISNUMERIC(CommercialTrunkID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of IN CommercialTrunk IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the CommercialTrunks have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempINCommercialTrunkIDTable 
						where CommercialTrunkID = 0
				  )
		Begin

                  set @INAllCommercialTrunkFlag = 1
				  GOTO PROCESSOUTCOMMERCIALTRUNK
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the CommercialTrunk IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempINCommercialTrunkIDTable 
						where CommercialTrunkID not in
						(
							Select TrunkID
							from ReferenceServer.UC_Reference.dbo.tb_Trunk
							where trunktypeID = 9 -- Commercial trunk
							and flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of IN CommercialTrunk IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

PROCESSOUTCOMMERCIALTRUNK:

		-----------------------------------------------------------------
		-- Create table for list of selected OUT Commercial Trunks from the 
		-- parameter passed
		-----------------------------------------------------------------
		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempOUTCommercialTrunkIDTable') )
				Drop table #TempOUTCommercialTrunkIDTable

		Create table #TempOUTCommercialTrunkIDTable (CommercialTrunkID varchar(100) )


		insert into #TempOUTCommercialTrunkIDTable
		select * from FN_ParseValueList ( @OUTCommercialTrunkIDList )

		----------------------------------------------------------------
		-- Check to ensure that none of the values are non numeric
		----------------------------------------------------------------

		if exists ( select 1 from #TempOUTCommercialTrunkIDTable where ISNUMERIC(CommercialTrunkID) = 0 )
		Begin

			set @ErrorDescription = 'ERROR !!! List of OUT CommercialTrunk IDs passed contain a non numeric value'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

		------------------------------------------------------
		-- Check if the All the CommercialTrunks have been selected 
		------------------------------------------------------

		if exists (
						select 1 
						from #TempOUTCommercialTrunkIDTable 
						where CommercialTrunkID = 0
				  )
		Begin

                  set @OUTAllCommercialTrunkFlag = 1
				  GOTO GENERATEREPORT
				  
		End
		
        -----------------------------------------------------------------
		-- Check to ensure that all the CommercialTrunk IDs passed are valid values
		-----------------------------------------------------------------
		
		if exists ( 
						select 1 
						from #TempOUTCommercialTrunkIDTable 
						where CommercialTrunkID not in
						(
							Select TrunkID
							from ReferenceServer.UC_Reference.dbo.tb_Trunk
							where trunktypeID = 9 -- Commercial trunk
							and flag & 1 <> 1
						)
					)
		Begin

			set @ErrorDescription = 'ERROR !!! List of OUT CommercialTrunk IDs passed contain value(s) which are not valid or do not exist'
			set @ResultFlag = 1
			GOTO ENDPROCESS

		End

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!! While processing the filter data for report'
		set @ResultFlag = 1
		Return 1

End Catch


GENERATEREPORT:


---------------------------------------------------------------------------------------------------------------------------------------------------
-- ***************************************** SECTION TO FIND RATE, REVENUE & COST FOR EACH ROUTE ***************************************** 
---------------------------------------------------------------------------------------------------------------------------------------------------


Begin Try

		-- Get all the IN CROSS OUT data for the specified dates
		-- NOTE: We need to take OUT Destination also in the list, as it will be used to calculate the correct CPM in scenarios
		-- where inbound traffic from multiple partners is being terminated on single partner on the outbound side.
		-- OUT Destination will provide us the exact distribution of vendor destinations across routing destinations for each Route

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempMarginReport') )
				Drop table #TempMarginReport

		select INAccountID , INCommercialTrunkID ,OUTAccountID , OutCommercialTrunkID, RoutingDestinationID , CalltypeID ,INServiceLevelID,
			   OutDestinationID,
			   sum(Answered) as Answered, 
			   sum(Seized) as Seized,
			   convert(Decimal(19,4) ,sum(CallDuration/60.0)) as CallDuration,
			   convert(Decimal(19,4) ,sum(INChargeDuration)) as INChargeDuration,
			   convert(Decimal(19,4) ,sum(OUTChargeDuration)) as OUTChargeDuration
		into #TempMarginReport
		from tb_MonthlyINCrossOutTraffic tbl1
		where CallMonth between @StartMonth and @EndMonth
		and CallTypeID = isnull(@CallTypeID , CalltypeID)
		group by INAccountID , INCommercialTrunkID ,OUTAccountID , OutCommercialTrunkID, RoutingDestinationID , CalltypeID ,INServiceLevelID,
		         OutDestinationID

		--select *
		--from #TempMarginReport

		-- Add Columns for RPM, CPM, Revenue, Cost to the Report table
		Alter table #TempMarginReport add RPM Decimal(19,6)
		Alter table #TempMarginReport add CPM Decimal(19,6)
		Alter table #TempMarginReport add Revenue Decimal(19,4)
		Alter table #TempMarginReport add Cost Decimal(19,4)


		-- Get all the Revenue for each Routing Destination

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRevenue') )
				Drop table #TempRevenue

		select AccountID , CommercialTrunkId , CalltypeID ,RoutingDestinationID ,INServiceLevelID,
			   convert(Decimal(19,4) ,sum(CallDuration/60.0)) as CallDuration ,
			   convert(Decimal(19,4) ,sum(RoundedCallDuration/60.0)) as RoundedCallDuration,
			   Case
					When convert(Decimal(19,4) ,sum(RoundedCallDuration/60.0)) = 0 then 0
					Else convert(Decimal(19,6),convert(Decimal(19,4) ,sum(Amount))/convert(Decimal(19,4) ,sum(RoundedCallDuration/60.0)))
			   End  as Rate,
			   sum(Answered) as Answered ,
			   sum(Seized) as Seized ,
			   convert(Decimal(19,2) ,sum(Amount)) as Amount			   
		into #TempRevenue
		from tb_MonthlyINUnionOutFinancial
		where CallMonth between @StartMonth and @EndMonth
		and DirectionID = 1 -- Inbound
		and CallTypeID = isnull(@CallTypeID , CalltypeID)
		group by AccountID , CommercialTrunkId , CalltypeID ,RoutingDestinationID ,INServiceLevelID 


		-- Update the Revenue Rate for each INAccount and Routing destination

		update tbl1
		set RPM = tbl2.Rate,
			Revenue = convert(Decimal(19,4) ,tbl2.Rate * tbl1.INChargeDuration)
		from #TempMarginReport tbl1
		inner join #TempRevenue tbl2 on 									   
		                                tbl1.INAccountID = tbl2.AccountID 
									   and 
										tbl1.RoutingDestinationID = tbl2.RoutingDestinationID
									   and
										tbl1.INServiceLevelID = tbl2.INServiceLevelID
									   and
									    tbl1.INCommercialTrunkID = tbl2.CommercialTrunkID
									   and
									    tbl1.CallTypeID = tbl2.CallTypeID


		-- Get all the Cost for each Routing Destination

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCost') )
				Drop table #TempCost

		--NOTE: Need to use the Settlement Destination along with ROUTE Destination to calculate Cost and Rate. This will
		-- tell us how the Route Destination is distributed across different vendor destinations when terminating traffic on a partner

		select AccountID , CommercialTrunkID , CallTypeID ,RoutingDestinationID , INServiceLevelID, SettlementDestinationID, 
			   convert(Decimal(19,4) ,sum(CallDuration/60.0)) as CallDuration ,
			   convert(Decimal(19,4) ,sum(RoundedCallDuration/60.0)) as RoundedCallDuration,
			   Case
					When convert(Decimal(19,4) ,sum(RoundedCallDuration/60.0)) = 0 then 0
					Else convert(Decimal(19,6),convert(Decimal(19,4) ,sum(Amount))/convert(Decimal(19,4) ,sum(RoundedCallDuration/60.0)))
			   End  as Rate,
			   sum(Answered) as Answered ,
			   sum(Seized) as Seized ,
			   convert(Decimal(19,2) ,sum(Amount)) as Amount			   
		into #TempCost
		from tb_MonthlyINUnionOutFinancial
		where CallMonth between @StartMonth and @EndMonth
		and DirectionID = 2 -- Outbound
		and CallTypeID = isnull(@CallTypeID , CalltypeID)
		group by AccountID , CommercialTrunkID , CallTypeID ,RoutingDestinationID , INServiceLevelID, SettlementDestinationID


		-- Update the Cost Rate for each OUtAccount and Routing destination

		update tbl1
		set CPM = tbl2.Rate,
			Cost = convert(Decimal(19,4) ,tbl2.Rate * tbl1.OUTChargeDuration)
		from #TempMarginReport tbl1
		inner join #TempCost tbl2 on
										tbl1.OUTAccountID = tbl2.AccountID 
									   and 
										tbl1.RoutingDestinationID = tbl2.RoutingDestinationID
									   and
										tbl1.INServiceLevelID = tbl2.INServiceLevelID
                                       and
									    tbl1.OUTCOmmercialTrunkID = tbl2.CommercialTrunkID
                                       and
									    tbl1.CalltypeID = tbl2.CallTypeID
									  and
									    tbl1.OUTDestinationID = tbl2.SettlementDestinationID

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!!! While Extracting data related to Rate and Rating Method for Margin Report. ' + ERROR_MESSAGE()
		RaisError('%s' , 16,1 ,@ErrorDescription)
		GOTO ENDPROCESS

End Catch

--------------------------------------------------------------------
-- Once we have calculated the Cost for each combination of Routing
-- and Out Destination, we need to summarize the data back again
-- removing the OUT Destiantion
--------------------------------------------------------------------

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempMarginReportSumm') )
		Drop table #TempMarginReportSumm

select Summ.INAccountID ,
		Summ.INCommercialTrunkID, 
		Summ.OUTAccountID,
		Summ.OUTCommercialTrunkID, 
		Summ.RoutingDestinationID,
        Summ.CallTypeID,
        Summ.INServiceLevelID,
		sum(Seized) as Seized, sum(Answered) as Answered,
		sum(CallDuration) as CallDuration,
		sum(INChargeDuration) as INChargeDuration, 
		sum(OUTChargeDuration) as OUTChargeDuration,
		Case 
			When sum(INChargeDuration) = 0 Then 0
			Else convert(Decimal(19,6) ,sum(Revenue)/sum(INChargeDuration))
		End as RPM,
		Case 
			When sum(OUTChargeDuration) = 0 Then 0
			Else convert(Decimal(19,6) ,sum(Cost)/sum(OUTChargeDuration))
		End as CPM,
		sum(Revenue) as Revenue,
		sum(Cost) as Cost
into #TempMarginReportSumm
from #TempMarginReport Summ
where INAccountID not in (59,110,119) -- Eliminating Records for all the test accounts created in the system
group by Summ.INAccountID ,
		Summ.INCommercialTrunkID, 
		Summ.OUTAccountID,
		Summ.OUTCommercialTrunkID, 
		Summ.RoutingDestinationID,
        Summ.CallTypeID,
        Summ.INServiceLevelID

-- Add Columns Profit Per Minute and Margin to the Report summarization table
Alter table #TempMarginReportSumm add PPM Decimal(19,6)
Alter table #TempMarginReportSumm add Margin Decimal(19,4)

-----------------------------------------------------------------------------------------------------------------------------------------
-- ***************************************** SECTION TO FIND MARGIN AND PROFIT PER MINUTE (PPM) ***************************************** 
-----------------------------------------------------------------------------------------------------------------------------------------

Update tbl1
set Margin = isnull(tbl1.Revenue , 0) - isnull(tbl1.Cost , 0),
	PPM = Case
	         When isnull(tbl1.Revenue , 0) - isnull(tbl1.Cost , 0) = 0 Then 0
			 When (isnull(tbl1.Revenue , 0) - isnull(tbl1.Cost , 0)) < 0 Then
					Case
						When tbl1.OUTChargeDuration = 0 Then 0
						Else convert(Decimal(19,4) ,(isnull(tbl1.Revenue , 0) - isnull(tbl1.Cost , 0))/tbl1.OUTChargeDuration)
					End
			 Else 
				Case
					When tbl1.INChargeDuration = 0 Then 0 
					Else convert(Decimal(19,4) ,(isnull(tbl1.Revenue , 0) - isnull(tbl1.Cost , 0))/tbl1.INChargeDuration)
				End			 
			 
		  End
from #TempMarginReportSumm tbl1

-----------------------------------------------------------------------------------------------------------------------
-- ***************************************** SECTION TO BUILD THE REPORT DATA ***************************************** 
-----------------------------------------------------------------------------------------------------------------------

Begin Try
		-- Extract the final Result

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempFinalMarginReport') )
				Drop table #TempFinalMarginReport

		select INAcc.Account as INAccount , Summ.INAccountID ,
		       isnull(INTrunk.Trunk, '***') as INCommercialTrunk , Summ.INCommercialTrunkID, -- Added ISNULL criteria for IN Commercial Trunk
		       OutAcc.Account as OUTAccount, Summ.OUTAccountID,
		       isnull(OUTTrunk.Trunk, '***') as OUTCommercialTrunk , Summ.OUTCommercialTrunkID, -- Added ISNULL criteria for OUT COmmercial Trunk
			   Cou.Country , Cou.CountryID,
			   Dest.Destination, Summ.RoutingDestinationID as DestinationID,
			   CTyp.Calltype , Summ.CallTypeID,
			   Slvl.ServiceLevel,Summ.INServiceLevelID as ServiceLevelID,
			   isnull(DestGrp.EntityGroupID,Summ.RoutingDestinationID)  as DestinationGroupID,
			   Seized, Answered,
			   convert(int ,round((Answered*100.0)/Seized,0)) as ASR,
			   CallDuration as Minutes,
			   Case
					When Answered = 0 then 0
					Else convert(Decimal(19,2) ,CallDuration/Answered)
			   End as ALOC,
			   INChargeDuration, 
			   OUTChargeDuration,
			   RPM, CPM, PPM,
			   Revenue,
			   Cost, 
			   Margin
		into #TempFinalMarginReport
		from #TempMarginReportSumm Summ
		left join Referenceserver.UC_Reference.dbo.tb_Account INAcc on Summ.INAccountID = INAcc.AccountID
		left join Referenceserver.UC_Reference.dbo.tb_Account OutAcc on Summ.OUTAccountID = OutAcc.AccountID
		left join Referenceserver.UC_Reference.dbo.tb_Trunk INTrunk on Summ.INCommercialTrunkID = INTrunk.TrunkID
		left join Referenceserver.UC_Reference.dbo.tb_Trunk OUTTrunk on Summ.OUTCommercialTrunkID = OUTTrunk.TrunkID
		inner join Referenceserver.UC_Reference.dbo.tb_Calltype CTyp on Summ.CallTypeID = CTyp.CalltypeID
		inner join Referenceserver.UC_Reference.dbo.tb_Destination Dest on Summ.RoutingDestinationID = Dest.DestinationID
		inner join Referenceserver.UC_Reference.dbo.tb_Country Cou on Dest.CountryID = Cou.CountryID
		inner join Referenceserver.UC_Reference.dbo.tb_ServiceLevel Slvl on Summ.INServiceLevelID = Slvl.SErviceLevelID
		left join
		(
			select tbl1.EntityGroupID , tbl1.EntityGroup, tbl2.InstanceID
			from Referenceserver.UC_Reference.dbo.tb_EntityGroup tbl1
			inner join Referenceserver.UC_Reference.dbo.tb_EntityGroupMember tbl2 on tbl1.EntityGroupID = tbl2.EntityGroupID
			inner join Referenceserver.UC_Reference.dbo.tb_Destination tbl3 on tbl2.InstanceID = tbl3.DestinationID
			where EntityGroupTypeID = -2
			and tbl3.NumberPlanID = -1 -- Only Routing number plan grouping
			and tbl1.Flag & 1 <> 1
		) DestGrp on summ.RoutingDestinationID = DestGrp.InstanceID
		--where Summ.CallDuration > 0 -- Dont want records where no Call Duration is there


End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!!! When extracting data for margin report. ' + ERROR_MESSAGE()
		RaisError('%s' , 16,1 ,@ErrorDescription)
		GOTO ENDPROCESS

End Catch

--Select 'Debug....'

--Select *
--from #TempFinalMarginReport

--select getdate()

-------------------------------------------------------------------------------------------------------------------------
-- ***************************************** SECTION TO DYNAMICALLY FILTER DATA ***************************************** 
-------------------------------------------------------------------------------------------------------------------------

Begin Try

		set @SQLStr1 = 'Select tbl1.INAccount , tbl1.INCommercialTrunk, tbl1.OUTAccount , tbl1.OUTCommercialTrunk ,tbl1.Country ,' + char(10)+
		               'tbl1.Destination , tbl1.Calltype, tbl1.ServiceLevel, '+ char(10)+
		               'tbl1.Answered , tbl1.Seized, tbl1.ASR, convert(Decimal(19,2), tbl1.Minutes) as Minutes, tbl1.ALOC, '+ char(10)+
					   'convert(Decimal(19,2) ,tbl1.INChargeDuration) as INchargeDuration, convert(Decimal(19,2) ,tbl1.OUTChargeDuration) as OutChargeDuration, ' + char(10) +
					   'tbl1.RPM, tbl1.CPM, tbl1.PPM, '+ char(10)+
					   'convert(Decimal(19,2) ,tbl1.Revenue) as Revenue, convert(Decimal(19,2),tbl1.Cost) as Cost, convert(Decimal(19,2) ,tbl1.Margin) as Margin' + char(10)

		set @SQLStr2 = 'from #TempFinalMarginReport tbl1 '+ char(10)+
		              Case
							When @INAllAccountFlag = 1 then ''
							Else 'inner join #TempINAccountIDTable tbl2 on tbl1.INAccountID = tbl2.AccountID ' + char(10)
					  End +
		              Case
							When @OUTAllAccountFlag = 1 then ''
							Else 'inner join #TempOUTAccountIDTable tbl3 on tbl1.OutAccountID = tbl3.AccountID ' + char(10)
					  End +
		              Case
							When @AllCountryFlag = 1 then ''
							Else 'inner join #TempCountryIDTable tbl4 on tbl1.CountryID = tbl4.CountryID ' + char(10)
					  End +
		              Case
							When @AllDestinationFlag = 1 then ''
							Else 'inner join #TempDestinationIDTable tbl5 on tbl1.DestinationID = tbl5.DestinationID ' + char(10)
					  End +
		              Case
							When @AllServiceLevelFlag = 1 then ''
							Else 'inner join #TempServiceLevelIDTable tbl6 on tbl1.ServiceLevelID = tbl6.ServiceLevelID ' + char(10)
					  End +
		              Case
							When @AllDestinationGroupFlag = 1 then ''
							Else 'inner join #TempDestinationGroupIDTable tbl7 on tbl1.DestinationGroupID = tbl7.DestinationGroupID ' + char(10)
					  End


			set @SQLStr3 = 'Order by tbl1.INAccount , tbl1.OUTAccount , tbl1.Country , tbl1.Destination , tbl1.ServiceLevel'	
			
			set  @SQLStr = @SQLStr1 + @SQLStr2 + @SQLStr3

			--print(@SQLStr)

			Exec(@SQLStr)

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!!! While building dynamic query to filter margin data . ' + ERROR_MESSAGE()
		RaisError('%s' , 16,1 ,@ErrorDescription)
		GOTO ENDPROCESS

End Catch

-------------------------------------------------------------------------------------------------------------------------
-- ***************************************** SECTION TO CALCULATE THE TOTAL ***************************************** 
-------------------------------------------------------------------------------------------------------------------------

Begin Try

		if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#tempTotalResult') )
				Drop table #tempTotalResult

       Create table #tempTotalResult
	   (
			Param1 varchar(100),
			Param2 varchar(100),
			Param3 varchar(100),
			Param4 varchar(100),
			Param5 varchar(100),
			Param6 varchar(100),
			Param7 varchar(100),
			Param8 varchar(100),
			Answered int,
			Seized int,
			ASR int,
			OriginalMinutes Decimal(19,2),
			ALOC Decimal(19,2),
            INChargeMinutes Decimal(19,2),
			OUTChargeMinutes Decimal(19,2),
			RPM Decimal(19,6),
			CPM Decimal(19,6),
			PPM Decimal(19,4),
			Revenue Decimal(19,2),
			Cost Decimal(19,2),
			Margin Decimal(19,2)
	   )

		set @SQLStr1 = 'Select ''Total'' ,'''' , '''' , '''' , '''' ,'''' , '''' , '''',sum(Answered), sum(Seized), ' + char(10) +
					   'convert(int ,round((sum(Answered)*100.0)/sum(Seized),0)), ' + char(10) + -- ASR
					   'sum(Minutes) , ' + char(10) + -- CallDuration
					   'Case When sum(Answered) = 0 then 0 Else convert(Decimal(19,2) ,sum(Minutes)/sum(Answered)) End, ' + char(10) + -- ALOC
					   'sum(convert(Decimal(19,2) ,INChargeDuration)) , sum(convert(Decimal(19,2),OUTChargeDuration)), ' + char(10) + -- IN and OUT Rounded Call Duration
					   'Case ' + char(10) +
					   '		When convert(Decimal(19,4) ,sum(INChargeDuration)) = 0 then 0 ' + char(10) +
					   '        Else convert(Decimal(19,6),convert(Decimal(19,4) ,sum(Revenue))/convert(Decimal(19,4) ,sum(INChargeDuration))) ' + char(10)+
			           'End ,' + char(10) + -- RPM
					   'Case ' + char(10) +
					   '		When convert(Decimal(19,4) ,sum(OUTChargeDuration)) = 0 then 0 ' + char(10) +
					   '        Else convert(Decimal(19,6),convert(Decimal(19,4) ,sum(Cost))/convert(Decimal(19,4) ,sum(OUTChargeDuration))) ' + char(10)+
			           'End ,' + char(10) + -- CPM
					   'Case ' + char(10) +
					   '		When convert(Decimal(19,4) ,sum(Minutes)) = 0 then 0 ' + char(10) +
					   '        Else convert(Decimal(19,6),convert(Decimal(19,4) ,sum(Margin))/convert(Decimal(19,4) ,sum(Minutes))) ' + char(10)+
			           'End ,' + char(10) + -- PPM
					   'sum(convert(Decimal(19,2),Revenue)) , sum(convert(Decimal(19,2) ,Cost)) , Sum(convert(DEcimal(19,2) ,Margin))' + char(10)

		set  @SQLStr = @SQLStr1 + @SQLStr2
		--print(@SQLStr)

		Insert into #tempTotalResult
		Exec(@SQLStr)

		select 	@TotalResult = 
				Param1 + '|' + Param2 + '|' + Param3 + '|' + Param4 + '|' + Param5 + '|' +
				Param6 + '|' + Param7 + '|' + Param8 + '|' +
				convert(varchar(100), isnull(Answered, 0)) + '|'+
				convert(varchar(100) ,isnull(Seized,0)) + '|' +
				convert(varchar(100) ,isnull(ASR,0)) + '|' +
				convert(varchar(100) ,isnull(OriginalMinutes,0)) + '|' +
				convert(varchar(100) ,isnull(ALOC,0)) + '|' +
				convert(varchar(100), isnull(INChargeMinutes,0)) + '|' +
				convert(varchar(100), isnull(OUTChargeMinutes,0)) + '|' +
				convert(varchar(100) ,isnull(RPM,0)) + '|' +
				convert(varchar(100) ,isnull(CPM,0)) + '|' +
				convert(varchar(100) ,isnull(PPM,0)) + '|' +
				convert(varchar(100) ,isnull(Revenue,0)) + '|' +
				convert(varchar(100) ,isnull(Cost,0)) + '|' +
				convert(varchar(100) ,isnull(Margin,0))
		from #tempTotalResult

End Try

Begin Catch

		set @ErrorDescription = 'ERROR !!!! While Calculating the Total for Margin Report. ' + ERROR_MESSAGE()
		RaisError('%s' , 16,1 ,@ErrorDescription)
		GOTO ENDPROCESS

End Catch

ENDPROCESS:

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempMarginReport') )
		Drop table #TempMarginReport

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempRevenue') )
		Drop table #TempRevenue

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCost') )
		Drop table #TempCost

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempFinalMarginReport') )
		Drop table #TempFinalMarginReport

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#tempTotalResult') )
		Drop table #tempTotalResult

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempINAccountIDTable') )
		Drop table #TempINAccountIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempOUTAccountIDTable') )
		Drop table #TempOUTAccountIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempINCommercialTrunkIDTable') )
		Drop table #TempINCommercialTrunkIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempOUTCommercialTrunkIDTable') )
		Drop table #TempOUTCommercialTrunkIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempCountryIDTable') )
		Drop table #TempCountryIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempServiceLevelIDTable') )
		Drop table #TempServiceLevelIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationIDTable') )
		Drop table #TempDestinationIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempDestinationGroupIDTable') )
		Drop table #TempDestinationGroupIDTable

if exists (select 1 from tempdb.dbo.sysobjects where xtype = 'U' and id = object_id(N'tempdb..#TempMarginReportSumm') )
		Drop table #TempMarginReportSumm

Return 0
GO

