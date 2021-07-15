USE [UC_Reference]
GO

/****** Object:  StoredProcedure [dbo].[SP_UIGetCommitmentMinutesAndAmountForCycle]    Script Date: 7/13/2021 11:32:45 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   procedure [dbo].[SP_UIGetCommitmentMinutesAndAmountForCycle]
(
	@AggregationCycleID int,
	@CommittedMinutes Decimal(19,4) Output,
	@CommittedAmount Decimal(19,4) Output,
	@ErrorDescription varchar(2000) output,
	@ResultFlag int output
)
As

set @ErrorDescription = NULL
set @ResultFlag = 0

Declare @AggregationTypeID int,
        @Commitment Decimal(19,4)

----------------------------------------------------------------
-- Check to ensure that the aggregation cycle Id passed is valid
----------------------------------------------------------------
if not exists (select 1 from tb_AggregationCycle where AggregationCycleID = @AggregationCycleID)
Begin

		set @ErrorDescription = 'ERROR !!! The Aggregation Cycle ID is not valid or does not exist in the system.'
		set @ResultFlag = 1

		return 1

End

----------------------------------------------------------------
-- Get the Aggregation Type fpr the cycle and use the info to
-- get details related to commitment minutes and amount
----------------------------------------------------------------
Select @AggregationTypeID = AggregationTypeID,
       @Commitment = Commitment
from Tb_AggregationCycle
where AggregationCycleID = @AggregationCycleID

------------------------------------------------------------
-- If the Aggregation is of the type "Amount Commitment"
-- then we need to find the Minutes based on the Amount 
-- and Rate Structure
------------------------------------------------------------
if (@AggregationTypeID in (-2,-4,-7,-9 ))
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
							return 1
					End

		End Try

		Begin Catch

					set @ErrorDescription = 'ERROR !!!! When extracting Committed Minutes for Amount based Aggregation. ' + ERROR_MESSAGE()
					set @ResultFlag = 1
					return 1

		End Catch

		set @CommittedAmount = @Commitment

End

------------------------------------------------------------
-- If the Aggregation is of the type "Volume Commitment"
-- then we need to find the Amount based on the Minutes 
-- and Rate Structure
------------------------------------------------------------
if (@AggregationTypeID in ( -1,-3,-6 ,-8) )
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
							return 1
					End

		End Try

		Begin Catch

					set @ErrorDescription = 'ERROR !!!! When extracting Committed Amount for Minutes based Aggregation. ' + ERROR_MESSAGE()
					set @ResultFlag = 1
					return 1

		End Catch

		set @CommittedMinutes = @Commitment

End

------------------------------------------------------
-- If the Aggregation type is No Commitment, then set
-- the Committed Minutes and Amount to 0
------------------------------------------------------
if (@AggregationTypeID = -5)
Begin

	set @CommittedAmount = 0
	set @CommittedMinutes = 0

End
GO


