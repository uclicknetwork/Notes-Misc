USE [UC_Reference]
GO
/****** Object:  View [dbo].[vw_AggregateSummary]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE             View [dbo].[vw_AggregateSummary]
As

Select tbl6.AccountID,
       tbl6.AccountAbbrv as Account,
	   tbl4.AggregationID,
       tbl4.AggregationName,
	   tbl4.DirectionID,	
       Case
			When tbl4.DirectionID = 1 then 'Inbound'
			When tbl4.DirectionID = 2 then 'Outbound'
	   End as Direction,
	   tbl1.AggregationCycleID,
	   tbl2.AggregationCycle,
	   tbl3.CommitmentType as CommitmentTypeVal,
	   Case
			When tbl3.CommitmentType = 0 then 'None'
			When tbl3.CommitmentType = 1 then 'Volume'
			When tbl3.CommitmentType = 2 then 'Amount'
	   End as CommitmentType,
	   tbl3.GracePeriod as GracePeriodVal,
	   Case
			When tbl3.GracePeriod = 0 then 'No'
			When tbl3.GracePeriod = 1 then 'Yes'
	   End as GracePeriod,
	   tbl3.Penalty as PenaltyVal,
	   Case
			When tbl3.Penalty = 0 then 'No'
			When tbl3.Penalty = 1 then 'Yes'
	   End as Penalty,
	   tbl1.IsAggregationActive as AggregationStatusFlag,
	   Case
			When tbl1.IsAggregationActive = 0 then 'Expired'
			When tbl1.IsAggregationActive = 1 then 'Active'
	   End as AggregationStatus,
	   tbl7.AggregationCriteriaID,
	   tbl7.AggregationCriteria,
	   tbl1.CycleStartDate,
	   tbl1.CycleEndDate,
	   tbl1.GracePeriodStartDate,
	   tbl1.GracePeriodEndDate,
	   convert(Decimal(19,2),tbl1.CommittedMinutes) as CommittedMinutes,
	   convert(Decimal(19,2),tbl1.CommittedAmount) as CommittedAmount,
	   tbl1.AggregationStartDate,
	   tbl1.AggregationEndDate,
	   convert(Decimal(19,2),tbl1.TrafficAggregatedInCycle) as TrafficAggregatedInCycle,
	   convert(Decimal(19,2),tbl1.TrafficAggregatedInGracePeriod) as TrafficAggregatedInGracePeriod,
	   convert(Decimal(19,2),tbl1.TotalTrafficAggregated) as TotalTrafficAggregated,
	   convert(Decimal(19,2),tbl1.TotalAmountAggregated) as TotalAmountAggregated,
	   convert(Decimal(19,2),tbl1.TrafficAggregatedOverCommitment) as TrafficAggregatedOverCommitment ,
	   tbl1.TrafficAggregatedInPercent,
	   convert(Decimal(19,2),tbl1.ShortfallMinutes ) as ShortfallMinutes,
	   convert(Decimal(19,2),tbl1.PenaltyAmount ) as PenaltyAmount,
	   tbl1.AggregationBlendedRate,
	   tbl1.AggregationBlendedRateAfterPenalty,
	   tbl1.CycleStartDate as ActualCycleStartDate,
	   Case
			When tbl1.GracePeriodEndDate is NULL then tbl1.CycleEndDate
			Else tbl1.GracePeriodEndDate
	   End as ActualCycleEndDate
from ReferenceServer.Uc_Reference.dbo.tb_AggregateSummary tbl1
inner join ReferenceServer.Uc_Reference.dbo.tb_AggregationCycle tbl2 on tbl1.AggregationCycleID = tbl2.AggregationCycleID
inner join ReferenceServer.Uc_Reference.dbo.Tb_AggregationType tbl3 on tbl2.AggregationtypeID = tbl3.AggregationTypeID
inner join ReferenceServer.Uc_Reference.dbo.Tb_Aggregation tbl4 on tbl2.AggregationID = tbl4.AggregationID
inner join ReferenceServer.Uc_Reference.dbo.tb_Agreement tbl5 on tbl4.AgreementID = tbl5.AgreementID
inner join ReferenceServer.Uc_Reference.dbo.tb_Account tbl6 on tbl5.AccountID = tbl6.AccountID
inner join ReferenceServer.Uc_Reference.dbo.tb_AggregationCriteria tbl7 on tbl2.AggregationCriteriaID = tbl7.AggregationCriteriaID
GO
/****** Object:  Table [dbo].[tb_AggregateSummary]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tb_AggregateSummary](
	[AggregationCycleID] [int] NULL,
	[CycleStartDate] [date] NULL,
	[CycleEndDate] [date] NULL,
	[GracePeriodStartDate] [date] NULL,
	[GracePeriodEndDate] [date] NULL,
	[CommittedMinutes] [decimal](19, 4) NULL,
	[CommittedAmount] [decimal](19, 4) NULL,
	[IsAggregationActive] [int] NULL,
	[AggregationStartDate] [date] NULL,
	[AggregationEndDate] [date] NULL,
	[TrafficAggregatedInCycle] [decimal](19, 4) NULL,
	[TrafficAggregatedInGracePeriod] [decimal](19, 4) NULL,
	[TotalTrafficAggregated] [decimal](19, 4) NULL,
	[TotalAmountAggregated] [decimal](19, 4) NULL,
	[TrafficAggregatedOverCommitment] [decimal](19, 4) NULL,
	[TrafficAggregatedInPercent] [decimal](19, 2) NULL,
	[ShortfallMinutes] [decimal](19, 4) NULL,
	[PenaltyAmount] [decimal](19, 4) NULL,
	[AggregationBlendedRate] [decimal](19, 6) NULL,
	[AggregationBlendedRateAfterPenalty] [decimal](19, 6) NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tb_AggregateSummaryRateDetails]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tb_AggregateSummaryRateDetails](
	[AggregationCycleID] [int] NOT NULL,
	[CallDate] [date] NOT NULL,
	[TierID] [int] NOT NULL,
	[AggregatedMinutes] [decimal](19, 4) NOT NULL,
	[RatedAmount] [decimal](19, 4) NOT NULL,
	[Rate] [decimal](19, 6) NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tb_AggregateSummaryRateTierAllocation]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tb_AggregateSummaryRateTierAllocation](
	[AggregationCycleID] [int] NOT NULL,
	[StartDate] [date] NOT NULL,
	[EndDate] [date] NOT NULL,
	[TierID] [int] NOT NULL,
	[AggregatedMinutes] [decimal](19, 4) NOT NULL,
	[RatedAmount] [decimal](19, 4) NOT NULL,
	[Rate] [decimal](19, 6) NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_AggregateSummaryTraffic]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_AggregateSummaryTraffic](
	[AggregationCycleID] [int] NOT NULL,
	[CallDate] [date] NOT NULL,
	[CommercialTrunkID] [int] NOT NULL,
	[ServiceLevelID] [int] NOT NULL,
	[DestinationID] [int] NOT NULL,
	[CallTypeID] [int] NOT NULL,
	[Minutes] [decimal](19, 4) NOT NULL,
	[Rate] [decimal](19, 6) NOT NULL,
	[Amount] [decimal](19, 4) NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
 CONSTRAINT [UC_Tb_AggregateSummaryTraffic] UNIQUE NONCLUSTERED 
(
	[CallDate] ASC,
	[CommercialTrunkID] ASC,
	[ServiceLevelID] ASC,
	[DestinationID] ASC,
	[CallTypeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_Aggregation]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_Aggregation](
	[AggregationID] [int] IDENTITY(1,1) NOT NULL,
	[AggregationName] [varchar](100) NOT NULL,
	[AggregationAbbrv] [varchar](50) NOT NULL,
	[AgreementID] [int] NOT NULL,
	[DirectionID] [int] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
	[Notes] [varchar](2000) NULL,
 CONSTRAINT [PK_Tb_Aggregation] PRIMARY KEY CLUSTERED 
(
	[AggregationID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_Tb_Aggregation] UNIQUE NONCLUSTERED 
(
	[AggregationName] ASC,
	[AgreementID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_AggregationCriteria]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_AggregationCriteria](
	[AggregationCriteriaID] [int] NOT NULL,
	[AggregationCriteria] [varchar](100) NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
 CONSTRAINT [PK_Tb_AggregationCriteria] PRIMARY KEY CLUSTERED 
(
	[AggregationCriteriaID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_Tb_AggregationCriteria] UNIQUE NONCLUSTERED 
(
	[AggregationCriteria] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_AggregationCycle]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_AggregationCycle](
	[AggregationCycleID] [int] IDENTITY(1,1) NOT NULL,
	[AggregationCycle] [varchar](100) NOT NULL,
	[AggregationID] [int] NOT NULL,
	[AggregationtypeID] [int] NOT NULL,
	[AggregationCriteriaID] [int] NOT NULL,
	[StartDate] [date] NOT NULL,
	[EndDate] [date] NOT NULL,
	[GracePeriodEndDate] [date] NULL,
	[Commitment] [decimal](19, 4) NULL,
	[CycleCreationDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
 CONSTRAINT [PK_Tb_AggregationCycle] PRIMARY KEY CLUSTERED 
(
	[AggregationCycleID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_Tb_AggregationCycle] UNIQUE NONCLUSTERED 
(
	[AggregationCycle] ASC,
	[AggregationID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_AggregationGrouping]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_AggregationGrouping](
	[AggregationGroupingID] [int] IDENTITY(1,1) NOT NULL,
	[AggregationID] [int] NOT NULL,
	[CommercialTrunkID] [int] NOT NULL,
	[ServiceLevelID] [int] NOT NULL,
	[DestinationID] [int] NOT NULL,
	[CallTypeID] [int] NOT NULL,
	[StartDate] [date] NOT NULL,
	[EndDate] [date] NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
 CONSTRAINT [PK_Tb_AggregationGrouping] PRIMARY KEY CLUSTERED 
(
	[AggregationGroupingID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_AggregationRate]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_AggregationRate](
	[AggregationRateID] [int] IDENTITY(1,1) NOT NULL,
	[AggregationCycleID] [int] NOT NULL,
	[CurrencyID] [int] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
 CONSTRAINT [PK_Tb_AggregationRate] PRIMARY KEY CLUSTERED 
(
	[AggregationRateID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_Tb_AggregationRate] UNIQUE NONCLUSTERED 
(
	[AggregationCycleID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_AggregationRateDetail]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_AggregationRateDetail](
	[AggregationRateDetailID] [int] IDENTITY(1,1) NOT NULL,
	[AggregationRateID] [int] NOT NULL,
	[TierID] [int] NOT NULL,
	[From] [int] NOT NULL,
	[To] [int] NULL,
	[ApplyFrom] [bit] NOT NULL,
	[Rate] [decimal](19, 6) NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
 CONSTRAINT [PK_Tb_AggregationRateDetail] PRIMARY KEY CLUSTERED 
(
	[AggregationRateDetailID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_Tb_AggregationRateDetail] UNIQUE NONCLUSTERED 
(
	[AggregationRateID] ASC,
	[TierID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_AggregationType]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_AggregationType](
	[AggregationTypeID] [int] NOT NULL,
	[AggregationType] [varchar](100) NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
	[CommitmentType] [int] NOT NULL,
	[GracePeriod] [int] NOT NULL,
	[Penalty] [int] NOT NULL,
 CONSTRAINT [PK_Tb_AggregationType] PRIMARY KEY CLUSTERED 
(
	[AggregationTypeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_Tb_AggregationType] UNIQUE NONCLUSTERED 
(
	[AggregationType] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tb_AggregationTypeAndCriteriaMapping]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tb_AggregationTypeAndCriteriaMapping](
	[AggregationTypeAndCriteriaMappingID] [int] IDENTITY(1,1) NOT NULL,
	[AggregationTypeID] [int] NOT NULL,
	[AggregationCriteriaID] [int] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
 CONSTRAINT [PK_tb_AggregationTypeAndCriteriaMapping] PRIMARY KEY CLUSTERED 
(
	[AggregationTypeAndCriteriaMappingID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UC_tb_AggregationTypeAndCriteriaMapping] UNIQUE NONCLUSTERED 
(
	[AggregationTypeID] ASC,
	[AggregationCriteriaID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Tb_AggregationtypePriority]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Tb_AggregationtypePriority](
	[AggregationtypePriorityID] [int] NOT NULL,
	[AggregationtypeID] [int] NOT NULL,
	[Priority] [int] NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
	[ModifiedByID] [int] NOT NULL,
 CONSTRAINT [PK_Tb_AggregationtypePriority] PRIMARY KEY CLUSTERED 
(
	[AggregationtypePriorityID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[wtb_AggregationGrouping_2976_20210710210601]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[wtb_AggregationGrouping_2976_20210710210601](
	[AggregationID] [int] NOT NULL,
	[RecordID] [int] NOT NULL,
	[CommercialTrunkID] [varchar](100) NULL,
	[CommercialTrunk] [varchar](60) NOT NULL,
	[ServiceLevelID] [varchar](100) NULL,
	[ServiceLevel] [varchar](60) NOT NULL,
	[CallTypeID] [varchar](100) NULL,
	[CallType] [varchar](60) NOT NULL,
	[DestinationID] [varchar](100) NULL,
	[Destination] [varchar](60) NOT NULL,
	[StartDate] [date] NULL,
	[EndDate] [date] NULL,
	[ErrorDescription] [varchar](200) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[wtb_AggregationGrouping_3809_20210709032808]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[wtb_AggregationGrouping_3809_20210709032808](
	[AggregationID] [int] NOT NULL,
	[RecordID] [int] NOT NULL,
	[CommercialTrunkID] [varchar](100) NULL,
	[CommercialTrunk] [varchar](60) NOT NULL,
	[ServiceLevelID] [varchar](100) NULL,
	[ServiceLevel] [varchar](60) NOT NULL,
	[CallTypeID] [varchar](100) NULL,
	[CallType] [varchar](60) NOT NULL,
	[DestinationID] [varchar](100) NULL,
	[Destination] [varchar](60) NOT NULL,
	[StartDate] [date] NULL,
	[EndDate] [date] NULL,
	[ErrorDescription] [varchar](200) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[wtb_AggregationGrouping_7692_20210715124615]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[wtb_AggregationGrouping_7692_20210715124615](
	[AggregationID] [int] NOT NULL,
	[RecordID] [int] NOT NULL,
	[CommercialTrunkID] [varchar](100) NULL,
	[CommercialTrunk] [varchar](60) NOT NULL,
	[ServiceLevelID] [varchar](100) NULL,
	[ServiceLevel] [varchar](60) NOT NULL,
	[CallTypeID] [varchar](100) NULL,
	[CallType] [varchar](60) NOT NULL,
	[DestinationID] [varchar](100) NULL,
	[Destination] [varchar](60) NOT NULL,
	[StartDate] [date] NULL,
	[EndDate] [date] NULL,
	[ErrorDescription] [varchar](200) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[wtb_AggregationGrouping_8410_20210709201118]    Script Date: 7/15/2021 3:01:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[wtb_AggregationGrouping_8410_20210709201118](
	[AggregationID] [int] NOT NULL,
	[RecordID] [int] NOT NULL,
	[CommercialTrunkID] [varchar](100) NULL,
	[CommercialTrunk] [varchar](60) NOT NULL,
	[ServiceLevelID] [varchar](100) NULL,
	[ServiceLevel] [varchar](60) NOT NULL,
	[CallTypeID] [varchar](100) NULL,
	[CallType] [varchar](60) NOT NULL,
	[DestinationID] [varchar](100) NULL,
	[Destination] [varchar](60) NOT NULL,
	[StartDate] [date] NULL,
	[EndDate] [date] NULL,
	[ErrorDescription] [varchar](200) NULL
) ON [PRIMARY]
GO
/****** Object:  Index [idx_tb_AggregateSummary_AggregationCycle]    Script Date: 7/15/2021 3:01:29 PM ******/
CREATE NONCLUSTERED INDEX [idx_tb_AggregateSummary_AggregationCycle] ON [dbo].[tb_AggregateSummary]
(
	[AggregationCycleID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [idx_tb_AggregateSummaryRateDetails_AggregationCycle]    Script Date: 7/15/2021 3:01:29 PM ******/
CREATE NONCLUSTERED INDEX [idx_tb_AggregateSummaryRateDetails_AggregationCycle] ON [dbo].[tb_AggregateSummaryRateDetails]
(
	[AggregationCycleID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [idx_tb_AggregateSummaryRateDetails_CallDate]    Script Date: 7/15/2021 3:01:29 PM ******/
CREATE NONCLUSTERED INDEX [idx_tb_AggregateSummaryRateDetails_CallDate] ON [dbo].[tb_AggregateSummaryRateDetails]
(
	[CallDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [idx_tb_AggregateSummaryRateTierAllocation_AggregationCycle]    Script Date: 7/15/2021 3:01:29 PM ******/
CREATE NONCLUSTERED INDEX [idx_tb_AggregateSummaryRateTierAllocation_AggregationCycle] ON [dbo].[tb_AggregateSummaryRateTierAllocation]
(
	[AggregationCycleID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [idx_Tb_AggregateSummaryTraffic_AggregationCycle]    Script Date: 7/15/2021 3:01:29 PM ******/
CREATE NONCLUSTERED INDEX [idx_Tb_AggregateSummaryTraffic_AggregationCycle] ON [dbo].[Tb_AggregateSummaryTraffic]
(
	[AggregationCycleID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [idx_tb_AggregationGrouping_Aggregation]    Script Date: 7/15/2021 3:01:29 PM ******/
CREATE NONCLUSTERED INDEX [idx_tb_AggregationGrouping_Aggregation] ON [dbo].[Tb_AggregationGrouping]
(
	[AggregationID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic] ADD  CONSTRAINT [DF_Tb_AggregateSummaryTraffic_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic] ADD  CONSTRAINT [DF_Tb_AggregateSummaryTraffic_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[Tb_Aggregation] ADD  CONSTRAINT [DF_Tb_Aggregation_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_Aggregation] ADD  CONSTRAINT [DF_Tb_Aggregation_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[Tb_AggregationCriteria] ADD  CONSTRAINT [DF_Tb_AggregationCriteria_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregationCriteria] ADD  CONSTRAINT [DF_Tb_AggregationCriteria_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[Tb_AggregationCycle] ADD  CONSTRAINT [DF_Tb_AggregationCycle_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregationCycle] ADD  CONSTRAINT [DF_Tb_AggregationCycle_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping] ADD  CONSTRAINT [DF_Tb_AggregationGrouping_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping] ADD  CONSTRAINT [DF_Tb_AggregationGrouping_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[Tb_AggregationRate] ADD  CONSTRAINT [DF_Tb_AggregationRate_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregationRate] ADD  CONSTRAINT [DF_Tb_AggregationRate_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[Tb_AggregationRateDetail] ADD  CONSTRAINT [DF_Tb_AggregationRateDetail_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregationRateDetail] ADD  CONSTRAINT [DF_Tb_AggregationRateDetail_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[Tb_AggregationType] ADD  CONSTRAINT [DF_Tb_AggregationType_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregationType] ADD  CONSTRAINT [DF_Tb_AggregationType_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[Tb_AggregationType] ADD  DEFAULT ((0)) FOR [CommitmentType]
GO
ALTER TABLE [dbo].[Tb_AggregationType] ADD  DEFAULT ((0)) FOR [GracePeriod]
GO
ALTER TABLE [dbo].[Tb_AggregationType] ADD  DEFAULT ((0)) FOR [Penalty]
GO
ALTER TABLE [dbo].[tb_AggregationTypeAndCriteriaMapping] ADD  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregationtypePriority] ADD  CONSTRAINT [DF_Tb_AggregationtypePriority_ModifiedDate]  DEFAULT (getdate()) FOR [ModifiedDate]
GO
ALTER TABLE [dbo].[Tb_AggregationtypePriority] ADD  CONSTRAINT [DF_Tb_AggregationtypePriority_ModifiedByID]  DEFAULT ((-1)) FOR [ModifiedByID]
GO
ALTER TABLE [dbo].[tb_AggregateSummary]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregateSummary_tb_AggregationCycle] FOREIGN KEY([AggregationCycleID])
REFERENCES [dbo].[Tb_AggregationCycle] ([AggregationCycleID])
GO
ALTER TABLE [dbo].[tb_AggregateSummary] CHECK CONSTRAINT [FK_Tb_AggregateSummary_tb_AggregationCycle]
GO
ALTER TABLE [dbo].[tb_AggregateSummaryRateDetails]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregateSummaryRateDetails_tb_AggregationCycle] FOREIGN KEY([AggregationCycleID])
REFERENCES [dbo].[Tb_AggregationCycle] ([AggregationCycleID])
GO
ALTER TABLE [dbo].[tb_AggregateSummaryRateDetails] CHECK CONSTRAINT [FK_Tb_AggregateSummaryRateDetails_tb_AggregationCycle]
GO
ALTER TABLE [dbo].[tb_AggregateSummaryRateTierAllocation]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregateSummaryRateTierAllocation_tb_AggregationCycle] FOREIGN KEY([AggregationCycleID])
REFERENCES [dbo].[Tb_AggregationCycle] ([AggregationCycleID])
GO
ALTER TABLE [dbo].[tb_AggregateSummaryRateTierAllocation] CHECK CONSTRAINT [FK_Tb_AggregateSummaryRateTierAllocation_tb_AggregationCycle]
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_AggregationCycle] FOREIGN KEY([AggregationCycleID])
REFERENCES [dbo].[Tb_AggregationCycle] ([AggregationCycleID])
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic] CHECK CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_AggregationCycle]
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_CallType] FOREIGN KEY([CallTypeID])
REFERENCES [dbo].[tb_CallType] ([CallTypeID])
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic] CHECK CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_CallType]
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_Destination] FOREIGN KEY([DestinationID])
REFERENCES [dbo].[tb_Destination] ([DestinationID])
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic] CHECK CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_Destination]
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_ServiceLevel] FOREIGN KEY([ServiceLevelID])
REFERENCES [dbo].[tb_ServiceLevel] ([ServiceLevelID])
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic] CHECK CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_ServiceLevel]
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_Trunk] FOREIGN KEY([CommercialTrunkID])
REFERENCES [dbo].[tb_Trunk] ([TrunkID])
GO
ALTER TABLE [dbo].[Tb_AggregateSummaryTraffic] CHECK CONSTRAINT [FK_Tb_AggregateSummaryTraffic_tb_Trunk]
GO
ALTER TABLE [dbo].[Tb_Aggregation]  WITH CHECK ADD  CONSTRAINT [FK_Tb_Aggregation_tb_Agreement] FOREIGN KEY([AgreementID])
REFERENCES [dbo].[tb_Agreement] ([AgreementID])
GO
ALTER TABLE [dbo].[Tb_Aggregation] CHECK CONSTRAINT [FK_Tb_Aggregation_tb_Agreement]
GO
ALTER TABLE [dbo].[Tb_Aggregation]  WITH CHECK ADD  CONSTRAINT [FK_Tb_Aggregation_tb_Direction] FOREIGN KEY([DirectionID])
REFERENCES [dbo].[tb_Direction] ([DirectionID])
GO
ALTER TABLE [dbo].[Tb_Aggregation] CHECK CONSTRAINT [FK_Tb_Aggregation_tb_Direction]
GO
ALTER TABLE [dbo].[Tb_AggregationCycle]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationCycle_tb_Aggregation] FOREIGN KEY([AggregationID])
REFERENCES [dbo].[Tb_Aggregation] ([AggregationID])
GO
ALTER TABLE [dbo].[Tb_AggregationCycle] CHECK CONSTRAINT [FK_Tb_AggregationCycle_tb_Aggregation]
GO
ALTER TABLE [dbo].[Tb_AggregationCycle]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationCycle_tb_AggregationCriteria] FOREIGN KEY([AggregationCriteriaID])
REFERENCES [dbo].[Tb_AggregationCriteria] ([AggregationCriteriaID])
GO
ALTER TABLE [dbo].[Tb_AggregationCycle] CHECK CONSTRAINT [FK_Tb_AggregationCycle_tb_AggregationCriteria]
GO
ALTER TABLE [dbo].[Tb_AggregationCycle]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationCycle_tb_AggregationType] FOREIGN KEY([AggregationtypeID])
REFERENCES [dbo].[Tb_AggregationType] ([AggregationTypeID])
GO
ALTER TABLE [dbo].[Tb_AggregationCycle] CHECK CONSTRAINT [FK_Tb_AggregationCycle_tb_AggregationType]
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationGrouping_tb_Aggregation] FOREIGN KEY([AggregationID])
REFERENCES [dbo].[Tb_Aggregation] ([AggregationID])
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping] CHECK CONSTRAINT [FK_Tb_AggregationGrouping_tb_Aggregation]
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationGrouping_tb_CallType] FOREIGN KEY([CallTypeID])
REFERENCES [dbo].[tb_CallType] ([CallTypeID])
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping] CHECK CONSTRAINT [FK_Tb_AggregationGrouping_tb_CallType]
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationGrouping_tb_Destination] FOREIGN KEY([DestinationID])
REFERENCES [dbo].[tb_Destination] ([DestinationID])
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping] CHECK CONSTRAINT [FK_Tb_AggregationGrouping_tb_Destination]
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationGrouping_tb_ServiceLevel] FOREIGN KEY([ServiceLevelID])
REFERENCES [dbo].[tb_ServiceLevel] ([ServiceLevelID])
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping] CHECK CONSTRAINT [FK_Tb_AggregationGrouping_tb_ServiceLevel]
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationGrouping_tb_Trunk] FOREIGN KEY([CommercialTrunkID])
REFERENCES [dbo].[tb_Trunk] ([TrunkID])
GO
ALTER TABLE [dbo].[Tb_AggregationGrouping] CHECK CONSTRAINT [FK_Tb_AggregationGrouping_tb_Trunk]
GO
ALTER TABLE [dbo].[Tb_AggregationRate]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationRate_tb_AggregationCycle] FOREIGN KEY([AggregationCycleID])
REFERENCES [dbo].[Tb_AggregationCycle] ([AggregationCycleID])
GO
ALTER TABLE [dbo].[Tb_AggregationRate] CHECK CONSTRAINT [FK_Tb_AggregationRate_tb_AggregationCycle]
GO
ALTER TABLE [dbo].[Tb_AggregationRate]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationRate_tb_Currency] FOREIGN KEY([CurrencyID])
REFERENCES [dbo].[tb_Currency] ([CurrencyID])
GO
ALTER TABLE [dbo].[Tb_AggregationRate] CHECK CONSTRAINT [FK_Tb_AggregationRate_tb_Currency]
GO
ALTER TABLE [dbo].[Tb_AggregationRateDetail]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationRateDetail_tb_AggregationRate] FOREIGN KEY([AggregationRateID])
REFERENCES [dbo].[Tb_AggregationRate] ([AggregationRateID])
GO
ALTER TABLE [dbo].[Tb_AggregationRateDetail] CHECK CONSTRAINT [FK_Tb_AggregationRateDetail_tb_AggregationRate]
GO
ALTER TABLE [dbo].[tb_AggregationTypeAndCriteriaMapping]  WITH CHECK ADD  CONSTRAINT [FK_tb_AggregationTypeAndCriteriaMapping_tb_AggregationCriteria] FOREIGN KEY([AggregationCriteriaID])
REFERENCES [dbo].[Tb_AggregationCriteria] ([AggregationCriteriaID])
GO
ALTER TABLE [dbo].[tb_AggregationTypeAndCriteriaMapping] CHECK CONSTRAINT [FK_tb_AggregationTypeAndCriteriaMapping_tb_AggregationCriteria]
GO
ALTER TABLE [dbo].[tb_AggregationTypeAndCriteriaMapping]  WITH CHECK ADD  CONSTRAINT [FK_tb_AggregationTypeAndCriteriaMapping_tb_AggregationType] FOREIGN KEY([AggregationTypeID])
REFERENCES [dbo].[Tb_AggregationType] ([AggregationTypeID])
GO
ALTER TABLE [dbo].[tb_AggregationTypeAndCriteriaMapping] CHECK CONSTRAINT [FK_tb_AggregationTypeAndCriteriaMapping_tb_AggregationType]
GO
ALTER TABLE [dbo].[Tb_AggregationtypePriority]  WITH CHECK ADD  CONSTRAINT [FK_Tb_AggregationtypePriority_tb_AggregationType] FOREIGN KEY([AggregationtypeID])
REFERENCES [dbo].[Tb_AggregationType] ([AggregationTypeID])
GO
ALTER TABLE [dbo].[Tb_AggregationtypePriority] CHECK CONSTRAINT [FK_Tb_AggregationtypePriority_tb_AggregationType]
GO
ALTER TABLE [dbo].[Tb_AggregationType]  WITH CHECK ADD  CONSTRAINT [CK_tb_AggregationType_CommitmentType] CHECK  (([CommitmentType]=(2) OR [CommitmentType]=(1) OR [CommitmentType]=(0)))
GO
ALTER TABLE [dbo].[Tb_AggregationType] CHECK CONSTRAINT [CK_tb_AggregationType_CommitmentType]
GO
ALTER TABLE [dbo].[Tb_AggregationType]  WITH CHECK ADD  CONSTRAINT [CK_tb_AggregationType_GracePeriod] CHECK  (([GracePeriod]=(1) OR [GracePeriod]=(0)))
GO
ALTER TABLE [dbo].[Tb_AggregationType] CHECK CONSTRAINT [CK_tb_AggregationType_GracePeriod]
GO
ALTER TABLE [dbo].[Tb_AggregationType]  WITH CHECK ADD  CONSTRAINT [CK_tb_AggregationType_Penalty] CHECK  (([Penalty]=(1) OR [Penalty]=(0)))
GO
ALTER TABLE [dbo].[Tb_AggregationType] CHECK CONSTRAINT [CK_tb_AggregationType_Penalty]
GO
