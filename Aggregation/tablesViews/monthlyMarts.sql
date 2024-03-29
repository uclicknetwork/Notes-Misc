USE [UC_Report]
GO
/****** Object:  Table [dbo].[tb_MonthlyINCrossOutKPIMartRefresh]    Script Date: 7/15/2021 3:14:48 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tb_MonthlyINCrossOutKPIMartRefresh](
	[CallMonth] [int] NOT NULL,
	[LastRefreshDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tb_MonthlyINCrossOutTraffic]    Script Date: 7/15/2021 3:14:48 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tb_MonthlyINCrossOutTraffic](
	[CallMonth] [int] NULL,
	[CallDuration] [int] NULL,
	[CircuitDuration] [int] NULL,
	[Answered] [int] NULL,
	[Seized] [int] NULL,
	[CallTypeID] [int] NULL,
	[INAccountID] [int] NULL,
	[OutAccountID] [int] NULL,
	[INTrunkID] [int] NULL,
	[OutTrunkID] [int] NULL,
	[INCommercialTrunkID] [int] NULL,
	[OUTCOmmercialTrunkID] [int] NULL,
	[INDestinationID] [int] NULL,
	[OUTDestinationID] [int] NULL,
	[RoutingDestinationID] [int] NULL,
	[INServiceLevelID] [int] NULL,
	[OUTServiceLevelID] [int] NULL,
	[INRoundedCallDuration] [int] NULL,
	[OutRoundedCallDuration] [int] NULL,
	[INChargeDuration] [decimal](19, 4) NULL,
	[OUTChargeDuration] [decimal](19, 4) NULL,
	[OriginDestinationID] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tb_MonthlyINUnionOutFinancial]    Script Date: 7/15/2021 3:14:48 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tb_MonthlyINUnionOutFinancial](
	[CallMonth] [int] NULL,
	[DirectionID] [int] NULL,
	[CallDuration] [int] NULL,
	[CircuitDuration] [int] NULL,
	[Answered] [int] NULL,
	[Seized] [int] NULL,
	[CallTypeID] [int] NULL,
	[AccountID] [int] NULL,
	[TrunkID] [int] NULL,
	[CommercialTrunkID] [int] NULL,
	[SettlementDestinationID] [int] NULL,
	[RoutingDestinationID] [int] NULL,
	[INServiceLevelID] [int] NULL,
	[OUTServiceLevelID] [int] NULL,
	[RatePlanID] [int] NULL,
	[RatingMethodID] [int] NULL,
	[RoundedCallDuration] [int] NULL,
	[ChargeDuration] [decimal](19, 4) NULL,
	[Amount] [decimal](19, 6) NULL,
	[Rate] [decimal](19, 6) NULL,
	[RateTypeID] [int] NULL,
	[CurrencyID] [int] NULL,
	[ErrorIndicator] [int] NULL,
	[OriginDestinationID] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tb_MonthlyKPIMartRefresh]    Script Date: 7/15/2021 3:14:48 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tb_MonthlyKPIMartRefresh](
	[CallMonth] [int] NOT NULL,
	[LastRefreshDate] [datetime] NOT NULL,
	[ModifiedDate] [datetime] NULL
) ON [PRIMARY]
GO
