USE [UC_Reference]
GO

/****** Object:  UserDefinedFunction [dbo].[FN_ParseValueListWithNULL]    Script Date: 7/15/2021 3:08:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   Function [dbo].[FN_ParseValueListWithNULL]
(
	@VariableValue nvarchar(max)
)
Returns @ValueList table ( RecordValue varchar(100) )
As

Begin

		Declare @Tempstring nvarchar(max) = @VariableValue,
		        @TempValue varchar(100) = ''


		while ( len(@Tempstring) > 0 )
		Begin

					----------------------------------------------
					-- Only non numeric values allowed are ","
					----------------------------------------------

					if ( substring(@Tempstring , 1 , 1)  = ',' )
					Begin
					        
							if ( len(rtrim(ltrim(@TempValue))) > 0 )
							Begin

									insert into @ValueList values ( rtrim(ltrim(@TempValue)) )

							End

							else
							Begin
								
									insert into @ValueList values (NULL)

							End

							set @TempValue = ''

					End


					Else
					Begin
				      
							set @TempValue = @TempValue + substring(@Tempstring , 1 , 1)

					End

					set @Tempstring = substring(@Tempstring , 2 , len(@Tempstring))

		End

		if ( len(rtrim(ltrim(@TempValue))) > 0 )
		Begin

				insert into @ValueList values ( rtrim(ltrim(@TempValue)) )

		End

		else
		Begin

				insert into @ValueList values ( NULL )

		End

		Return
		
End




GO


