SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[cleanupData]
	-- Add the parameters for the stored procedure here
	@MaxBatchesToKeep int
AS
BEGIN

declare @MaxId int, @BatchId int

SELECT @MaxId = max(id) - (@MaxBatchesToKeep-1)
  FROM [dbo].[Batches]

Declare c Cursor For Select Distinct [BatchId] From [dbo].[Stage] where [BatchId] < @MaxId
  Open c
  Fetch next From c into @BatchId
	While @@Fetch_Status=0 Begin
	   delete from [dbo].[Stage] where @BatchId = [BatchId]

	   Fetch next From c into @BatchId
	End
 Close c
Deallocate c
delete from [dbo].[Batches] where [id] < @MaxId
END
GO


