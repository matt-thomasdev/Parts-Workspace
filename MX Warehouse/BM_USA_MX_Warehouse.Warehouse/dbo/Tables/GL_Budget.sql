CREATE TABLE [dbo].[GL_Budget] (

	[McaId] varchar(8000) NULL, 
	[DateEnd] datetime2(6) NULL, 
	[AmtDb] float NULL, 
	[AmtCr] float NULL, 
	[AmtDb_Closed] float NULL, 
	[AmtCr_Closed] float NULL, 
	[AmtDb_MemoPosted] float NULL, 
	[AmtCr_MemoPosted] float NULL, 
	[AmtBud] float NULL, 
	[CountBud] int NULL, 
	[CountDb] int NULL, 
	[CountCr] int NULL
);